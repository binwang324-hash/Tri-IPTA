using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Thermo.Interfaces.ExactiveAccess_V1;
using Thermo.Interfaces.InstrumentAccess_V1.MsScanContainer;
using IMsScan = Thermo.Interfaces.InstrumentAccess_V2.MsScanContainer.IMsScan;

namespace TriIPTAIAPI
{
    /// <summary>
    /// Real-time d0/d2/d8 isotope-triplet detection.
    /// Design notes:
    /// - Keep the MsScanArrived callback lightweight by extracting only required peaks and queueing them.
    /// - Use a background worker to maintain the rolling time window and perform triplet matching.
    /// - Apply strict retention-time filtering; skip matching when RT metadata is unavailable to reduce false triggers.
    /// - Keep syntax compatible with older C# compiler settings used in some instrument-control environments.
    /// </summary>
    internal class TriIptaTripletDetector : IDisposable
    {
        // ===== Configuration parameters; adjust or externalize as needed =====
        private readonly double TIME_WINDOW_SECONDS = 5.0;
        private readonly double D2_MASS_SHIFT = 2.014552;
        private readonly double D8_MASS_SHIFT = 8.050208;
        private readonly double MASS_TOLERANCE_PPM = 5.0;
        private readonly double MIN_INTENSITY = 10000.0;
        private readonly double MAX_TRIPLET_INTENSITY_RATIO = 10.0;
        private readonly double RT_TOLERANCE_MIN = 5.0 / 60.0; // Retention-time tolerance in minutes; 5 s for d0/d2/d8 co-elution matching.
        private readonly int TRIPLET_COOLDOWN_SECONDS = 30;    // Cooldown for the same d0 precursor before another MS2 trigger is allowed.
        private readonly int BACKGROUND_SLEEP_MS = 80;
        // =========================================

        private readonly ConcurrentQueue<ScanData> _scanQueue = new ConcurrentQueue<ScanData>();
        private readonly Queue<ScanData> _timeWindowBuffer = new Queue<ScanData>(); // Used only by the background worker.
        private readonly List<DetectedTripletRecord> _recentTriplets = new List<DetectedTripletRecord>();

        private int _totalScans = 0;
        private int _totalPeaks = 0;
        private int _detectedTripletsCount = 0;
        private int _skippedNonMs1Scans = 0;
        private bool _ms1FilterMetadataWarningPrinted = false;

        private readonly bool _ms2Enabled;
        private DynamicInclusionListTrigger _inclusionListTrigger;

        private StreamWriter _csvWriter;
        private readonly string _csvFilePath;

        private CancellationTokenSource _cts;
        private Task _processingTask;

        public TriIptaTripletDetector(bool enableMS2)
        {
            _ms2Enabled = enableMS2;
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
            _csvFilePath = "TriIPTA_Triplets_" + timestamp + ".csv";
        }

        public void DoJob()
        {
            InitializeCsvFile();

            try
            {
                using (IExactiveInstrumentAccess instrument = Connection.GetFirstInstrument())
                {
                    // Initialize the MS2 trigger when requested.
                    if (_ms2Enabled)
                    {
                        var cfg = new DynamicInclusionListTrigger.Config
                        {
                            MaxListSize = 50,
                            MassTolerancePpm = MASS_TOLERANCE_PPM,
                            RetentionTimeToleranceMin = RT_TOLERANCE_MIN,
                            DuplicateCooldownSeconds = 30,
                            MinUpdateIntervalMs = 300,
                            BackgroundUpdateMs = 400
                        };

                        _inclusionListTrigger = new DynamicInclusionListTrigger(cfg);
                        bool initOk = _inclusionListTrigger.Initialize(instrument);
                        if (!initOk)
                        {
                            Console.WriteLine("[WARN] Dynamic MS/MS triggering could not be initialized; continuing in MS1-only mode.");
                            _inclusionListTrigger = null;
                        }
                        else
                        {
                            _inclusionListTrigger.Start();
                        }
                    }

                    IMsScanContainer orbitrap = instrument.GetMsScanContainer(0);

                    Console.WriteLine("=== Real-Time Tri-IPTA Detection ===");
                    Console.WriteLine("[INFO] Listening to full-scan MS1 data. Press any key to stop.");

                    // Start the background processing task.
                    _cts = new CancellationTokenSource();
                    _processingTask = Task.Run(() => BackgroundProcessor(_cts.Token), _cts.Token);

                    // Subscribe to scan-arrival events; keep the callback lightweight.
                    orbitrap.MsScanArrived += Orbitrap_MsScanArrived;

                    // Wait for a key press to stop listening.
                    while (!Console.KeyAvailable)
                    {
                        Thread.Sleep(100);
                    }

                    // Stop listening to scan events.
                    orbitrap.MsScanArrived -= Orbitrap_MsScanArrived;

                    // Cancel the background task and wait briefly for shutdown.
                    _cts.Cancel();
                    try
                    {
                        _processingTask.Wait(2000);
                    }
                    catch { }

                    // Stop the MS2 trigger.
                    if (_inclusionListTrigger != null)
                    {
                        _inclusionListTrigger.Stop();
                        _inclusionListTrigger.PrintStatistics();
                    }
                }
            }
            finally
            {
                CloseCsvFile();
                PrintFinalStatistics();
            }
        }

        // Scan callback; keep this path as fast as possible.
        private void Orbitrap_MsScanArrived(object sender, MsScanEventArgs e)
        {
            try
            {
                using (IMsScan scan = (IMsScan)e.GetScan())
                {
                    Interlocked.Increment(ref _totalScans);

                    // In DDA methods, MsScanArrived can include both full MS1 and ddMS2 scans.
                    // Tri-IPTA triplet recognition must only use full-scan MS1 events.
                    if (!IsFullMs1Scan(scan))
                    {
                        Interlocked.Increment(ref _skippedNonMs1Scans);
                        return;
                    }

                    double rt = GetRetentionTime(scan); // May be NaN if RT metadata is unavailable.
                    DateTime scanTime = DateTime.UtcNow;

                    List<PeakData> peaks = new List<PeakData>();
                    foreach (var centroid in scan.Centroids)
                    {
                        double intensity = centroid.Intensity;
                        if (intensity >= MIN_INTENSITY)
                        {
                            PeakData p = new PeakData();
                            p.Mz = centroid.Mz;
                            p.Intensity = centroid.Intensity;
                            p.Charge = centroid.Charge.HasValue ? centroid.Charge.Value : 0;
                            p.RetentionTime = rt;
                            p.ScanTime = scanTime;
                            peaks.Add(p);
                        }
                    }

                    if (peaks.Count > 0)
                    {
                        Interlocked.Add(ref _totalPeaks, peaks.Count);
                        ScanData sd = new ScanData();
                        sd.ScanTime = scanTime;
                        sd.RetentionTime = rt;
                        sd.Peaks = peaks;
                        sd.ScanNumber = _totalScans;
                        _scanQueue.Enqueue(sd);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[ERROR] Scan callback failed: " + ex.Message);
            }
        }

        private void BackgroundProcessor(CancellationToken token)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    bool dequeued = false;
                    ScanData sd;
                    while (_scanQueue.TryDequeue(out sd))
                    {
                        dequeued = true;
                        _timeWindowBuffer.Enqueue(sd);
                    }

                    if (dequeued)
                    {
                        DateTime cutoff = DateTime.UtcNow.AddSeconds(-TIME_WINDOW_SECONDS);
                        while (_timeWindowBuffer.Count > 0 && _timeWindowBuffer.Peek().ScanTime < cutoff)
                        {
                            _timeWindowBuffer.Dequeue();
                        }

                        AnalyzeTimeWindow();
                    }

                    Thread.Sleep(BACKGROUND_SLEEP_MS);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Console.WriteLine("[ERROR] Background triplet processor failed: " + ex.Message);
            }
        }

        private void AnalyzeTimeWindow()
        {
            if (_timeWindowBuffer.Count < 2) return;

            List<PeakData> allPeaks = new List<PeakData>();
            foreach (var s in _timeWindowBuffer)
            {
                if (s.Peaks != null && s.Peaks.Count > 0)
                {
                    allPeaks.AddRange(s.Peaks);
                }
            }

            if (allPeaks.Count < 3) return;

            List<PeakData> sortedPeaks = allPeaks.OrderBy(p => p.Mz).ToList();

            for (int i = 0; i < sortedPeaks.Count; i++)
            {
                PeakData candidate = sortedPeaks[i];
                CheckAsD0(candidate, sortedPeaks);
                CheckAsD2(candidate, sortedPeaks);
                CheckAsD8(candidate, sortedPeaks);
            }
        }

        private void CheckAsD0(PeakData d0Candidate, List<PeakData> peaks)
        {
            double expectedD2Mz = d0Candidate.Mz + D2_MASS_SHIFT;
            double expectedD8Mz = d0Candidate.Mz + D8_MASS_SHIFT;

            List<PeakData> d2Matches = FindMatches(peaks, expectedD2Mz, d0Candidate.Mz, d0Candidate.RetentionTime);
            List<PeakData> d8Matches = FindMatches(peaks, expectedD8Mz, d0Candidate.Mz, d0Candidate.RetentionTime);

            if (d2Matches.Count > 0 && d8Matches.Count > 0)
            {
                PeakData bestD2 = d2Matches.OrderByDescending(p => p.Intensity).First();
                PeakData bestD8 = d8Matches.OrderByDescending(p => p.Intensity).First();
                RegisterTriplet(d0Candidate, bestD2, bestD8, "D0");
            }
        }

        private void CheckAsD2(PeakData d2Candidate, List<PeakData> peaks)
        {
            double expectedD0Mz = d2Candidate.Mz - D2_MASS_SHIFT;
            double expectedD8Mz = d2Candidate.Mz + (D8_MASS_SHIFT - D2_MASS_SHIFT);

            List<PeakData> d0Matches = FindMatches(peaks, expectedD0Mz, d2Candidate.Mz, d2Candidate.RetentionTime);
            List<PeakData> d8Matches = FindMatches(peaks, expectedD8Mz, d2Candidate.Mz, d2Candidate.RetentionTime);

            if (d0Matches.Count > 0 && d8Matches.Count > 0)
            {
                PeakData bestD0 = d0Matches.OrderByDescending(p => p.Intensity).First();
                PeakData bestD8 = d8Matches.OrderByDescending(p => p.Intensity).First();
                RegisterTriplet(bestD0, d2Candidate, bestD8, "D2");
            }
        }

        private void CheckAsD8(PeakData d8Candidate, List<PeakData> peaks)
        {
            double expectedD0Mz = d8Candidate.Mz - D8_MASS_SHIFT;
            double expectedD2Mz = d8Candidate.Mz - (D8_MASS_SHIFT - D2_MASS_SHIFT);

            List<PeakData> d0Matches = FindMatches(peaks, expectedD0Mz, d8Candidate.Mz, d8Candidate.RetentionTime);
            List<PeakData> d2Matches = FindMatches(peaks, expectedD2Mz, d8Candidate.Mz, d8Candidate.RetentionTime);

            if (d0Matches.Count > 0 && d2Matches.Count > 0)
            {
                PeakData bestD0 = d0Matches.OrderByDescending(p => p.Intensity).First();
                PeakData bestD2 = d2Matches.OrderByDescending(p => p.Intensity).First();
                RegisterTriplet(bestD0, bestD2, d8Candidate, "D8");
            }
        }

        private List<PeakData> FindMatches(List<PeakData> peaks, double targetMz, double referenceMz, double referenceRt)
        {
            List<PeakData> matches = new List<PeakData>();

            double tolDa = GetMassToleranceDa(targetMz);
            double minMz = targetMz - tolDa;
            double maxMz = targetMz + tolDa;

            double selfTol = GetMassToleranceDa(referenceMz);

            foreach (var peak in peaks)
            {
                if (peak.Mz < minMz) continue;
                if (peak.Mz > maxMz) break;

                if (Math.Abs(peak.Mz - referenceMz) <= selfTol) continue;

                if (Double.IsNaN(referenceRt) || Double.IsNaN(peak.RetentionTime))
                {
                    // Skip matching when RT is unavailable to avoid excessive false matches.
                    continue;
                }

                if (Math.Abs(peak.RetentionTime - referenceRt) > RT_TOLERANCE_MIN) continue;

                matches.Add(peak);
            }

            return matches;
        }

        private double GetMassToleranceDa(double mz)
        {
            return mz * MASS_TOLERANCE_PPM / 1000000.0;
        }

        private bool IsSamePrecursorMz(double mz1, double mz2)
        {
            double referenceMz = (mz1 + mz2) / 2.0;
            return Math.Abs(mz1 - mz2) <= GetMassToleranceDa(referenceMz);
        }

        private bool IsRecentlyDetectedTriplet(double precursorMz, DateTime now)
        {
            _recentTriplets.RemoveAll(t => (now - t.DetectionTime).TotalSeconds >= TRIPLET_COOLDOWN_SECONDS);
            return _recentTriplets.Any(t => IsSamePrecursorMz(t.PrecursorMz, precursorMz));
        }

        private void RegisterTriplet(PeakData d0, PeakData d2, PeakData d8, string seedIonChannel)
        {
            DateTime now = DateTime.UtcNow;
            double intensityRatio = CalculateTripletIntensityRatio(d0, d2, d8);
            bool ms2TriggerEligible = intensityRatio <= MAX_TRIPLET_INTENSITY_RATIO;

            if (IsRecentlyDetectedTriplet(d0.Mz, now))
            {
                return;
            }

            _recentTriplets.Add(new DetectedTripletRecord
            {
                PrecursorMz = d0.Mz,
                DetectionTime = now
            });

            int tripletId = Interlocked.Increment(ref _detectedTripletsCount);

            Console.WriteLine(String.Format("[DETECT] #{0} d0 m/z={1:F4}, RT={2:F3} min, intensity={3:E2}, seed={4}",
                tripletId, d0.Mz, d0.RetentionTime, d0.Intensity, seedIonChannel));

            WriteTripletToCsv(tripletId, d0, d2, d8, seedIonChannel, intensityRatio, ms2TriggerEligible);

            if (_ms2Enabled && _inclusionListTrigger != null)
            {
                if (ms2TriggerEligible)
                {
                    string comment = String.Format("Triplet #{0} ({1})", tripletId, seedIonChannel);
                    bool r0 = _inclusionListTrigger.AddPrecursorIon(d0.Mz, d0.RetentionTime, comment + " - d0");

                    if (r0)
                    {
                        Console.WriteLine(
                            String.Format("[MS2] Queued d0 precursor from triplet #{0} for dynamic inclusion-list update.", tripletId));
                    }
                }
                else
                {
                    Console.WriteLine(
                        String.Format("[MS2] Skipped triplet #{0}: intensity ratio {1:F2} exceeds {2:F1}.",
                            tripletId, intensityRatio, MAX_TRIPLET_INTENSITY_RATIO));
                }
            }

        }

        private double CalculateTripletIntensityRatio(PeakData d0, PeakData d2, PeakData d8)
        {
            double maxIntensity = Math.Max(d0.Intensity, Math.Max(d2.Intensity, d8.Intensity));
            double minIntensity = Math.Min(d0.Intensity, Math.Min(d2.Intensity, d8.Intensity));

            if (minIntensity <= 0)
            {
                return Double.PositiveInfinity;
            }

            return maxIntensity / minIntensity;
        }

        private void InitializeCsvFile()
        {
            try
            {
                _csvWriter = new StreamWriter(_csvFilePath, false, Encoding.UTF8);
                _csvWriter.WriteLine("TripletID,DetectionTime,RetentionTime,D0_Mz,D0_Intensity,D2_Mz,D2_Intensity,D8_Mz,D8_Intensity,IntensityRatio,SeedIonChannel,Ms2TriggerEligible");
                _csvWriter.Flush();
                Console.WriteLine("[INFO] Output CSV: " + _csvFilePath);
            }
            catch (Exception ex)
            {
                Console.WriteLine("[ERROR] Failed to create output CSV: " + ex.Message);
                throw;
            }
        }

        private void WriteTripletToCsv(int tripletId, PeakData d0, PeakData d2, PeakData d8, string seedIonChannel, double intensityRatio, bool ms2TriggerEligible)
        {
            try
            {
                if (_csvWriter == null) return;

                string line = String.Format(CultureInfo.InvariantCulture,
                    "{0},{1},{2:F4},{3:F6},{4:E4},{5:F6},{6:E4},{7:F6},{8:E4},{9:F4},{10},{11}",
                    tripletId,
                    DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                    d0.RetentionTime,
                    d0.Mz,
                    d0.Intensity,
                    d2.Mz,
                    d2.Intensity,
                    d8.Mz,
                    d8.Intensity,
                    intensityRatio,
                    seedIonChannel,
                    ms2TriggerEligible ? "True" : "False"
                    );

                _csvWriter.WriteLine(line);
                if (tripletId % 5 == 0) _csvWriter.Flush();
            }
            catch (Exception ex)
            {
                Console.WriteLine("[ERROR] Failed to write output CSV: " + ex.Message);
            }
        }

        private void CloseCsvFile()
        {
            try
            {
                if (_csvWriter != null)
                {
                    _csvWriter.Flush();
                    _csvWriter.Close();
                    _csvWriter.Dispose();
                    Console.WriteLine("[INFO] Output CSV saved: " + _csvFilePath);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[ERROR] Failed to close output CSV: " + ex.Message);
            }
        }

        /// <summary>
        /// Determine whether the current scan is a full-scan MS1 event.
        /// In DDA methods, MsScanArrived may deliver both full MS1 and ddMS2 scans.
        /// The method first checks MSOrder/MS Level metadata, then ScanType/Scan Filter fields.
        /// If the current API or instrument does not expose recognizable metadata, it fails open and preserves the original behavior.
        /// </summary>
        private bool IsFullMs1Scan(IMsScan scan)
        {
            bool hasRelevantMetadata = false;
            bool isMs1;
            string value;

            string[] orderKeys = new string[] {
                "MSOrder", "MS Order", "MS order", "MSLevel", "MS Level",
                "MSn Order", "MSnOrder"
            };

            foreach (string key in orderKeys)
            {
                if (TryGetScanInfoValue(scan, key, out value))
                {
                    hasRelevantMetadata = true;
                    if (TryClassifyMsOrder(value, out isMs1))
                    {
                        return isMs1;
                    }
                }
            }

            string[] typeKeys = new string[] {
                "Scan Filter", "ScanFilter", "Filter", "Filter String",
                "ScanType", "Scan Type", "ScanMode", "Scan Mode",
                "Scan Description", "ScanDescription"
            };

            foreach (string key in typeKeys)
            {
                if (TryGetScanInfoValue(scan, key, out value))
                {
                    hasRelevantMetadata = true;
                    if (TryClassifyScanType(value, out isMs1))
                    {
                        return isMs1;
                    }
                }
            }

            if (TryFindScanClassificationFromContainer(scan.CommonInformation, out isMs1))
            {
                return isMs1;
            }
            if (TryFindScanClassificationFromContainer(scan.SpecificInformation, out isMs1))
            {
                return isMs1;
            }

            if (!hasRelevantMetadata && !_ms1FilterMetadataWarningPrinted)
            {
                _ms1FilterMetadataWarningPrinted = true;
                Console.WriteLine("[WARN] Cannot determine MS order from scan metadata; processing incoming scans as full-scan MS1.");
            }

            return true;
        }

        private bool TryFindScanClassificationFromContainer(IInfoContainer container, out bool isMs1)
        {
            isMs1 = false;
            if (container == null || container.Names == null) return false;

            foreach (string key in container.Names)
            {
                string normalizedKey = (key ?? "").Replace(" ", "").Replace("_", "").Replace("-", "").ToLowerInvariant();
                string value;

                if (normalizedKey == "msorder" || normalizedKey == "mslevel" || normalizedKey == "msnorder")
                {
                    if (container.TryGetValue(key, out value) && TryClassifyMsOrder(value, out isMs1))
                    {
                        return true;
                    }
                }

                if (normalizedKey.IndexOf("filter", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    normalizedKey == "scantype" ||
                    normalizedKey == "scanmode" ||
                    normalizedKey == "scandescription")
                {
                    if (container.TryGetValue(key, out value) && TryClassifyScanType(value, out isMs1))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private bool TryGetScanInfoValue(IMsScan scan, string key, out string value)
        {
            value = null;
            if (scan == null) return false;

            try
            {
                if (scan.CommonInformation != null && scan.CommonInformation.TryGetValue(key, out value))
                {
                    return !String.IsNullOrWhiteSpace(value);
                }
            }
            catch { }

            try
            {
                if (scan.SpecificInformation != null && scan.SpecificInformation.TryGetValue(key, out value))
                {
                    return !String.IsNullOrWhiteSpace(value);
                }
            }
            catch { }

            return false;
        }

        private bool TryClassifyMsOrder(string value, out bool isMs1)
        {
            isMs1 = false;
            if (String.IsNullOrWhiteSpace(value)) return false;

            string v = value.Trim();
            int order;
            if (Int32.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out order))
            {
                isMs1 = (order == 1);
                return true;
            }

            string lower = v.ToLowerInvariant();
            if (lower == "ms" || lower == "ms1" || lower == "full" || lower.IndexOf("ms1", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                isMs1 = true;
                return true;
            }
            if (lower.IndexOf("ms2", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower.IndexOf("msn", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower.IndexOf("ddms2", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                isMs1 = false;
                return true;
            }

            return false;
        }

        private bool TryClassifyScanType(string value, out bool isMs1)
        {
            isMs1 = false;
            if (String.IsNullOrWhiteSpace(value)) return false;

            string lower = value.ToLowerInvariant();

            if (lower.IndexOf("ms2", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower.IndexOf("msn", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower.IndexOf("ddms2", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower.IndexOf("@hcd", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower.IndexOf("@cid", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower.IndexOf("@etd", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower.IndexOf("fragment", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                isMs1 = false;
                return true;
            }

            if (lower.IndexOf("full ms", StringComparison.OrdinalIgnoreCase) >= 0 ||
                lower == "full" ||
                lower == "ms" ||
                lower == "ms1" ||
                lower.IndexOf("ms1", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                isMs1 = true;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Try to parse retention time in minutes. Returns NaN if parsing fails.
        /// </summary>
        private double GetRetentionTime(IMsScan scan)
        {
            try
            {
                var commonInfo = scan.CommonInformation;
                if (commonInfo != null)
                {
                    // Common candidate field names.
                    string[] candidates = new string[] {
                        "Retention Time [min]",
                        "Retention Time",
                        "RT [min]",
                        "RT",
                        "Scan Start Time",
                        "Start Time"
                    };

                    foreach (string key in candidates)
                    {
                        if (commonInfo.Names != null && commonInfo.Names.Contains(key))
                        {
                            string val;
                            bool ok = commonInfo.TryGetValue(key, out val);
                            if (ok)
                            {
                                double rtParsed;
                                if (Double.TryParse(val, NumberStyles.Float, CultureInfo.InvariantCulture, out rtParsed))
                                {
                                    return rtParsed;
                                }
                            }
                        }
                    }

                    // Generic fallback: try fields whose names contain "retention" or "rt".
                    foreach (string key in commonInfo.Names)
                    {
                        if (key.IndexOf("retention", StringComparison.OrdinalIgnoreCase) >= 0 ||
                            key.IndexOf("rt", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            string vv;
                            bool ok2 = commonInfo.TryGetValue(key, out vv);
                            if (ok2)
                            {
                                double rt2;
                                if (Double.TryParse(vv, NumberStyles.Float, CultureInfo.InvariantCulture, out rt2))
                                {
                                    return rt2;
                                }
                            }
                        }
                    }
                }
            }
            catch
            {
                // Ignore metadata parsing errors.
            }

            return Double.NaN;
        }

        private void PrintFinalStatistics()
        {
            Console.WriteLine();
            Console.WriteLine("=== Run Summary ===");
            Console.WriteLine("Total scans processed: " + _totalScans);
            Console.WriteLine("Non-MS1 scans skipped: " + _skippedNonMs1Scans);
            Console.WriteLine("Total peaks above threshold: " + _totalPeaks);
            Console.WriteLine("Total triplets detected: " + _detectedTripletsCount);
            Console.WriteLine("Output CSV: " + _csvFilePath);
            if (_ms2Enabled)
            {
                Console.WriteLine("Dynamic MS/MS triggering: " + (_inclusionListTrigger != null ? "Enabled" : "Not enabled"));
                if (_inclusionListTrigger != null)
                {
                    Console.WriteLine("Final in-memory inclusion-list size: " + _inclusionListTrigger.InclusionListSize);
                }
            }
        }

        public void Dispose()
        {
            try { _cts?.Cancel(); } catch { }

            if (_processingTask != null)
            {
                try { _processingTask.Wait(500); } catch { }
            }

            if (_inclusionListTrigger != null)
            {
                try { _inclusionListTrigger.Stop(); _inclusionListTrigger.Dispose(); } catch { }
            }

            CloseCsvFile();
        }

        // Data containers.
        private class ScanData
        {
            public DateTime ScanTime { get; set; }
            public double RetentionTime { get; set; }
            public List<PeakData> Peaks { get; set; }
            public int ScanNumber { get; set; }
        }

        private class PeakData
        {
            public double Mz { get; set; }
            public double Intensity { get; set; }
            public int Charge { get; set; }
            public double RetentionTime { get; set; }
            public DateTime ScanTime { get; set; }
        }

        private class DetectedTripletRecord
        {
            public double PrecursorMz { get; set; }
            public DateTime DetectionTime { get; set; }
        }
    }
}
