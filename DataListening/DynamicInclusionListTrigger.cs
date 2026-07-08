using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Thermo.Interfaces.ExactiveAccess_V1;
using Thermo.Interfaces.InstrumentAccess_V1.Control.Methods;
using Thermo.Interfaces.InstrumentAccess_V1.MsScanContainer;

namespace TriIPTAIAPI
{
    /// <summary>
    /// Dynamic MS/MS trigger based on real-time inclusion-list updates.
    /// Key design points:
    /// - AddPrecursorIon only updates the in-memory list and increments an update version; the background worker calls ReplaceTable.
    /// - The background update interval is configurable to balance timeliness and instrument-control overhead.
    /// - A configurable RT window is used for inclusion-table entries.
    /// </summary>
    class DynamicInclusionListTrigger : IDisposable
    {
        private readonly Config _config;
        private IMethods _methods = null;
        private IExactiveInstrumentAccess _instrument;

        private readonly List<InclusionEntry> _inclusionList = new List<InclusionEntry>();
        private readonly object _listLock = new object();

        private bool _isInitialized = false;
        private bool _isRunning = false;

        private int _totalAdded = 0;
        private int _totalUpdates = 0;
        private int _duplicatePrecursorsSkipped = 0;

        private Thread _updaterThread;
        private volatile bool _updaterStopRequested = false;
        private int _updateVersion = 0;
        private int _lastAppliedVersion = 0;

        public class Config
        {
            public int MaxListSize { get; set; } = 80;           // Maximum inclusion-list size.
            public double MassTolerancePpm { get; set; } = 5.0;  // ppm
            public double RetentionTimeToleranceMin { get; set; } = 5.0 / 60.0; // minutes; 5 s inclusion-window half-width
            public int DuplicateCooldownSeconds { get; set; } = 30; // Suppress repeated additions of the same precursor within one chromatographic peak.
            public int MinUpdateIntervalMs { get; set; } = 300;  // Minimum update interval in milliseconds for throttling.
            public int BackgroundUpdateMs { get; set; } = 500;   // Background polling interval in milliseconds; shorter values update faster but more often.
        }

        internal DynamicInclusionListTrigger(Config config = null)
        {
            _config = config ?? new Config();
        }

        internal bool Initialize(IExactiveInstrumentAccess instrument)
        {
            try
            {
                _instrument = instrument;
                _methods = _instrument.Control.Methods;
                if (_methods == null)
                {
                    Console.WriteLine("[ERROR] Failed to access the instrument method-control interface.");
                    return false;
                }

                // Clear the instrument inclusion table on initialization.
                ClearInstrumentList();

                _isInitialized = true;
                Console.WriteLine("[INFO] Dynamic MS/MS triggering initialized.");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to initialize dynamic MS/MS triggering: {ex.Message}");
                return false;
            }
        }

        internal void Start()
        {
            if (!_isInitialized)
            {
                Console.WriteLine("[ERROR] Dynamic MS/MS triggering cannot start because initialization failed.");
                return;
            }

            _isRunning = true;
            _updaterStopRequested = false;
            _updaterThread = new Thread(UpdaterLoop) { IsBackground = true, Name = "DynamicInclusionListTrigger-Updater" };
            _updaterThread.Start();
            Console.WriteLine("[INFO] Dynamic MS/MS triggering is active.");
        }

        internal void Stop()
        {
            if (!_isRunning && _updaterStopRequested)
            {
                return;
            }

            bool wasRunning = _isRunning;
            _isRunning = false;
            _updaterStopRequested = true;
            if (_updaterThread != null && _updaterThread.IsAlive)
            {
                _updaterThread.Join(2000);
            }
            // Ensure the instrument inclusion list is cleared on shutdown.
            ClearInstrumentList();
            if (wasRunning)
            {
                Console.WriteLine("[INFO] Dynamic MS/MS triggering stopped.");
            }
        }

        /// <summary>
        /// Add a precursor ion to the in-memory inclusion list and request a background update.
        /// Returns true when the precursor ion was added to memory; this does not guarantee that the instrument table has already been updated.
        /// </summary>
        internal bool AddPrecursorIon(double precursorMz, double retentionTime, string comment = "")
        {
            if (!_isRunning)
            {
                Console.WriteLine($"[WARN] Dynamic MS/MS triggering is not active; skipped precursor m/z={precursorMz:F4}.");
                return false;
            }

            lock (_listLock)
            {
                DateTime now = DateTime.Now;

                // Suppress repeated additions of the same precursor during the same chromatographic peak.
                // This check is independent of the reported RT, because consecutive MS1 scans may report
                // slightly different RT values for the same eluting ion.
                bool recentlyQueued = _inclusionList.Any(e =>
                    IsSamePrecursorMz(e.PrecursorMz, precursorMz) &&
                    (now - e.AddedTime).TotalSeconds < _config.DuplicateCooldownSeconds);

                if (recentlyQueued)
                {
                    _duplicatePrecursorsSkipped++;
                    return false;
                }

                // Deduplicate entries using mass and retention time as a second guard.
                bool exists = _inclusionList.Any(e =>
                    IsSamePrecursorMz(e.PrecursorMz, precursorMz) &&
                    Math.Abs(e.RetentionTime - retentionTime) < _config.RetentionTimeToleranceMin);

                if (exists)
                {
                    _duplicatePrecursorsSkipped++;
                    return false;
                }

                var entry = new InclusionEntry
                {
                    PrecursorMz = precursorMz,
                    RetentionTime = retentionTime,
                    AddedTime = now,
                    Comment = comment
                };

                _inclusionList.Add(entry);
                _totalAdded++;

                // Keep the in-memory list within the configured size limit.
                if (_inclusionList.Count > _config.MaxListSize)
                {
                    var toRemove = _inclusionList.OrderBy(e => e.AddedTime).Take(_inclusionList.Count - _config.MaxListSize).ToList();
                    foreach (var r in toRemove) _inclusionList.Remove(r);
                }

                // Increment the update version. The background worker catches up to the latest version and avoids lost updates.
                Interlocked.Increment(ref _updateVersion);
            }

            return true;
        }

        private void UpdaterLoop()
        {
            DateTime lastUpdate = DateTime.MinValue;
            while (!_updaterStopRequested)
            {
                try
                {
                    int observedVersion = Volatile.Read(ref _updateVersion);
                    int appliedVersion = Volatile.Read(ref _lastAppliedVersion);
                    if (observedVersion != appliedVersion)
                    {
                        var now = DateTime.Now;
                        if ((now - lastUpdate).TotalMilliseconds >= _config.MinUpdateIntervalMs)
                        {
                            UpdateInstrumentList();
                            lastUpdate = now;
                            Volatile.Write(ref _lastAppliedVersion, observedVersion);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Dynamic inclusion-list updater failed: {ex.Message}");
                }

                // The sleep interval controls update timeliness.
                int sleepMs = Math.Max(50, _config.BackgroundUpdateMs);
                Thread.Sleep(sleepMs);
            }
        }

        private void UpdateInstrumentList()
        {
            try
            {
                List<InclusionEntry> snapshot;
                lock (_listLock)
                {
                    snapshot = _inclusionList.OrderByDescending(e => e.AddedTime).Take(_config.MaxListSize).ToList();
                }

                // Build the replacement inclusion table.
                ITable inclusionTable = _methods.CreateTable(typeof(IInclusionTable));

                foreach (var entry in snapshot)
                {
                    ITableRow row = inclusionTable.CreateRow();

                    row.ColumnValues["Mass [m/z]"] = entry.PrecursorMz.ToString("F4");
                    row.ColumnValues["Polarity"] = "Positive";

                    // Use the configured RT tolerance as a half-width RT window.
                    double startTime = Math.Max(0, entry.RetentionTime - _config.RetentionTimeToleranceMin);
                    double endTime = entry.RetentionTime + _config.RetentionTimeToleranceMin;

                    row.ColumnValues["Start [min]"] = startTime.ToString("F2");
                    row.ColumnValues["End [min]"] = endTime.ToString("F2");

                    if (!string.IsNullOrEmpty(entry.Comment))
                        row.ColumnValues["Comment"] = entry.Comment;

                    inclusionTable.Rows.Add(row);
                }

                // ReplaceTable parameters (methodIndex, tableId) may vary across instruments and methods.
                // The official Thermo example uses (1, 13); adjust these values if your method requires different indices.
                _methods.ReplaceTable(1, 13, inclusionTable);

                Interlocked.Increment(ref _totalUpdates);

                // Print a compact progress message periodically.
                if (_totalUpdates % 20 == 0)
                {
                    Console.WriteLine($"[MS2] Inclusion list synchronized: {snapshot.Count} active entries, {_totalUpdates} updates sent.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to update the instrument inclusion list: {ex.Message}");
            }
        }

        private void ClearInstrumentList()
        {
            try
            {
                if (_methods != null)
                {
                    ITable emptyTable = _methods.CreateTable(typeof(IInclusionTable));
                    _methods.ReplaceTable(1, 13, emptyTable);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to clear the instrument inclusion list: {ex.Message}");
            }
        }

        private double GetMassToleranceDa(double mz)
        {
            return mz * _config.MassTolerancePpm / 1000000.0;
        }

        private bool IsSamePrecursorMz(double mz1, double mz2)
        {
            double referenceMz = (mz1 + mz2) / 2.0;
            return Math.Abs(mz1 - mz2) <= GetMassToleranceDa(referenceMz);
        }

        internal int InclusionListSize
        {
            get
            {
                lock (_listLock)
                {
                    return _inclusionList.Count;
                }
            }
        }

        internal void PrintStatistics()
        {
            Console.WriteLine("\n=== Dynamic MS/MS Trigger Summary ===");
            Console.WriteLine($"Precursor ions queued: {_totalAdded}");
            Console.WriteLine($"Duplicate precursor ions skipped: {_duplicatePrecursorsSkipped}");
            Console.WriteLine($"Instrument-list updates sent: {_totalUpdates}");
            Console.WriteLine($"Final in-memory inclusion-list size: {InclusionListSize}");
            if (InclusionListSize > 0)
            {
                Console.WriteLine("Recent inclusion-list entries:");
                foreach (var entry in _inclusionList.OrderByDescending(e => e.AddedTime).Take(5))
                {
                    var age = (DateTime.Now - entry.AddedTime).TotalSeconds;
                    Console.WriteLine($"  m/z={entry.PrecursorMz:F4}, RT={entry.RetentionTime:F2} min, age={age:F1} s");
                }
            }
        }

        public void Dispose()
        {
            try
            {
                Stop();
            }
            catch { }
        }

        private class InclusionEntry
        {
            public double PrecursorMz { get; set; }
            public double RetentionTime { get; set; }
            public DateTime AddedTime { get; set; }
            public string Comment { get; set; }
        }
    }
}
