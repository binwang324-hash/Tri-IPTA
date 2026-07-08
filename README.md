# Tri-IPTA IAPI Real-Time MS/MS Triggering

This repository contains the C# source code used for real-time triple-isotope probe-triggered acquisition (Tri-IPTA) on a Thermo Exactive-series Orbitrap mass spectrometer.

The program listens to centroided scan data through the Thermo Fisher Scientific Instrument Application Programming Interface (IAPI), recognizes co-eluting `d0/d2/d8` isotope triplets in full-scan MS1 data, and dynamically appends the corresponding `d0` precursor ion to the instrument inclusion list for targeted MS/MS acquisition.

## Requirements

- Windows 10 or Windows 11
- Visual Studio with .NET Framework build tools, or a compatible `dotnet build` setup
- .NET Framework 4.6
- Thermo Fisher Scientific Exactive-series IAPI access and libraries
- A Thermo instrument method configured to allow inclusion-list-based or preferred precursor fragmentation during DDA acquisition

See [DEPENDENCIES.md](DEPENDENCIES.md) and [Dependencies/README.md](Dependencies/README.md) for vendor library details.

## Building

Place the required Thermo IAPI libraries in the local `Dependencies/` directory and build `DataListening.sln` using Visual Studio.


## Running

Run in MS1-only detection mode:

```powershell
TriIPTAIAPI.exe
```

Run with dynamic MS/MS triggering enabled:

```powershell
TriIPTAIAPI.exe -ms2
```

When `-ms2` is enabled, validated `d0` precursor ions are added to the instrument inclusion list through the IAPI method-control interface. The active instrument method must be configured so that inclusion-list entries can guide MS/MS acquisition.

Triplet matching uses a 5 ppm mass tolerance and a retention-time tolerance of 5 s. For MS/MS triggering, the triplet intensity ratio `max(d0,d2,d8)/min(d0,d2,d8)` must be no greater than 10.
