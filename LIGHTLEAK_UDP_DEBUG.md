# Lightleak UDP read debugging

## Finding

The supplied 2026-11-17 diagnostic logs show the exploit stages completing, followed by repeated reads of the first chunk at offset `0x0` and an empty dump. The checked-in diagnostic patch adds useful TX/RX logging, but its UDP sockets are not associated with the Wi-Fi network selected by Android's `NetworkRequest`. Binding a listener to `0.0.0.0` only chooses a local interface; it does not select the network used for sending or receiving. The original 10 ms polling loop also spins while waiting for the listener coroutine to enqueue a reply.

The diagnostic log contains no `CLOUDCUTTER_UDP_DIAG_V1` entries, so it appears to predate a build with that instrumentation. It cannot establish whether packets left the phone or reached the app's listener.

## Changes

- Pass the selected Wi-Fi `Network` from `NetworkAwareFragment` through `LightleakData` to `LightleakService`.
- Bind the UDP receive socket on port 6667 and each request socket on port 6669 to that network. The legacy Android path uses `ConnectivityManager.setProcessDefaultNetwork` because per-socket binding is unavailable before API 23.
- Log listener bind failures, received packet size, CRC result, source address, request destination, return IP, and decoded offsets under `CLOUDCUTTER_UDP_DIAG_V1`.
- Yield briefly when no reply is queued, validate response lengths before slicing, and restore the original eight-packet window and 10 ms collection interval after the slower diagnostic settings.
- Declare `kotlinx-coroutines-android` explicitly for Android coroutine dispatchers.

## Build

Build the debug APK with:

```powershell
.\gradlew.bat assembleDebug
```

Output: `app/build/outputs/apk/debug/app-debug.apk`.

The APK compiles successfully. It has not been installed or exercised against the bulb; verify the next read using the diagnostic log and check for a listener bind line followed by valid UDP RX entries. No bulb firmware changes or ADB install/uninstall operations were performed.
