# Android platform-tools (bundled, multi-OS)

Lege die ADB-Dateien in OS-spezifische Unterordner:

- Windows: `src-tauri/resources/platform-tools/windows/`
- Linux: `src-tauri/resources/platform-tools/linux/`

## Windows

Mindestens:

- `adb.exe`
- `AdbWinApi.dll`
- `AdbWinUsbApi.dll`

## Linux

Mindestens:

- `adb`

Zusätzlich muss die Datei ausführbar sein:

```bash
chmod +x src-tauri/resources/platform-tools/linux/adb
```

## Hinweis

Die App sucht jetzt zuerst in diesen OS-Unterordnern. Danach wird auf PATH (`adb`) zurückgefallen.
