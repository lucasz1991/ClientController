# Driver Resources

This folder is bundled into the Tauri app and is used for optional automatic USB/ADB driver setup on Windows.

Expected structure:

- drivers/google-usb-driver/android_winusb.inf
- drivers/google-usb-driver/androidwinusb86.cat
- drivers/google-usb-driver/androidwinusba64.cat

You can obtain the Google USB Driver from Android SDK Manager or Android developer downloads.

Note:
- Driver installation on Windows requires Administrator rights.
- Some devices still require OEM-specific drivers.
