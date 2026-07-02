# Google USB Driver Placeholder

Place Google USB driver files here for automated install attempts by ClientController:

- android_winusb.inf
- androidwinusb86.cat
- androidwinusba64.cat

The app will try to run:

pnputil /add-driver <path to android_winusb.inf> /install

If this fails, run ClientController (or terminal) as Administrator and install manually in Device Manager.
