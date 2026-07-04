#[cfg(target_os = "windows")]
fn stop_stale_project_adb() {
    use std::os::windows::process::CommandExt;
    use std::process::Command;

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR must be available to the Tauri build script");
    let script = r#"
$ErrorActionPreference = 'Stop'
$roots = @()
$roots += [IO.Path]::GetFullPath((Join-Path $env:FOLLOWFLOW_TAURI_ROOT 'target')).TrimEnd('\') + '\'
$roots += [IO.Path]::GetFullPath((Join-Path $env:FOLLOWFLOW_TAURI_ROOT 'resources')).TrimEnd('\') + '\'
$stopped = @()

Get-CimInstance Win32_Process -Filter "Name = 'adb.exe'" | ForEach-Object {
    $executable = $_.ExecutablePath
    if ($executable -and ($roots | Where-Object {
        $executable.StartsWith($_, [StringComparison]::OrdinalIgnoreCase)
    })) {
        Stop-Process -Id $_.ProcessId -Force
        $stopped += "$($_.ProcessId):$executable"
    }
}

if ($stopped.Count -gt 0) {
    Write-Output ($stopped -join '; ')
}
"#;

    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("FOLLOWFLOW_TAURI_ROOT", manifest_dir)
        .creation_flags(0x08000000)
        .output()
        .expect("failed to launch PowerShell for stale project ADB cleanup");

    if !output.status.success() {
        panic!(
            "failed to stop stale project ADB before copying Tauri resources: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let stopped = String::from_utf8_lossy(&output.stdout);
    let stopped = stopped.trim();
    if !stopped.is_empty() {
        println!("cargo:warning=Stopped stale project ADB process(es): {stopped}");
    }
}

fn main() {
    #[cfg(target_os = "windows")]
    stop_stale_project_adb();

    tauri_build::build()
}
