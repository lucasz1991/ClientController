use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use hmac::{Hmac, Mac};
use reqwest::blocking::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tar::{Archive, Builder};
use tauri::Manager;

const DEFAULT_SERVER_DOMAIN: &str = "https://factory.follow-flow.de";
const DEFAULT_BOOTSTRAP_API_KEY: &str = "followflow-default-node-key-change-me";
const GOOGLE_USB_DRIVER_ZIP_URL: &str =
    "https://dl.google.com/android/repository/latest_usb_driver_windows.zip";
static WINDOWS_DRIVER_INSTALL_ATTEMPTED: AtomicBool = AtomicBool::new(false);
static LOCAL_PROCESS_RECOVERY_PERFORMED: AtomicBool = AtomicBool::new(false);
static WORKFLOW_JOB_RUNNING: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
struct ClientConfig {
    server_domain: String,
    node_uuid: String,
    node_key: String,
    api_key: String,
    bootstrap_api_key: String,
    environment: String,
    allow_server_rebind: bool,
    adb_enabled: bool,
    adb_device_discovery_enabled: bool,
    last_successful_server: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_domain: DEFAULT_SERVER_DOMAIN.to_string(),
            node_uuid: format!("node-{}", Utc::now().timestamp_millis()),
            node_key: format!("local-key-{}", Utc::now().timestamp_millis()),
            api_key: DEFAULT_BOOTSTRAP_API_KEY.to_string(),
            bootstrap_api_key: DEFAULT_BOOTSTRAP_API_KEY.to_string(),
            environment: "production".to_string(),
            allow_server_rebind: true,
            adb_enabled: true,
            adb_device_discovery_enabled: true,
            last_successful_server: DEFAULT_SERVER_DOMAIN.to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ClientStatus {
    config: ClientConfig,
    pending_events: i64,
    local_devices: i64,
    adb_source: String,
    adb_available: bool,
    db_path: String,
    config_path: String,
    node_available: bool,
    workflow_runtime_available: bool,
    workflow_runtime_path: String,
    app_version: String,
    running_processes: i64,
    cpu_load_percent: Option<f64>,
    updater_available: bool,
}

#[derive(Debug, Deserialize)]
struct RebindRequest {
    new_server_domain: String,
    expires_at: String,
    signature: String,
}

#[derive(Debug, Deserialize)]
struct AdbSettingsUpdate {
    adb_enabled: bool,
    adb_device_discovery_enabled: bool,
}

#[derive(Debug, Serialize)]
struct GenericResult {
    success: bool,
    message: String,
}

#[derive(Debug, Serialize)]
struct OutboxEvent {
    id: i64,
    event_type: String,
    payload_json: String,
    created_at: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LocalDevice {
    id: i64,
    device_uuid: String,
    name: String,
    platform: String,
    adb_serial: Option<String>,
    status: String,
    last_seen_at: String,
    raw_json: String,
}

#[derive(Debug, Serialize)]
struct SyncSummary {
    registered: bool,
    discovered_devices: usize,
    synced_devices: usize,
    heartbeat_sent: bool,
    jobs_started: usize,
    message: String,
}

#[derive(Debug, Serialize)]
struct LocalProcess {
    id: i64,
    job_id: Option<String>,
    job_type: String,
    status: String,
    details_json: String,
    created_at: String,
}

#[derive(Debug, Serialize)]
struct WorkflowProcessPreview {
    job_uuid: String,
    status: Value,
    result: Value,
    checkpoint: Value,
    workflow_steps: Value,
    screenshot_data_url: Option<String>,
    stdout_tail: String,
    stderr_tail: String,
    run_directory: String,
}

#[derive(Debug, Deserialize, Clone)]
struct RemoteJob {
    job_uuid: String,
    #[serde(rename = "type")]
    job_type: String,
    payload: Value,
    #[serde(default)]
    signature: String,
    #[serde(default)]
    device_uuid: Option<String>,
    #[serde(default)]
    execution_scope: String,
    #[serde(default = "default_payload_version")]
    payload_version: u64,
    #[serde(default)]
    lease_token: String,
    #[serde(default)]
    lease_expires_at: Option<String>,
    #[serde(default)]
    control: Option<RemoteControl>,
}

#[derive(Debug, Deserialize, Clone)]
struct RemoteControl {
    command: String,
    #[serde(default)]
    sequence: i64,
    #[serde(default)]
    payload: Value,
}

#[derive(Debug)]
struct DeliveryAck {
    acknowledged_sequence: i64,
    control: Option<RemoteControl>,
}

fn default_payload_version() -> u64 {
    1
}

fn ensure_runtime_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| format!("app_data_dir unavailable: {e}"))?;

    let runtime_dir = base.join("runtime");
    fs::create_dir_all(&runtime_dir).map_err(|e| format!("cannot create runtime dir: {e}"))?;
    Ok(runtime_dir)
}

fn tooling_runtime_root(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(ensure_runtime_dir(app)?.join("tooling"))
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), String> {
    if !src.exists() {
        return Ok(());
    }

    fs::create_dir_all(dst).map_err(|e| {
        format!(
            "cannot create destination directory {}: {}",
            dst.to_string_lossy(),
            e
        )
    })?;

    for entry in fs::read_dir(src)
        .map_err(|e| format!("cannot read directory {}: {}", src.to_string_lossy(), e))?
    {
        let entry = entry.map_err(|e| format!("read_dir entry error: {e}"))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let file_type = entry.file_type().map_err(|e| {
            format!(
                "cannot read file type for {}: {}",
                src_path.to_string_lossy(),
                e
            )
        })?;

        if file_type.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else if file_type.is_file() {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    format!(
                        "cannot create parent directory {}: {}",
                        parent.to_string_lossy(),
                        e
                    )
                })?;
            }
            fs::copy(&src_path, &dst_path).map_err(|e| {
                format!(
                    "cannot copy {} -> {}: {}",
                    src_path.to_string_lossy(),
                    dst_path.to_string_lossy(),
                    e
                )
            })?;
        }
    }

    Ok(())
}

fn find_bundled_subdir(app: &tauri::AppHandle, subdir: &str) -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir.join(subdir));
        candidates.push(resource_dir.join("resources").join(subdir));
    }

    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("src-tauri").join("resources").join(subdir));
        candidates.push(cwd.join("resources").join(subdir));
    }

    candidates.into_iter().find(|p| p.exists() && p.is_dir())
}

fn stage_bundled_tooling_best_effort(app: &tauri::AppHandle) {
    let Ok(tool_root) = tooling_runtime_root(app) else {
        return;
    };

    let _ = fs::create_dir_all(&tool_root);

    for sub in ["platform-tools", "drivers"] {
        if let Some(src_dir) = find_bundled_subdir(app, sub) {
            let dst_dir = tool_root.join(sub);
            let _ = copy_dir_recursive(&src_dir, &dst_dir);
        }
    }
}

fn config_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(ensure_runtime_dir(app)?.join("client.json"))
}

fn db_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(ensure_runtime_dir(app)?.join("client_local.db"))
}

fn load_or_create_config(app: &tauri::AppHandle) -> Result<ClientConfig, String> {
    let path = config_path(app)?;

    if !path.exists() {
        let cfg = ClientConfig::default();
        let content = serde_json::to_string_pretty(&cfg)
            .map_err(|e| format!("serialize default config failed: {e}"))?;
        fs::write(&path, content).map_err(|e| format!("write default config failed: {e}"))?;
        return Ok(cfg);
    }

    let raw = fs::read_to_string(&path).map_err(|e| format!("read config failed: {e}"))?;
    let mut cfg: ClientConfig =
        serde_json::from_str(&raw).map_err(|e| format!("parse config failed: {e}"))?;

    if cfg.bootstrap_api_key.trim().is_empty() {
        cfg.bootstrap_api_key = DEFAULT_BOOTSTRAP_API_KEY.to_string();
    }

    if cfg.api_key.trim().is_empty() {
        cfg.api_key = cfg.bootstrap_api_key.clone();
    }

    if normalize_config_domains(&mut cfg) {
        save_config(app, &cfg)?;
    }

    Ok(cfg)
}

fn save_config(app: &tauri::AppHandle, cfg: &ClientConfig) -> Result<(), String> {
    let path = config_path(app)?;
    let content =
        serde_json::to_string_pretty(cfg).map_err(|e| format!("serialize config failed: {e}"))?;
    fs::write(path, content).map_err(|e| format!("save config failed: {e}"))
}

fn adb_enabled(app: &tauri::AppHandle) -> bool {
    load_or_create_config(app)
        .map(|cfg| cfg.adb_enabled)
        .unwrap_or(true)
}

fn adb_device_discovery_enabled(app: &tauri::AppHandle) -> bool {
    load_or_create_config(app)
        .map(|cfg| cfg.adb_enabled && cfg.adb_device_discovery_enabled)
        .unwrap_or(true)
}

fn open_db(app: &tauri::AppHandle) -> Result<Connection, String> {
    let path = db_path(app)?;
    Connection::open(path).map_err(|e| format!("open db failed: {e}"))
}

fn init_db(app: &tauri::AppHandle) -> Result<(), String> {
    let conn = open_db(app)?;

    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS outbox_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            retry_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            sent_at TEXT
        );

        CREATE TABLE IF NOT EXISTS job_delivery_state (
            job_uuid TEXT PRIMARY KEY,
            lease_token TEXT NOT NULL DEFAULT '',
            last_sequence INTEGER NOT NULL DEFAULT 0,
            last_screenshot_hash TEXT,
            control_command TEXT,
            control_sequence INTEGER NOT NULL DEFAULT 0,
            control_payload_json TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS job_executions_local (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS heartbeat_logs_local (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rebind_logs_local (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            old_server_domain TEXT,
            new_server_domain TEXT,
            status TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS local_devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_uuid TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            platform TEXT NOT NULL,
            adb_serial TEXT,
            status TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );
        "#,
    )
    .map_err(|e| format!("init db schema failed: {e}"))?;

    for column in [
        "job_uuid TEXT",
        "sequence INTEGER",
        "next_attempt_at TEXT",
        "last_error TEXT",
        "screenshot_path TEXT",
    ] {
        let _ = conn.execute(
            &format!("ALTER TABLE outbox_events ADD COLUMN {column}"),
            [],
        );
    }

    for column in [
        "control_command TEXT",
        "control_sequence INTEGER NOT NULL DEFAULT 0",
        "control_payload_json TEXT",
    ] {
        let _ = conn.execute(
            &format!("ALTER TABLE job_delivery_state ADD COLUMN {column}"),
            [],
        );
    }

    if !LOCAL_PROCESS_RECOVERY_PERFORMED.swap(true, Ordering::SeqCst) {
        conn.execute(
            "UPDATE job_executions_local
             SET status = 'interrupted', details_json = ?1
             WHERE status = 'running'",
            params![json!({
                "statusMessage": "ClientController wurde während des Prozesses neu gestartet.",
                "recoveredAt": now_iso(),
            })
            .to_string()],
        )
        .map_err(|e| format!("recover interrupted local processes failed: {e}"))?;
    }

    Ok(())
}

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

fn base_url(domain: &str) -> String {
    domain.trim().trim_end_matches('/').to_string()
}

fn canonical_server_domain(input: &str) -> String {
    let mut domain = base_url(input);

    if domain.is_empty() {
        return DEFAULT_SERVER_DOMAIN.to_string();
    }

    if domain.eq_ignore_ascii_case("https://factory.followflow.de") {
        domain = DEFAULT_SERVER_DOMAIN.to_string();
    }

    if domain.contains("example.com") {
        domain = DEFAULT_SERVER_DOMAIN.to_string();
    }

    domain
}

fn normalize_config_domains(cfg: &mut ClientConfig) -> bool {
    let mut changed = false;

    let normalized_server = canonical_server_domain(&cfg.server_domain);
    if normalized_server != cfg.server_domain {
        cfg.server_domain = normalized_server;
        changed = true;
    }

    if cfg.last_successful_server.trim().is_empty() {
        cfg.last_successful_server = cfg.server_domain.clone();
        changed = true;
    } else {
        let normalized_last = canonical_server_domain(&cfg.last_successful_server);
        if normalized_last != cfg.last_successful_server {
            cfg.last_successful_server = normalized_last;
            changed = true;
        }
    }

    changed
}

fn preview_body(input: &str, max_chars: usize) -> String {
    let preview: String = input.chars().take(max_chars).collect();
    if input.chars().count() > max_chars {
        format!("{}...", preview)
    } else {
        preview
    }
}

fn http_client() -> Result<Client, String> {
    Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|e| format!("http client init failed: {e}"))
}

fn queue_local_event(
    app: &tauri::AppHandle,
    event_type: &str,
    payload: Value,
) -> Result<(), String> {
    init_db(app)?;
    let conn = open_db(app)?;
    let payload_json =
        serde_json::to_string(&payload).map_err(|e| format!("serialize payload failed: {e}"))?;

    conn.execute(
        "INSERT INTO outbox_events (event_type, payload_json, status, created_at) VALUES (?1, ?2, 'pending', ?3)",
        params![event_type, payload_json, now_iso()],
    )
    .map_err(|e| format!("insert outbox event failed: {e}"))?;

    Ok(())
}

fn adb_executable_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "adb.exe"
    } else {
        "adb"
    }
}

fn platform_tools_subdir() -> &'static str {
    if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else {
        "linux"
    }
}

fn candidate_adb_paths(app: &tauri::AppHandle) -> Vec<PathBuf> {
    let adb_name = adb_executable_name();
    let os_subdir = platform_tools_subdir();
    let mut candidates: Vec<PathBuf> = Vec::new();

    // Preferred: staged runtime tooling (prevents file locks on src-tauri/resources during builds)
    if let Ok(tool_root) = tooling_runtime_root(app) {
        candidates.push(
            tool_root
                .join("platform-tools")
                .join(os_subdir)
                .join(adb_name),
        );
        // legacy flat fallback in runtime
        candidates.push(tool_root.join("platform-tools").join(adb_name));
    }

    // Packaged resource directory (installed app)
    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(
            resource_dir
                .join("platform-tools")
                .join(os_subdir)
                .join(adb_name),
        );
        candidates.push(
            resource_dir
                .join("resources")
                .join("platform-tools")
                .join(os_subdir)
                .join(adb_name),
        );

        // legacy flat fallback
        candidates.push(resource_dir.join("platform-tools").join(adb_name));
        candidates.push(
            resource_dir
                .join("resources")
                .join("platform-tools")
                .join(adb_name),
        );
    }

    // Executable relative paths (installed / portable layouts)
    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            candidates.push(
                exe_dir
                    .join("platform-tools")
                    .join(os_subdir)
                    .join(adb_name),
            );
            candidates.push(
                exe_dir
                    .join("resources")
                    .join("platform-tools")
                    .join(os_subdir)
                    .join(adb_name),
            );

            // legacy flat fallback
            candidates.push(exe_dir.join("platform-tools").join(adb_name));
            candidates.push(
                exe_dir
                    .join("resources")
                    .join("platform-tools")
                    .join(adb_name),
            );
        }
    }

    candidates.sort();
    candidates.dedup();
    candidates
}

#[cfg(target_os = "windows")]
fn stop_bundled_adb_processes(app: &tauri::AppHandle) {
    if !adb_enabled(app) {
        return;
    }

    let executable_paths = candidate_adb_paths(app)
        .into_iter()
        .filter(|path| path.is_file())
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("\n");

    if executable_paths.is_empty() {
        return;
    }

    let script = r#"
$ErrorActionPreference = 'Stop'
$adbPaths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
$env:FOLLOWFLOW_ADB_PATHS -split "`n" | Where-Object { $_ } | ForEach-Object {
    [void] $adbPaths.Add([IO.Path]::GetFullPath($_))
}

Get-CimInstance Win32_Process -Filter "Name = 'adb.exe'" | ForEach-Object {
    if ($_.ExecutablePath -and $adbPaths.Contains([IO.Path]::GetFullPath($_.ExecutablePath))) {
        Stop-Process -Id $_.ProcessId -Force
    }
}
"#;

    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("FOLLOWFLOW_ADB_PATHS", executable_paths)
        .creation_flags(0x08000000)
        .output();

    match output {
        Ok(output) if output.status.success() => {}
        Ok(output) => eprintln!(
            "Failed to stop bundled ADB process(es) on exit: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        Err(error) => eprintln!("Failed to run bundled ADB exit cleanup: {error}"),
    }
}

#[cfg(not(target_os = "windows"))]
fn stop_bundled_adb_processes(app: &tauri::AppHandle) {
    if !adb_enabled(app) {
        return;
    }

    if let Some(path) = candidate_adb_paths(app)
        .into_iter()
        .find(|path| path.is_file())
    {
        let _ = Command::new(path).arg("kill-server").output();
    }
}

fn resolve_adb(app: &tauri::AppHandle) -> (Option<PathBuf>, String, Vec<String>) {
    stage_bundled_tooling_best_effort(app);
    let candidates = candidate_adb_paths(app);
    let checked: Vec<String> = candidates
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    for candidate in candidates {
        if candidate.exists() {
            return (
                Some(candidate.clone()),
                format!("bundled/local: {}", candidate.to_string_lossy()),
                checked,
            );
        }
    }

    let path_probe = Command::new("adb").arg("version").output();
    if let Ok(out) = path_probe {
        if out.status.success() {
            return (None, "system-path: adb".to_string(), checked);
        }
    }

    (None, "not-found".to_string(), checked)
}

fn detect_adb_source(app: &tauri::AppHandle) -> (String, bool) {
    if !adb_enabled(app) {
        return ("deaktiviert".to_string(), false);
    }

    let (_path, source, _checked) = resolve_adb(app);
    let available = source != "not-found";
    (source, available)
}

fn candidate_driver_inf_paths(app: &tauri::AppHandle) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    // Preferred: staged runtime tooling
    if let Ok(tool_root) = tooling_runtime_root(app) {
        candidates.push(
            tool_root
                .join("drivers")
                .join("google-usb-driver")
                .join("android_winusb.inf"),
        );
    }

    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(
            resource_dir
                .join("drivers")
                .join("google-usb-driver")
                .join("android_winusb.inf"),
        );
        candidates.push(
            resource_dir
                .join("resources")
                .join("drivers")
                .join("google-usb-driver")
                .join("android_winusb.inf"),
        );
    }

    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(
            cwd.join("src-tauri")
                .join("resources")
                .join("drivers")
                .join("google-usb-driver")
                .join("android_winusb.inf"),
        );
        candidates.push(
            cwd.join("resources")
                .join("drivers")
                .join("google-usb-driver")
                .join("android_winusb.inf"),
        );
    }

    candidates
}

fn bundled_google_usb_inf(app: &tauri::AppHandle) -> Option<PathBuf> {
    candidate_driver_inf_paths(app)
        .into_iter()
        .find(|p| p.exists())
}

fn find_file_recursive(root: &Path, filename: &str) -> Option<PathBuf> {
    if !root.exists() {
        return None;
    }

    let entries = fs::read_dir(root).ok()?;
    for entry in entries {
        let entry = entry.ok()?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_file_recursive(&path, filename) {
                return Some(found);
            }
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.eq_ignore_ascii_case(filename))
            .unwrap_or(false)
        {
            return Some(path);
        }
    }

    None
}

fn ps_quote_literal(input: &str) -> String {
    input.replace("'", "''")
}

fn ensure_windows_google_usb_driver_available(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    if !cfg!(target_os = "windows") {
        return Err("driver bootstrap skipped (non-windows)".to_string());
    }

    if let Some(existing) = bundled_google_usb_inf(app) {
        return Ok(existing);
    }

    let tool_root = tooling_runtime_root(app)?;
    let drivers_root = tool_root.join("drivers");
    let final_driver_dir = drivers_root.join("google-usb-driver");
    let final_inf = final_driver_dir.join("android_winusb.inf");

    fs::create_dir_all(&drivers_root)
        .map_err(|e| format!("cannot create drivers runtime dir: {e}"))?;

    if final_inf.exists() {
        return Ok(final_inf);
    }

    let zip_path = drivers_root.join("google-usb-driver.zip");
    let extract_root = drivers_root.join("google-usb-driver-extracted");

    let zip_ps = ps_quote_literal(&zip_path.to_string_lossy());
    let url_ps = ps_quote_literal(GOOGLE_USB_DRIVER_ZIP_URL);

    let download_script = format!(
        "$ProgressPreference='SilentlyContinue'; Invoke-WebRequest -UseBasicParsing -Uri '{}' -OutFile '{}'",
        url_ps, zip_ps
    );

    let dl = Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &download_script,
        ])
        .output()
        .map_err(|e| format!("powershell Invoke-WebRequest failed: {e}"))?;

    if !dl.status.success() {
        let stderr = String::from_utf8_lossy(&dl.stderr).to_string();
        let stdout = String::from_utf8_lossy(&dl.stdout).to_string();
        return Err(format!(
            "google usb driver download failed. stdout: {} stderr: {}",
            preview_body(&stdout, 400),
            preview_body(&stderr, 400)
        ));
    }

    if extract_root.exists() {
        let _ = fs::remove_dir_all(&extract_root);
    }
    fs::create_dir_all(&extract_root).map_err(|e| format!("cannot create extract dir: {e}"))?;

    let extract_ps_zip = ps_quote_literal(&zip_path.to_string_lossy());
    let extract_ps_dst = ps_quote_literal(&extract_root.to_string_lossy());
    let extract_script = format!(
        "Expand-Archive -LiteralPath '{}' -DestinationPath '{}' -Force",
        extract_ps_zip, extract_ps_dst
    );

    let ex = Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &extract_script,
        ])
        .output()
        .map_err(|e| format!("powershell Expand-Archive failed: {e}"))?;

    if !ex.status.success() {
        let stderr = String::from_utf8_lossy(&ex.stderr).to_string();
        let stdout = String::from_utf8_lossy(&ex.stdout).to_string();
        return Err(format!(
            "google usb driver extraction failed. stdout: {} stderr: {}",
            preview_body(&stdout, 400),
            preview_body(&stderr, 400)
        ));
    }

    let found_inf = find_file_recursive(&extract_root, "android_winusb.inf").ok_or_else(|| {
        "downloaded driver archive did not contain android_winusb.inf".to_string()
    })?;

    let inf_parent = found_inf
        .parent()
        .ok_or_else(|| "invalid extracted inf parent path".to_string())?
        .to_path_buf();

    if final_driver_dir.exists() {
        let _ = fs::remove_dir_all(&final_driver_dir);
    }
    fs::create_dir_all(&final_driver_dir)
        .map_err(|e| format!("cannot create normalized driver dir: {e}"))?;

    copy_dir_recursive(&inf_parent, &final_driver_dir)?;

    if !final_inf.exists() {
        return Err(
            "driver download/extract finished but android_winusb.inf still missing".to_string(),
        );
    }

    Ok(final_inf)
}
fn try_install_windows_usb_driver_best_effort(app: &tauri::AppHandle) -> Result<String, String> {
    if !cfg!(target_os = "windows") {
        return Ok("driver install skipped (non-windows)".to_string());
    }

    let inf = ensure_windows_google_usb_driver_available(app)?;

    let inf_str = inf
        .to_str()
        .ok_or_else(|| "driver inf path contains invalid unicode".to_string())?
        .to_string();

    let output = Command::new("pnputil")
        .args(["/add-driver", &inf_str, "/install"])
        .output()
        .map_err(|e| format!("pnputil execution failed: {e}"))?;

    if output.status.success() {
        Ok(format!(
            "driver install attempted successfully via pnputil: {}",
            inf_str
        ))
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        Err(format!(
            "driver install failed. Run app/terminal as Administrator. stdout: {} stderr: {}",
            preview_body(&stdout, 500),
            preview_body(&stderr, 500)
        ))
    }
}

fn restart_adb_server(app: &tauri::AppHandle) {
    if !adb_enabled(app) {
        return;
    }

    let (adb_path, source, _checked) = resolve_adb(app);

    if let Some(path) = adb_path {
        let _ = Command::new(&path).args(["kill-server"]).output();
        let _ = Command::new(&path).args(["start-server"]).output();
    } else if source == "system-path: adb" {
        let _ = Command::new("adb").args(["kill-server"]).output();
        let _ = Command::new("adb").args(["start-server"]).output();
    }
}

fn adb_devices_raw_output(app: &tauri::AppHandle) -> Result<(String, String), String> {
    if !adb_enabled(app) {
        return Err("ADB-Steuerung ist lokal deaktiviert.".to_string());
    }

    let (adb_path, adb_source, checked) = resolve_adb(app);

    let output = if let Some(path) = adb_path {
        Command::new(&path)
            .args(["devices", "-l"])
            .output()
            .map_err(|e| format!("adb command failed via {}: {}", path.to_string_lossy(), e))?
    } else if adb_source == "system-path: adb" {
        Command::new("adb")
            .args(["devices", "-l"])
            .output()
            .map_err(|e| format!("adb command failed via system PATH: {e}"))?
    } else {
        return Err(format!(
            "ADB not found. Checked paths:\n- {}\n\nPlease place Android platform-tools in src-tauri/resources/platform-tools/{}/ ({} and required libs) or install adb in PATH.",
            if checked.is_empty() {
                "(no path candidates generated)".to_string()
            } else {
                checked.join("\n- ")
            },
            platform_tools_subdir(),
            adb_executable_name()
        ));
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(format!(
            "adb returned non-zero status via {}: {}",
            adb_source, stderr
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    Ok((stdout, adb_source))
}

fn discover_android_devices_internal(
    app: &tauri::AppHandle,
    allow_driver_attempt: bool,
) -> Result<Vec<LocalDevice>, String> {
    init_db(app)?;

    if !adb_enabled(app) {
        return Err("ADB-Steuerung ist lokal deaktiviert.".to_string());
    }

    if !adb_device_discovery_enabled(app) {
        return Err("ADB-Gerätesuche ist lokal deaktiviert.".to_string());
    }

    let (stdout, _) = adb_devices_raw_output(app)?;
    let mut devices = parse_adb_devices_output(&stdout);

    if cfg!(target_os = "windows") && devices.is_empty() && allow_driver_attempt {
        let first_attempt = !WINDOWS_DRIVER_INSTALL_ATTEMPTED.swap(true, Ordering::SeqCst);
        if first_attempt {
            let driver_msg = try_install_windows_usb_driver_best_effort(app)
                .unwrap_or_else(|e| format!("driver auto-install skipped/failed: {}", e));
            let _ = queue_local_event(
                app,
                "driver_install_attempt",
                json!({ "message": driver_msg }),
            );
            restart_adb_server(app);
            if let Ok((stdout2, _)) = adb_devices_raw_output(app) {
                devices = parse_adb_devices_output(&stdout2);
            }
        }
    }

    save_discovered_devices(app, &devices)?;
    load_local_devices_internal(app)
}

fn register_node_remote_internal(
    app: &tauri::AppHandle,
    node_name: Option<String>,
) -> Result<bool, String> {
    let mut cfg = load_or_create_config(app)?;
    let endpoint = format!(
        "{}/api/client-controller/register-node",
        base_url(&cfg.server_domain)
    );
    let workflow_ready = workflow_runtime_ready(app);

    let register_key = cfg.bootstrap_api_key.clone();
    let adb_capable = cfg.adb_enabled;
    let adb_device_discovery_capable = cfg.adb_enabled && cfg.adb_device_discovery_enabled;

    let payload = json!({
        "name": node_name.unwrap_or_else(|| format!("ClientNode-{}", &cfg.node_uuid)),
        "node_uuid": cfg.node_uuid,
        "version": env!("CARGO_PKG_VERSION"),
        "os": std::env::consts::OS,
        "current_server_domain": cfg.server_domain,
        "last_successful_server_domain": cfg.last_successful_server,
        "bootstrap_api_key": register_key,
        "capabilities": {
            "android": adb_capable,
            "adb": adb_capable,
            "adb_device_discovery": adb_device_discovery_capable,
            "remote_network": true,
            "screenshots": true,
            "browser": workflow_ready,
            "cloakbrowser": workflow_ready,
            "workflow_tasks": workflow_ready,
            "workflow_bundle_v1": workflow_ready,
            "job_protocol_version": 2,
            "node_execution": true,
            "appium": false,
            "server_rebind": true,
            "auto_update": true
        }
    });

    let client = http_client()?;
    let response = client
        .post(&endpoint)
        .header("X-BOOTSTRAP-API-KEY", register_key.clone())
        .header("X-NODE-API-KEY", register_key)
        .json(&payload)
        .send()
        .map_err(|e| format!("register request failed: {e}"))?;

    let status_code = response.status();
    let raw_body = response
        .text()
        .map_err(|e| format!("register response read failed: {e}"))?;

    if !status_code.is_success() {
        return Err(format!(
            "register failed: HTTP {} - {}",
            status_code.as_u16(),
            preview_body(&raw_body, 500)
        ));
    }

    let body: Value = serde_json::from_str(&raw_body).map_err(|e| {
        format!(
            "register response parse failed: {} (HTTP {}, body: {})",
            e,
            status_code.as_u16(),
            preview_body(&raw_body, 500)
        )
    })?;

    if !body
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(format!(
            "register failed: HTTP {} - {}",
            status_code.as_u16(),
            body
        ));
    }

    let api_key = body
        .get("node")
        .and_then(|n| n.get("api_key"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "register response did not contain a node api key".to_string())?;
    cfg.api_key = api_key.to_string();
    cfg.last_successful_server = cfg.server_domain.clone();
    save_config(app, &cfg)?;

    Ok(true)
}

fn authentication_failed(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();

    normalized.contains("http 401")
        || normalized.contains("http 403")
        || normalized.contains("unauthorized node")
        || normalized.contains("invalid node")
}

fn recover_node_registration(app: &tauri::AppHandle, error: &str) -> Result<bool, String> {
    if !authentication_failed(error) {
        return Ok(false);
    }

    register_node_remote_internal(app, None)?;

    Ok(true)
}

fn parse_first_number(raw: &str) -> Option<f64> {
    raw.split(|character: char| {
        !(character.is_ascii_digit() || character == '.' || character == ',')
    })
    .filter(|part| !part.trim().is_empty())
    .find_map(|part| part.replace(',', ".").parse::<f64>().ok())
}

fn normalize_cpu_percent(value: f64) -> Option<f64> {
    if !value.is_finite() {
        return None;
    }

    Some((value.clamp(0.0, 100.0) * 100.0).round() / 100.0)
}

#[cfg(target_os = "windows")]
fn local_cpu_load_percent() -> Option<f64> {
    let output = Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            "(Get-CimInstance Win32_Processor | Measure-Object -Property LoadPercentage -Average).Average",
        ])
        .creation_flags(0x08000000)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    normalize_cpu_percent(parse_first_number(&raw)?)
}

#[cfg(not(target_os = "windows"))]
fn local_cpu_load_percent() -> Option<f64> {
    let output = Command::new("sh")
        .args(["-c", "ps -A -o %cpu= | awk '{s+=$1} END {print s}'"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    let total = parse_first_number(&raw)?;
    let cpus = std::thread::available_parallelism()
        .map(|count| count.get().max(1) as f64)
        .unwrap_or(1.0);

    normalize_cpu_percent(total / cpus)
}

fn heartbeat_remote_internal(
    app: &tauri::AppHandle,
    status: &str,
    payload: Option<Value>,
) -> Result<bool, String> {
    let cfg = load_or_create_config(app)?;

    if cfg.api_key.trim().is_empty() {
        return Err("Missing api_key. Register node first.".to_string());
    }

    let endpoint = format!(
        "{}/api/client-controller/heartbeat",
        base_url(&cfg.server_domain)
    );
    let workflow_ready = workflow_runtime_ready(app);
    let adb_capable = cfg.adb_enabled;
    let adb_device_discovery_capable = cfg.adb_enabled && cfg.adb_device_discovery_enabled;
    let mut heartbeat_payload = payload.unwrap_or_else(|| json!({"source": "tauri-client"}));

    if let Some(object) = heartbeat_payload.as_object_mut() {
        object.insert(
            "metrics".to_string(),
            json!({
                "cpuLoadPercent": local_cpu_load_percent(),
                "reportedAt": now_iso(),
            }),
        );
    } else {
        heartbeat_payload = json!({
            "source": "tauri-client",
            "value": heartbeat_payload,
            "metrics": {
                "cpuLoadPercent": local_cpu_load_percent(),
                "reportedAt": now_iso(),
            },
        });
    }

    let body = json!({
        "status": status,
        "payload": heartbeat_payload,
        "version": env!("CARGO_PKG_VERSION"),
        "os": std::env::consts::OS,
        "current_server_domain": cfg.server_domain,
        "last_successful_server_domain": cfg.last_successful_server,
        "api_key": cfg.api_key,
        "capabilities": {
            "android": adb_capable,
            "adb": adb_capable,
            "adb_device_discovery": adb_device_discovery_capable,
            "remote_network": true,
            "screenshots": true,
            "browser": workflow_ready,
            "cloakbrowser": workflow_ready,
            "workflow_tasks": workflow_ready,
            "workflow_bundle_v1": workflow_ready,
            "job_protocol_version": 2,
            "node_execution": true,
            "server_rebind": true,
            "auto_update": true
        }
    });

    let client = http_client()?;
    let response = client
        .post(&endpoint)
        .header("X-NODE-API-KEY", cfg.api_key.clone())
        .json(&body)
        .send()
        .map_err(|e| format!("heartbeat request failed: {e}"))?;

    let status_code = response.status();
    let resp_body: Value = response
        .json()
        .map_err(|e| format!("heartbeat response parse failed: {e}"))?;

    if !status_code.is_success()
        || !resp_body
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Err(format!(
            "heartbeat failed: HTTP {} - {}",
            status_code.as_u16(),
            resp_body
        ));
    }

    Ok(true)
}

fn parse_adb_devices_output(raw: &str) -> Vec<LocalDevice> {
    let now = now_iso();
    let mut devices: Vec<LocalDevice> = Vec::new();

    for line in raw.lines() {
        let l = line.trim();
        if l.is_empty() || l.starts_with("List of devices attached") {
            continue;
        }

        if l.contains("\tdevice") || l.contains(" device ") {
            let mut serial = String::new();
            if let Some(first) = l.split_whitespace().next() {
                serial = first.to_string();
            }

            if serial.is_empty() {
                continue;
            }

            let model = l
                .split_whitespace()
                .find_map(|part| part.strip_prefix("model:"))
                .unwrap_or("Android Device");

            let raw_json = json!({ "adb_line": l }).to_string();
            devices.push(LocalDevice {
                id: 0,
                device_uuid: serial.clone(),
                name: model.replace('_', " "),
                platform: "android".to_string(),
                adb_serial: Some(serial),
                status: "online".to_string(),
                last_seen_at: now.clone(),
                raw_json,
            });
        }
    }

    devices
}

fn save_discovered_devices(
    app: &tauri::AppHandle,
    discovered: &[LocalDevice],
) -> Result<(), String> {
    init_db(app)?;
    let conn = open_db(app)?;

    conn.execute(
        "UPDATE local_devices SET status = 'offline' WHERE platform = 'android'",
        [],
    )
    .map_err(|e| format!("mark old devices offline failed: {e}"))?;

    for d in discovered {
        conn.execute(
            r#"
            INSERT INTO local_devices (device_uuid, name, platform, adb_serial, status, last_seen_at, raw_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(device_uuid) DO UPDATE SET
                name = excluded.name,
                platform = excluded.platform,
                adb_serial = excluded.adb_serial,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at,
                raw_json = excluded.raw_json
            "#,
            params![
                d.device_uuid,
                d.name,
                d.platform,
                d.adb_serial,
                d.status,
                d.last_seen_at,
                d.raw_json
            ],
        )
        .map_err(|e| format!("upsert local device failed: {e}"))?;
    }

    Ok(())
}

fn load_local_devices_internal(app: &tauri::AppHandle) -> Result<Vec<LocalDevice>, String> {
    init_db(app)?;
    let conn = open_db(app)?;

    let mut stmt = conn
        .prepare(
            "SELECT id, device_uuid, name, platform, adb_serial, status, last_seen_at, raw_json
             FROM local_devices
             ORDER BY status DESC, name ASC",
        )
        .map_err(|e| format!("prepare load devices failed: {e}"))?;

    let rows = stmt
        .query_map([], |row| {
            Ok(LocalDevice {
                id: row.get(0)?,
                device_uuid: row.get(1)?,
                name: row.get(2)?,
                platform: row.get(3)?,
                adb_serial: row.get(4)?,
                status: row.get(5)?,
                last_seen_at: row.get(6)?,
                raw_json: row.get(7)?,
            })
        })
        .map_err(|e| format!("query load devices failed: {e}"))?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row.map_err(|e| format!("read device row failed: {e}"))?);
    }

    Ok(result)
}

fn sync_devices_remote_internal(app: &tauri::AppHandle) -> Result<usize, String> {
    let cfg = load_or_create_config(app)?;
    if cfg.api_key.trim().is_empty() {
        return Err("Missing api_key. Register node first.".to_string());
    }

    let local_devices = load_local_devices_internal(app)?;
    let endpoint = format!(
        "{}/api/client-controller/sync-devices",
        base_url(&cfg.server_domain)
    );

    let payload = json!({
        "api_key": cfg.api_key,
        "devices": local_devices.iter().map(|d| json!({
            "name": d.name,
            "platform": d.platform,
            "device_uuid": d.device_uuid,
            "adb_serial": d.adb_serial,
            "status": d.status,
            "settings_json": {
                "raw": d.raw_json
            }
        })).collect::<Vec<Value>>()
    });

    let client = http_client()?;
    let response = client
        .post(&endpoint)
        .header("X-NODE-API-KEY", cfg.api_key)
        .json(&payload)
        .send()
        .map_err(|e| format!("sync-devices request failed: {e}"))?;

    let status_code = response.status();
    let body: Value = response
        .json()
        .map_err(|e| format!("sync-devices response parse failed: {e}"))?;

    if !status_code.is_success()
        || !body
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Err(format!(
            "sync-devices failed: HTTP {} - {}",
            status_code.as_u16(),
            body
        ));
    }

    let count = body
        .get("synced_count")
        .and_then(Value::as_u64)
        .unwrap_or(local_devices.len() as u64) as usize;

    Ok(count)
}

fn bundled_workflow_node_binary(runtime_root: &Path) -> Option<PathBuf> {
    let name = if cfg!(target_os = "windows") {
        "node.exe"
    } else {
        "node"
    };
    let binary = runtime_root.join("bin").join(name);

    binary.is_file().then_some(binary)
}

fn bundled_cloakbrowser_binary(runtime_root: &Path) -> Option<PathBuf> {
    let name = if cfg!(target_os = "windows") {
        "chrome.exe"
    } else {
        "chrome"
    };

    find_file_recursive(&runtime_root.join(".cloakbrowser"), name)
}

fn workflow_modules_ready(modules_root: &Path) -> bool {
    [
        "puppeteer",
        "puppeteer-core",
        "puppeteer-extra",
        "puppeteer-extra-plugin-stealth",
        "cloakbrowser",
    ]
    .iter()
    .all(|package| modules_root.join(package).join("package.json").is_file())
}

fn workflow_runtime_ready(app: &tauri::AppHandle) -> bool {
    let workflow_runtime = resolve_workflow_runtime(app);

    workflow_runtime.as_deref().is_some_and(|runtime_root| {
        bundled_workflow_node_binary(runtime_root).is_some()
            && bundled_cloakbrowser_binary(runtime_root).is_some()
            && runtime_root.join("node_modules.tar.gz").is_file()
    })
}

fn workflow_runtime_candidates(app: &tauri::AppHandle) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir.join("workflow-runtime"));
        candidates.push(resource_dir.join("resources").join("workflow-runtime"));
    }

    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(
            cwd.join("src-tauri")
                .join("resources")
                .join("workflow-runtime"),
        );
        candidates.push(cwd.join("resources").join("workflow-runtime"));
        candidates.push(cwd.join("..").join("AiUserFactory"));
    }

    candidates
}

fn resolve_workflow_runtime(app: &tauri::AppHandle) -> Option<PathBuf> {
    workflow_runtime_candidates(app).into_iter().find(|root| {
        root.join("node")
            .join("workflows")
            .join("run_step.cjs")
            .is_file()
            && root
                .join("resources")
                .join("node")
                .join("register")
                .join("lib")
                .join("browser-launcher.cjs")
                .is_file()
    })
}

fn ensure_workflow_dependencies(
    app: &tauri::AppHandle,
    runtime_root: &Path,
) -> Result<PathBuf, String> {
    let bundled_modules = runtime_root.join("node_modules");

    if workflow_modules_ready(&bundled_modules) {
        return Ok(bundled_modules);
    }

    let dependencies_root = ensure_runtime_dir(app)?.join("workflow-dependencies");
    let staged_modules = dependencies_root.join("node_modules");
    let archive_path = runtime_root.join("node_modules.tar.gz");

    if !archive_path.is_file() {
        return Err("workflow dependency archive was not found".to_string());
    }

    let archive_bytes = fs::read(&archive_path)
        .map_err(|e| format!("read workflow dependency archive failed: {e}"))?;
    let archive_fingerprint = hex::encode(Sha256::digest(&archive_bytes));
    let fingerprint_path = dependencies_root.join(".archive-sha256");
    let staged_fingerprint = fs::read_to_string(&fingerprint_path).unwrap_or_default();

    if workflow_modules_ready(&staged_modules) && staged_fingerprint.trim() == archive_fingerprint {
        return Ok(staged_modules);
    }

    if dependencies_root.exists() {
        fs::remove_dir_all(&dependencies_root)
            .map_err(|e| format!("remove stale workflow dependencies failed: {e}"))?;
    }

    fs::create_dir_all(&dependencies_root)
        .map_err(|e| format!("create workflow dependencies directory failed: {e}"))?;
    let archive_file = fs::File::open(&archive_path)
        .map_err(|e| format!("open workflow dependency archive failed: {e}"))?;
    let decoder = GzDecoder::new(archive_file);
    let mut archive = Archive::new(decoder);
    archive
        .unpack(&dependencies_root)
        .map_err(|e| format!("extract workflow dependencies failed: {e}"))?;

    if !workflow_modules_ready(&staged_modules) {
        return Err(
            "workflow dependency archive is incomplete or missing required portable modules"
                .to_string(),
        );
    }

    fs::write(&fingerprint_path, archive_fingerprint)
        .map_err(|e| format!("write workflow dependency fingerprint failed: {e}"))?;

    Ok(staged_modules)
}

fn workflow_node_path(
    app: &tauri::AppHandle,
    runtime_root: &Path,
) -> Result<std::ffi::OsString, String> {
    std::env::join_paths([ensure_workflow_dependencies(app, runtime_root)?])
        .map_err(|e| format!("build portable NODE_PATH failed: {e}"))
}

fn executable_workflow_runtime(
    app: &tauri::AppHandle,
    resource_runtime_root: &Path,
) -> Result<PathBuf, String> {
    let modules_root = ensure_workflow_dependencies(app, resource_runtime_root)?;

    if modules_root == resource_runtime_root.join("node_modules") {
        return Ok(resource_runtime_root.to_path_buf());
    }

    let staged_runtime_root = modules_root
        .parent()
        .ok_or_else(|| "portable workflow dependency root has no parent directory".to_string())?
        .to_path_buf();

    for directory in ["node", "resources"] {
        copy_dir_recursive(
            &resource_runtime_root.join(directory),
            &staged_runtime_root.join(directory),
        )?;
    }

    fs::copy(
        resource_runtime_root.join("package.json"),
        staged_runtime_root.join("package.json"),
    )
    .map_err(|e| format!("stage portable workflow package manifest failed: {e}"))?;

    Ok(staged_runtime_root)
}

fn verify_job_signature(cfg: &ClientConfig, job: &RemoteJob) -> Result<(), String> {
    if job.signature.trim().is_empty() {
        return Err("job signature missing".to_string());
    }

    let canonical = serde_json::to_string(&job.payload)
        .map_err(|e| format!("serialize job payload for signature failed: {e}"))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(cfg.api_key.as_bytes())
        .map_err(|e| format!("initialize job signature failed: {e}"))?;
    mac.update(canonical.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());

    if !expected.eq_ignore_ascii_case(job.signature.trim()) {
        return Err("job signature invalid".to_string());
    }

    Ok(())
}

fn clear_outbox_internal(app: &tauri::AppHandle) -> Result<usize, String> {
    init_db(app)?;
    let conn = open_db(app)?;

    conn.execute("DELETE FROM outbox_events", [])
        .map_err(|e| format!("clear outbox failed: {e}"))
}

fn pending_outbox_internal(app: &tauri::AppHandle, limit: i64) -> Result<Vec<OutboxEvent>, String> {
    init_db(app)?;
    let conn = open_db(app)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, event_type, payload_json, created_at
             FROM outbox_events
             WHERE status = 'pending'
             ORDER BY id ASC
             LIMIT ?1",
        )
        .map_err(|e| format!("prepare outbox query failed: {e}"))?;
    let rows = stmt
        .query_map(params![limit.max(1)], |row| {
            Ok(OutboxEvent {
                id: row.get(0)?,
                event_type: row.get(1)?,
                payload_json: row.get(2)?,
                created_at: row.get(3)?,
            })
        })
        .map_err(|e| format!("query pending outbox failed: {e}"))?;
    let mut events = Vec::new();

    for row in rows {
        events.push(row.map_err(|e| format!("read outbox row failed: {e}"))?);
    }

    Ok(events)
}

fn report_job_result_remote(
    app: &tauri::AppHandle,
    job_uuid: &str,
    status: &str,
    result: Value,
    error_message: Option<String>,
    lease_token: &str,
    sequence: i64,
) -> Result<DeliveryAck, String> {
    let cfg = load_or_create_config(app)?;
    let endpoint = format!(
        "{}/api/client-controller/job-result",
        base_url(&cfg.server_domain)
    );
    let result = if result.is_object() {
        result
    } else {
        json!({ "value": result })
    };
    let body = json!({
        "api_key": cfg.api_key,
        "job_uuid": job_uuid,
        "status": status,
        "result": result,
        "error_message": error_message,
        "lease_token": lease_token,
        "sequence": sequence,
    });
    let response = http_client()?
        .post(endpoint)
        .header("X-NODE-API-KEY", cfg.api_key)
        .json(&body)
        .send()
        .map_err(|e| format!("job result request failed: {e}"))?;
    let status_code = response.status();
    let raw_body = response
        .text()
        .map_err(|e| format!("job result response read failed: {e}"))?;
    let response_body: Value = serde_json::from_str(&raw_body).map_err(|e| {
        format!(
            "job result response parse failed: {} (HTTP {}, body: {})",
            e,
            status_code.as_u16(),
            preview_body(&raw_body, 500)
        )
    })?;

    if !status_code.is_success()
        || !response_body
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Err(format!(
            "job result failed: HTTP {} - {}",
            status_code.as_u16(),
            response_body
        ));
    }

    delivery_ack_from_response(&response_body, sequence)
}

fn report_job_progress_remote(
    app: &tauri::AppHandle,
    job_uuid: &str,
    progress: &Value,
    screenshot_path: &Path,
    include_screenshot: bool,
    lease_token: &str,
    sequence: i64,
) -> Result<DeliveryAck, String> {
    let cfg = load_or_create_config(app)?;
    let endpoint = format!(
        "{}/api/client-controller/job-progress",
        base_url(&cfg.server_domain)
    );
    let mut form = reqwest::blocking::multipart::Form::new()
        .text("job_uuid", job_uuid.to_string())
        .text("progress", progress.to_string())
        .text("lease_token", lease_token.to_string())
        .text("sequence", sequence.to_string());

    if include_screenshot && screenshot_path.is_file() {
        let screenshot = fs::read(screenshot_path)
            .map_err(|e| format!("read workflow screenshot failed: {e}"))?;
        let part = reqwest::blocking::multipart::Part::bytes(screenshot)
            .file_name("live.png")
            .mime_str("image/png")
            .map_err(|e| format!("build workflow screenshot upload failed: {e}"))?;
        form = form.part("screenshot", part);
    }

    let response = http_client()?
        .post(endpoint)
        .header("X-NODE-API-KEY", cfg.api_key)
        .multipart(form)
        .send()
        .map_err(|e| format!("job progress request failed: {e}"))?;
    let status_code = response.status();
    let raw_body = response
        .text()
        .map_err(|e| format!("job progress response read failed: {e}"))?;
    let response_body: Value = serde_json::from_str(&raw_body).map_err(|e| {
        format!(
            "job progress response parse failed: {} (HTTP {}, body: {})",
            e,
            status_code.as_u16(),
            preview_body(&raw_body, 500)
        )
    })?;

    if !status_code.is_success()
        || !response_body
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Err(format!(
            "job progress failed: HTTP {} - {}",
            status_code.as_u16(),
            response_body
        ));
    }

    delivery_ack_from_response(&response_body, sequence)
}

fn delivery_ack_from_response(body: &Value, fallback_sequence: i64) -> Result<DeliveryAck, String> {
    let control = body
        .get("control")
        .filter(|value| value.is_object())
        .cloned()
        .map(serde_json::from_value::<RemoteControl>)
        .transpose()
        .map_err(|e| format!("parse job control response failed: {e}"))?;

    Ok(DeliveryAck {
        acknowledged_sequence: body
            .get("acknowledged_sequence")
            .and_then(Value::as_i64)
            .unwrap_or(fallback_sequence),
        control,
    })
}

fn initialize_job_delivery(app: &tauri::AppHandle, job: &RemoteJob) -> Result<(), String> {
    init_db(app)?;
    let conn = open_db(app)?;
    conn.execute(
        "INSERT INTO job_delivery_state
         (job_uuid, lease_token, last_sequence, control_command, control_sequence, control_payload_json, updated_at)
         VALUES (?1, ?2, 0, ?3, ?4, ?5, ?6)
         ON CONFLICT(job_uuid) DO UPDATE SET
            lease_token = excluded.lease_token,
            control_command = COALESCE(excluded.control_command, job_delivery_state.control_command),
            control_sequence = MAX(excluded.control_sequence, job_delivery_state.control_sequence),
            control_payload_json = COALESCE(excluded.control_payload_json, job_delivery_state.control_payload_json),
            updated_at = excluded.updated_at",
        params![
            job.job_uuid,
            job.lease_token,
            job.control.as_ref().map(|control| control.command.as_str()),
            job.control.as_ref().map(|control| control.sequence).unwrap_or(0),
            job.control.as_ref().map(|control| control.payload.to_string()),
            now_iso()
        ],
    )
    .map_err(|e| format!("initialize job delivery state failed: {e}"))?;
    Ok(())
}

fn store_delivery_ack(conn: &Connection, job_uuid: &str, ack: &DeliveryAck) -> Result<(), String> {
    if let Some(control) = ack.control.as_ref() {
        conn.execute(
            "UPDATE job_delivery_state
             SET control_command = ?1, control_sequence = ?2, control_payload_json = ?3, updated_at = ?4
             WHERE job_uuid = ?5 AND control_sequence <= ?2",
            params![
                control.command,
                control.sequence,
                control.payload.to_string(),
                now_iso(),
                job_uuid
            ],
        )
        .map_err(|e| format!("store job control failed: {e}"))?;
    }

    let _ = ack.acknowledged_sequence;
    Ok(())
}

fn pending_job_control(
    app: &tauri::AppHandle,
    job_uuid: &str,
) -> Result<Option<RemoteControl>, String> {
    init_db(app)?;
    let conn = open_db(app)?;
    conn.query_row(
        "SELECT control_command, control_sequence, COALESCE(control_payload_json, '{}')
         FROM job_delivery_state
         WHERE job_uuid = ?1 AND control_command IS NOT NULL",
        params![job_uuid],
        |row| {
            let command: String = row.get(0)?;
            let sequence: i64 = row.get(1)?;
            let payload_raw: String = row.get(2)?;
            Ok(RemoteControl {
                command,
                sequence,
                payload: serde_json::from_str(&payload_raw).unwrap_or_else(|_| json!({})),
            })
        },
    )
    .optional()
    .map_err(|e| format!("read pending job control failed: {e}"))
}

fn next_job_sequence(app: &tauri::AppHandle, job_uuid: &str) -> Result<i64, String> {
    init_db(app)?;
    let mut conn = open_db(app)?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("start delivery sequence transaction failed: {e}"))?;
    let current = tx
        .query_row(
            "SELECT last_sequence FROM job_delivery_state WHERE job_uuid = ?1",
            params![job_uuid],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let next = current + 1;
    tx.execute(
        "INSERT INTO job_delivery_state (job_uuid, lease_token, last_sequence, updated_at)
         VALUES (?1, '', ?2, ?3)
         ON CONFLICT(job_uuid) DO UPDATE SET last_sequence = excluded.last_sequence, updated_at = excluded.updated_at",
        params![job_uuid, next, now_iso()],
    )
    .map_err(|e| format!("update delivery sequence failed: {e}"))?;
    tx.commit()
        .map_err(|e| format!("commit delivery sequence failed: {e}"))?;
    Ok(next)
}

fn queue_job_progress_delivery(
    app: &tauri::AppHandle,
    job: &RemoteJob,
    status_path: &Path,
    screenshot_path: &Path,
    include_screenshot: bool,
) -> Result<bool, String> {
    let raw_status = match fs::read_to_string(status_path) {
        Ok(raw_status) => raw_status,
        Err(_) => return Ok(false),
    };
    let progress: Value = match serde_json::from_str::<Value>(&raw_status) {
        Ok(progress) if progress.is_object() => progress,
        _ => return Ok(false),
    };
    let sequence = next_job_sequence(app, &job.job_uuid)?;
    let mut screenshot_to_send = None;
    let conn = open_db(app)?;

    if include_screenshot && screenshot_path.is_file() {
        let bytes = fs::read(screenshot_path)
            .map_err(|e| format!("read workflow screenshot for queue failed: {e}"))?;
        let hash = hex::encode(Sha256::digest(&bytes));
        let previous_hash = conn
            .query_row(
                "SELECT last_screenshot_hash FROM job_delivery_state WHERE job_uuid = ?1",
                params![job.job_uuid],
                |row| row.get::<_, Option<String>>(0),
            )
            .ok()
            .flatten();

        if previous_hash.as_deref() != Some(hash.as_str()) {
            screenshot_to_send = Some(screenshot_path.to_string_lossy().to_string());
            conn.execute(
                "UPDATE job_delivery_state SET last_screenshot_hash = ?1, updated_at = ?2 WHERE job_uuid = ?3",
                params![hash, now_iso(), job.job_uuid],
            )
            .map_err(|e| format!("update screenshot delivery hash failed: {e}"))?;
        }
    }

    conn.execute(
        "DELETE FROM outbox_events WHERE status = 'pending' AND event_type = 'job_progress' AND job_uuid = ?1",
        params![job.job_uuid],
    )
    .map_err(|e| format!("coalesce job progress failed: {e}"))?;
    conn.execute(
        "INSERT INTO outbox_events
         (event_type, payload_json, status, retry_count, created_at, job_uuid, sequence, next_attempt_at, screenshot_path)
         VALUES ('job_progress', ?1, 'pending', 0, ?2, ?3, ?4, ?2, ?5)",
        params![
            progress.to_string(),
            now_iso(),
            job.job_uuid,
            sequence,
            screenshot_to_send
        ],
    )
    .map_err(|e| format!("queue job progress delivery failed: {e}"))?;
    Ok(true)
}

fn queue_job_result_delivery(
    app: &tauri::AppHandle,
    job: &RemoteJob,
    status: &str,
    result: Value,
    error_message: Option<String>,
) -> Result<(), String> {
    let sequence = next_job_sequence(app, &job.job_uuid)?;
    let payload = json!({
        "status": status,
        "result": if result.is_object() { result } else { json!({"value": result}) },
        "error_message": error_message,
    });
    let conn = open_db(app)?;
    conn.execute(
        "INSERT INTO outbox_events
         (event_type, payload_json, status, retry_count, created_at, job_uuid, sequence, next_attempt_at)
         VALUES ('job_result', ?1, 'pending', 0, ?2, ?3, ?4, ?2)",
        params![payload.to_string(), now_iso(), job.job_uuid, sequence],
    )
    .map_err(|e| format!("queue job result delivery failed: {e}"))?;
    Ok(())
}

fn flush_job_delivery_outbox(app: &tauri::AppHandle, limit: i64) -> Result<usize, String> {
    init_db(app)?;
    let conn = open_db(app)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, event_type, payload_json, job_uuid, sequence, screenshot_path, retry_count
             FROM outbox_events
             WHERE status = 'pending'
               AND event_type IN ('job_progress', 'job_result')
               AND (next_attempt_at IS NULL OR next_attempt_at <= ?1)
             ORDER BY id ASC LIMIT ?2",
        )
        .map_err(|e| format!("prepare job delivery outbox failed: {e}"))?;
    let rows = stmt
        .query_map(params![now_iso(), limit.max(1)], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                row.get::<_, Option<i64>>(4)?.unwrap_or(0),
                row.get::<_, Option<String>>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .map_err(|e| format!("query job delivery outbox failed: {e}"))?;
    let events = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read job delivery outbox failed: {e}"))?;
    drop(stmt);
    let mut sent = 0usize;

    for (id, event_type, payload_json, job_uuid, sequence, screenshot_path, retry_count) in events {
        let payload: Value = serde_json::from_str(&payload_json)
            .map_err(|e| format!("parse queued {event_type} failed: {e}"))?;
        let lease_token = conn
            .query_row(
                "SELECT lease_token FROM job_delivery_state WHERE job_uuid = ?1",
                params![job_uuid],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or_default();
        let delivery = if event_type == "job_progress" {
            report_job_progress_remote(
                app,
                &job_uuid,
                &payload,
                Path::new(screenshot_path.as_deref().unwrap_or("")),
                screenshot_path.is_some(),
                &lease_token,
                sequence,
            )
        } else {
            report_job_result_remote(
                app,
                &job_uuid,
                payload
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("failed"),
                payload.get("result").cloned().unwrap_or_else(|| json!({})),
                payload
                    .get("error_message")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                &lease_token,
                sequence,
            )
        };

        match delivery {
            Ok(ack) => {
                store_delivery_ack(&conn, &job_uuid, &ack)?;
                conn.execute(
                    "UPDATE outbox_events SET status = 'sent', sent_at = ?1, last_error = NULL WHERE id = ?2",
                    params![now_iso(), id],
                )
                .map_err(|e| format!("acknowledge job delivery failed: {e}"))?;
                sent += 1;
            }
            Err(error) => {
                let next_retry_count = retry_count + 1;
                let exponent = (next_retry_count.min(8)) as u32;
                let delay_seconds = (1_i64 << exponent).min(300);
                let next_attempt =
                    (Utc::now() + ChronoDuration::seconds(delay_seconds)).to_rfc3339();
                conn.execute(
                    "UPDATE outbox_events
                     SET retry_count = ?1, next_attempt_at = ?2, last_error = ?3
                     WHERE id = ?4",
                    params![next_retry_count, next_attempt, error, id],
                )
                .map_err(|e| format!("defer failed job delivery failed: {e}"))?;
            }
        }
    }

    Ok(sent)
}

fn start_local_job_execution(app: &tauri::AppHandle, job: &RemoteJob) -> Result<i64, String> {
    init_db(app)?;
    let conn = open_db(app)?;
    conn.execute(
        "INSERT INTO job_executions_local (job_id, job_type, status, details_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![job.job_uuid, job.job_type, "running", json!({
            "startedAt": now_iso(),
            "payloadVersion": job.payload_version,
            "leaseExpiresAt": job.lease_expires_at,
        }).to_string(), now_iso()],
    )
    .map_err(|e| format!("store local job execution failed: {e}"))?;

    Ok(conn.last_insert_rowid())
}

fn finish_local_job_execution(
    app: &tauri::AppHandle,
    execution_id: i64,
    status: &str,
    details: &Value,
) -> Result<(), String> {
    init_db(app)?;
    let conn = open_db(app)?;
    conn.execute(
        "UPDATE job_executions_local SET status = ?1, details_json = ?2 WHERE id = ?3",
        params![status, details.to_string(), execution_id],
    )
    .map_err(|e| format!("update local job execution failed: {e}"))?;

    Ok(())
}

fn local_processes_internal(
    app: &tauri::AppHandle,
    limit: i64,
) -> Result<Vec<LocalProcess>, String> {
    init_db(app)?;
    let conn = open_db(app)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, job_id, job_type, status, COALESCE(details_json, '{}'), created_at
             FROM job_executions_local
             ORDER BY id DESC
             LIMIT ?1",
        )
        .map_err(|e| format!("prepare local process query failed: {e}"))?;
    let rows = stmt
        .query_map(params![limit.max(1)], |row| {
            Ok(LocalProcess {
                id: row.get(0)?,
                job_id: row.get(1)?,
                job_type: row.get(2)?,
                status: row.get(3)?,
                details_json: row.get(4)?,
                created_at: row.get(5)?,
            })
        })
        .map_err(|e| format!("query local processes failed: {e}"))?;
    let mut processes = Vec::new();

    for row in rows {
        processes.push(row.map_err(|e| format!("read local process row failed: {e}"))?);
    }

    Ok(processes)
}

fn validated_job_uuid(job_uuid: &str) -> Result<&str, String> {
    let job_uuid = job_uuid.trim();
    if job_uuid.is_empty()
        || !job_uuid
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Err("invalid workflow job UUID".to_string());
    }
    Ok(job_uuid)
}

fn workflow_job_directory(app: &tauri::AppHandle, job_uuid: &str) -> Result<PathBuf, String> {
    Ok(ensure_runtime_dir(app)?
        .join("workflow-jobs")
        .join(validated_job_uuid(job_uuid)?))
}

fn read_json_file(path: &Path) -> Value {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .unwrap_or_else(|| json!({}))
}

fn read_file_tail(path: &Path, maximum_bytes: usize) -> String {
    let Ok(bytes) = fs::read(path) else {
        return String::new();
    };
    let start = bytes.len().saturating_sub(maximum_bytes);
    String::from_utf8_lossy(&bytes[start..]).to_string()
}

fn workflow_process_preview_internal(
    app: &tauri::AppHandle,
    job_uuid: &str,
) -> Result<WorkflowProcessPreview, String> {
    let job_uuid = validated_job_uuid(job_uuid)?.to_string();
    let run_directory = workflow_job_directory(app, &job_uuid)?;
    if !run_directory.is_dir() {
        return Err(format!("workflow run directory not found: {job_uuid}"));
    }

    let mut result = read_json_file(&run_directory.join("result.json"));
    if let Some(process) = local_processes_internal(app, 250)?
        .into_iter()
        .find(|process| process.job_id.as_deref() == Some(job_uuid.as_str()))
    {
        if process.status != "running"
            || result
                .as_object()
                .map(|object| object.is_empty())
                .unwrap_or(true)
        {
            result = serde_json::from_str(&process.details_json).unwrap_or_else(|_| json!({}));
        }
    }
    let screenshot_path = run_directory.join("live.png");
    let screenshot_data_url = fs::read(&screenshot_path)
        .ok()
        .map(|bytes| format!("data:image/png;base64,{}", BASE64.encode(bytes)));

    Ok(WorkflowProcessPreview {
        job_uuid,
        status: read_json_file(&run_directory.join("status.json")),
        result,
        checkpoint: read_json_file(&run_directory.join("workflow-checkpoint.json")),
        workflow_steps: read_json_file(&run_directory.join("workflow-bundle.json"))
            .get("steps")
            .cloned()
            .filter(Value::is_array)
            .unwrap_or_else(|| json!([])),
        screenshot_data_url,
        stdout_tail: read_file_tail(&run_directory.join("stdout.log"), 32_000),
        stderr_tail: read_file_tail(&run_directory.join("stderr.log"), 32_000),
        run_directory: run_directory.to_string_lossy().to_string(),
    })
}

fn append_debug_json<W: Write>(
    archive: &mut Builder<W>,
    path: &str,
    payload: &Value,
) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(payload)
        .map_err(|error| format!("serialize debug payload failed: {error}"))?;
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o600);
    header.set_mtime(Utc::now().timestamp().max(0) as u64);
    header.set_cksum();
    archive
        .append_data(&mut header, path, bytes.as_slice())
        .map_err(|error| format!("append debug payload failed: {error}"))
}

fn redact_debug_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| {
                    let normalized = key.to_ascii_lowercase().replace(['_', '-'], "");
                    let sensitive = [
                        "password",
                        "secret",
                        "apikey",
                        "leasetoken",
                        "sessionpayload",
                        "encrypted",
                        "cookies",
                        "localstorage",
                        "sessionstorage",
                        "wsendpoint",
                    ]
                    .iter()
                    .any(|candidate| normalized.contains(candidate));
                    (
                        key.clone(),
                        if sensitive {
                            Value::String("[redacted]".to_string())
                        } else {
                            redact_debug_value(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact_debug_value).collect()),
        _ => value.clone(),
    }
}

fn export_workflow_process_debug_internal(
    app: &tauri::AppHandle,
    job_uuid: &str,
) -> Result<String, String> {
    let preview = workflow_process_preview_internal(app, job_uuid)?;
    let run_directory = workflow_job_directory(app, &preview.job_uuid)?;
    let export_directory = app
        .path()
        .download_dir()
        .unwrap_or(ensure_runtime_dir(app)?.join("debug-exports"));
    fs::create_dir_all(&export_directory)
        .map_err(|error| format!("create debug export directory failed: {error}"))?;
    let export_path = export_directory.join(format!(
        "workflow-debug-client-{}-{}.tar.gz",
        preview.job_uuid,
        Utc::now().format("%Y%m%d-%H%M%S")
    ));
    let file = fs::File::create(&export_path)
        .map_err(|error| format!("create workflow debug export failed: {error}"))?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut archive = Builder::new(encoder);

    for name in ["stdout.log", "stderr.log", "live.png", "live-dom.json"] {
        let source = run_directory.join(name);
        if source.is_file() {
            archive
                .append_path_with_name(&source, format!("workflow-debug/files/{name}"))
                .map_err(|error| format!("append {name} to debug export failed: {error}"))?;
        }
    }
    for name in [
        "status.json",
        "result.json",
        "workflow-checkpoint.json",
        "workflow-bundle.json",
        "runtime.json",
        "runner-diagnostics.json",
    ] {
        let source = run_directory.join(name);
        if source.is_file() {
            append_debug_json(
                &mut archive,
                &format!("workflow-debug/files/{name}"),
                &redact_debug_value(&read_json_file(&source)),
            )?;
        }
    }

    let processes = local_processes_internal(app, 250)?
        .into_iter()
        .filter(|process| process.job_id.as_deref() == Some(preview.job_uuid.as_str()))
        .collect::<Vec<_>>();
    append_debug_json(
        &mut archive,
        "workflow-debug/manifest.json",
        &json!({
            "exportedAt": now_iso(),
            "jobUuid": preview.job_uuid,
            "runDirectory": preview.run_directory,
            "status": redact_debug_value(&preview.status),
            "result": redact_debug_value(&preview.result),
            "checkpoint": redact_debug_value(&preview.checkpoint),
            "workflowSteps": redact_debug_value(&preview.workflow_steps),
            "localProcesses": redact_debug_value(&serde_json::to_value(processes).unwrap_or_else(|_| json!([]))),
        }),
    )?;
    if let Some(runtime_root) = resolve_workflow_runtime(app) {
        let runtime_manifest = runtime_root.join("workflow-runtime-manifest.json");
        if runtime_manifest.is_file() {
            append_debug_json(
                &mut archive,
                "workflow-debug/workflow-runtime-manifest.json",
                &redact_debug_value(&read_json_file(&runtime_manifest)),
            )?;
        }
    }
    archive
        .finish()
        .map_err(|error| format!("finish workflow debug archive failed: {error}"))?;
    let encoder = archive
        .into_inner()
        .map_err(|error| format!("close workflow debug archive failed: {error}"))?;
    encoder
        .finish()
        .map_err(|error| format!("close workflow debug compression failed: {error}"))?;

    Ok(export_path.to_string_lossy().to_string())
}

fn interrupted_workflow_job_ids(app: &tauri::AppHandle) -> Result<Vec<String>, String> {
    init_db(app)?;
    let conn = open_db(app)?;
    let mut stmt = conn
        .prepare(
            "SELECT current.job_id
             FROM job_executions_local current
             WHERE current.id = (
                 SELECT MAX(latest.id) FROM job_executions_local latest
                 WHERE latest.job_id = current.job_id
             )
               AND current.job_type IN ('workflow_task', 'workflow_run')
               AND current.status = 'interrupted'
               AND current.job_id IS NOT NULL
             ORDER BY current.id ASC LIMIT 10",
        )
        .map_err(|e| format!("prepare interrupted workflow query failed: {e}"))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| format!("query interrupted workflows failed: {e}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read interrupted workflows failed: {e}"))
}

fn acknowledge_pending_job_stop(
    app: &tauri::AppHandle,
    job: &RemoteJob,
    status_path: &Path,
    screenshot_path: &Path,
) -> Result<Option<Value>, String> {
    let Some(control) = pending_job_control(app, &job.job_uuid)? else {
        return Ok(None);
    };

    if control.command != "stop" {
        return Ok(None);
    }

    let result_status = control
        .payload
        .get("result_status")
        .and_then(Value::as_str)
        .filter(|status| matches!(*status, "cancelled" | "timed_out"))
        .unwrap_or("cancelled");
    let reason = control
        .payload
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("Workflow wurde von der AI User Factory gestoppt.");
    let stopping = json!({
        "state": "stopping",
        "status": result_status,
        "isRunning": true,
        "message": reason,
        "statusMessage": reason,
        "controlAcknowledged": "stop",
        "controlSequence": control.sequence,
        "at": now_iso(),
    });
    fs::write(
        status_path,
        serde_json::to_vec_pretty(&stopping)
            .map_err(|e| format!("serialize stop acknowledgement failed: {e}"))?,
    )
    .map_err(|e| format!("write stop acknowledgement failed: {e}"))?;
    let _ = queue_job_progress_delivery(app, job, status_path, screenshot_path, false);
    let _ = flush_job_delivery_outbox(app, 10);

    Ok(Some(json!({
        "ok": false,
        "status": result_status,
        "state": result_status,
        "statusMessage": reason,
        "controlAcknowledged": "stop",
        "controlSequence": control.sequence,
        "finishedAt": now_iso(),
    })))
}

fn terminate_child_tree(child: &mut Child) {
    #[cfg(target_os = "windows")]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &child.id().to_string(), "/T", "/F"])
            .creation_flags(0x08000000)
            .status();
    }

    let _ = child.kill();
    let _ = child.wait();
}

fn terminate_child_process_only(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn workflow_task_hard_timeout_seconds(runtime: &Value) -> u64 {
    let tasks_timeout_sum = runtime
        .get("tasks")
        .and_then(Value::as_array)
        .map(|tasks| {
            tasks
                .iter()
                .map(|task| {
                    task.get("timeout_seconds")
                        .and_then(Value::as_u64)
                        .unwrap_or(60)
                })
                .sum::<u64>()
        })
        .unwrap_or(60);
    let runtime_timeout = runtime
        .get("timeout_seconds")
        .or_else(|| runtime.get("timeoutSeconds"))
        .and_then(Value::as_u64)
        .unwrap_or(0);

    tasks_timeout_sum
        .max(runtime_timeout)
        .saturating_add(90)
        .clamp(120, 3600)
}

fn workflow_task_release_on_result(runtime: &Value) -> bool {
    matches!(
        runtime.get("clientControllerReleaseOnResult"),
        Some(Value::Bool(true))
    ) || matches!(
        runtime
            .get("client_controller_release_on_result")
            .and_then(Value::as_bool),
        Some(true)
    )
}

fn chromium_no_sandbox_enabled(runtime: &Value) -> bool {
    [
        "chromiumNoSandbox",
        "chromium_no_sandbox",
        "disableChromiumSandbox",
        "disable_chromium_sandbox",
    ]
    .iter()
    .any(|key| {
        runtime.get(*key).is_some_and(|value| {
            value.as_bool().unwrap_or(false)
                || value.as_u64() == Some(1)
                || matches!(value.as_str(), Some("true") | Some("1"))
        })
    })
}

fn workflow_task_finished_result(result_path: &Path) -> Option<Value> {
    let result = fs::read_to_string(result_path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())?;

    if !result.is_object() {
        return None;
    }

    let status = result.get("status").and_then(Value::as_str).unwrap_or("");

    if result.get("finishedAt").is_some()
        || matches!(
            status,
            "success" | "failed" | "timed_out" | "timeout" | "cancelled" | "canceled"
        )
    {
        return Some(result);
    }

    None
}

fn write_workflow_runner_diagnostics(run_dir: &Path, payload: Value) {
    let _ = fs::write(
        run_dir.join("runner-diagnostics.json"),
        serde_json::to_vec_pretty(&payload).unwrap_or_else(|_| b"{}".to_vec()),
    );
}

fn enrich_workflow_task_result(mut result: Value) -> Value {
    for (path_key, payload_key) in [
        ("webmailSessionFilePath", "remoteWebmailSessionPayload"),
        ("browserSessionFilePath", "remoteBrowserSessionPayload"),
    ] {
        let session_path = result
            .get(path_key)
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                result
                    .get("tasks")
                    .and_then(Value::as_array)
                    .and_then(|tasks| {
                        tasks.iter().rev().find_map(|task| {
                            task.get(path_key)
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        })
                    })
            });

        if let Some(path) = session_path {
            if let Ok(payload) = fs::read_to_string(path) {
                if let Some(object) = result.as_object_mut() {
                    object.insert(payload_key.to_string(), Value::String(payload));
                }
            }
        }
    }

    result
}

fn execute_workflow_task_job(app: &tauri::AppHandle, job: &RemoteJob) -> Result<Value, String> {
    let runtime_root = resolve_workflow_runtime(app)
        .ok_or_else(|| "workflow runtime not found in application resources".to_string())?;
    let node_binary = bundled_workflow_node_binary(&runtime_root).ok_or_else(|| {
        "bundled Node.js runtime is missing from ClientController resources".to_string()
    })?;
    let browser_binary = bundled_cloakbrowser_binary(&runtime_root).ok_or_else(|| {
        "bundled CloakBrowser binary is missing from ClientController resources".to_string()
    })?;
    let execution_runtime_root = executable_workflow_runtime(app, &runtime_root)?;
    let mut runtime = job
        .payload
        .get("runtime")
        .cloned()
        .filter(Value::is_object)
        .ok_or_else(|| "workflow_task payload.runtime is missing".to_string())?;
    let run_dir = ensure_runtime_dir(app)?
        .join("workflow-jobs")
        .join(&job.job_uuid);
    fs::create_dir_all(&run_dir)
        .map_err(|e| format!("create workflow job directory failed: {e}"))?;

    let status_path = run_dir.join("status.json");
    let result_path = run_dir.join("result.json");
    let _ = fs::remove_file(&result_path);
    let config_path = run_dir.join("runtime.json");
    let preview_path = run_dir.join("live.png");
    let browser_profile_path = run_dir.join("browser-profile");
    let runtime_object = runtime
        .as_object_mut()
        .ok_or_else(|| "workflow runtime is not an object".to_string())?;
    runtime_object.insert("statusPath".into(), json!(status_path));
    runtime_object.insert("resultPath".into(), json!(result_path));
    runtime_object.insert("runDirectory".into(), json!(run_dir));
    runtime_object.insert("livePreviewPath".into(), json!(preview_path));
    runtime_object.insert("browserProfilePath".into(), json!(browser_profile_path));
    runtime_object.insert("clientControllerJobUuid".into(), json!(job.job_uuid));
    runtime_object.insert("clientControllerDeviceUuid".into(), json!(job.device_uuid));
    runtime_object.insert(
        "clientControllerExecutionScope".into(),
        json!(job.execution_scope),
    );

    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&runtime)
            .map_err(|e| format!("serialize workflow runtime failed: {e}"))?,
    )
    .map_err(|e| format!("write workflow runtime failed: {e}"))?;

    let live_preview_enabled = runtime
        .get("livePreviewEnabled")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let progress_interval_seconds = runtime
        .get("livePreviewPollIntervalSeconds")
        .or_else(|| runtime.get("livePreviewIntervalSeconds"))
        .and_then(Value::as_u64)
        .unwrap_or(3)
        .clamp(1, 60);
    let release_on_finished_result = workflow_task_release_on_result(&runtime);
    let hard_timeout_seconds = workflow_task_hard_timeout_seconds(&runtime);
    let runtime_manifest = read_json_file(&runtime_root.join("workflow-runtime-manifest.json"));
    let initial_status = json!({
        "runId": runtime.get("runId").cloned().unwrap_or_else(|| json!(job.job_uuid)),
        "workflow": runtime.get("workflow").cloned().unwrap_or_else(|| json!({})),
        "state": "running",
        "stage": "client-controller-process-started",
        "message": "Workflow-Task-Prozess wurde auf dem ClientController gestartet.",
        "isRunning": true,
        "livePreviewEnabled": live_preview_enabled,
        "livePreviewIntervalSeconds": progress_interval_seconds,
        "livePreviewPollIntervalSeconds": progress_interval_seconds,
        "tasks": runtime.get("tasks").cloned().unwrap_or_else(|| json!([])),
        "events": [],
        "browserWindows": [],
        "runnerDiagnostics": {
            "releaseOnFinishedResult": release_on_finished_result,
            "hardTimeoutSeconds": hard_timeout_seconds,
            "chromiumNoSandboxFlag": cfg!(target_os = "linux") && chromium_no_sandbox_enabled(&runtime),
            "chromiumNoSandboxNote": "Die Chromium-Sandbox bleibt standardmaessig aktiv. --no-sandbox wird nur bei expliziter Runtime-Konfiguration gesetzt.",
            "runtimeManifest": runtime_manifest.clone(),
        },
        "at": now_iso(),
    });
    fs::write(
        &status_path,
        serde_json::to_vec_pretty(&initial_status)
            .map_err(|e| format!("serialize initial workflow status failed: {e}"))?,
    )
    .map_err(|e| format!("write initial workflow status failed: {e}"))?;

    if let Some(stopped) = acknowledge_pending_job_stop(app, job, &status_path, &preview_path)? {
        return Ok(stopped);
    }

    let script = execution_runtime_root
        .join("node")
        .join("workflows")
        .join("run_step.cjs");
    let mut command = Command::new(node_binary);
    command
        .arg(script)
        .arg(&config_path)
        .current_dir(&execution_runtime_root);

    command.env("NODE_PATH", workflow_node_path(app, &runtime_root)?);
    command
        .env("CLIENTCONTROLLER_PORTABLE_RUNTIME", "1")
        .env("CLOAKBROWSER_CACHE_DIR", runtime_root.join(".cloakbrowser"))
        .env("CLOAKBROWSER_BINARY_PATH", &browser_binary)
        .env("PUPPETEER_EXECUTABLE_PATH", &browser_binary)
        .env("MAIL_REGISTRATION_BROWSER_EXECUTABLE_PATH", &browser_binary)
        .env("PUPPETEER_CACHE_DIR", runtime_root.join(".puppeteer-cache"));
    if let Some(timezone) = runtime.get("timezone").and_then(Value::as_str) {
        command.env("TZ", timezone).env("APP_TIMEZONE", timezone);
    }

    let stdout_path = run_dir.join("stdout.log");
    let stderr_path = run_dir.join("stderr.log");
    let stdout = fs::File::create(&stdout_path)
        .map_err(|e| format!("create workflow stdout log failed: {e}"))?;
    let stderr = fs::File::create(&stderr_path)
        .map_err(|e| format!("create workflow stderr log failed: {e}"))?;
    let mut child = command
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|e| format!("start workflow task process failed: {e}"))?;
    let child_pid = child.id();
    let child_started_at = Instant::now();
    let mut finished_result_seen_at: Option<Instant> = None;
    let output_status = loop {
        match child
            .try_wait()
            .map_err(|e| format!("poll workflow task process failed: {e}"))?
        {
            Some(status) => break status,
            None => {
                let _ = queue_job_progress_delivery(
                    app,
                    job,
                    &status_path,
                    &preview_path,
                    live_preview_enabled,
                );
                let _ = flush_job_delivery_outbox(app, 10);
                if let Some(stopped) =
                    acknowledge_pending_job_stop(app, job, &status_path, &preview_path)?
                {
                    terminate_child_tree(&mut child);
                    return Ok(stopped);
                }

                if release_on_finished_result {
                    if let Some(mut result) = workflow_task_finished_result(&result_path) {
                        let result_seen_at =
                            finished_result_seen_at.get_or_insert_with(Instant::now);

                        if result_seen_at.elapsed() >= Duration::from_secs(3) {
                            if let Some(object) = result.as_object_mut() {
                                object.insert(
                                    "clientControllerRunnerReleased".to_string(),
                                    json!(true),
                                );
                                object.insert(
                                    "clientControllerRunnerReleaseReason".to_string(),
                                    json!("node-exit-grace-timeout"),
                                );
                                object.insert(
                                    "clientControllerRunnerProcessId".to_string(),
                                    json!(child_pid),
                                );
                                object.insert(
                                    "clientControllerRunnerElapsedSeconds".to_string(),
                                    json!(child_started_at.elapsed().as_secs()),
                                );
                            }
                            write_workflow_runner_diagnostics(
                                &run_dir,
                                json!({
                                    "stage": "node-exit-grace-timeout",
                                    "message": "Der Node-Runner hat sein Ergebnis geschrieben, den Browser aber nicht innerhalb der Karenzzeit sauber freigegeben. Nur der Runner-Prozess wird beendet.",
                                    "jobUuid": job.job_uuid,
                                    "runId": runtime.get("runId").cloned(),
                                    "childProcessId": child_pid,
                                    "elapsedSeconds": child_started_at.elapsed().as_secs(),
                                    "releaseOnFinishedResult": release_on_finished_result,
                                    "hardTimeoutSeconds": hard_timeout_seconds,
                                    "runtimeManifest": runtime_manifest.clone(),
                                    "status": read_json_file(&status_path),
                                    "result": result.clone(),
                                }),
                            );
                            let _ = queue_local_event(
                                app,
                                "workflow_task_runner_release_timeout",
                                json!({
                                    "job_uuid": job.job_uuid,
                                    "pid": child_pid,
                                    "elapsed_seconds": child_started_at.elapsed().as_secs(),
                                }),
                            );
                            let _ = queue_job_progress_delivery(
                                app,
                                job,
                                &status_path,
                                &preview_path,
                                live_preview_enabled,
                            );
                            let _ = flush_job_delivery_outbox(app, 10);
                            terminate_child_process_only(&mut child);

                            return Ok(enrich_workflow_task_result(result));
                        }
                    }
                }

                if child_started_at.elapsed().as_secs() >= hard_timeout_seconds {
                    let timeout_result = json!({
                        "ok": false,
                        "status": "timed_out",
                        "state": "timed_out",
                        "statusMessage": format!("ClientController-Runner hat nach {} Sekunden kein Ergebnis geliefert.", hard_timeout_seconds),
                        "clientControllerRunnerHardTimeout": true,
                        "clientControllerRunnerProcessId": child_pid,
                        "clientControllerRunnerElapsedSeconds": child_started_at.elapsed().as_secs(),
                        "finishedAt": now_iso(),
                    });
                    fs::write(
                        &status_path,
                        serde_json::to_vec_pretty(&json!({
                            "runId": runtime.get("runId").cloned().unwrap_or_else(|| json!(job.job_uuid)),
                            "state": "timed_out",
                            "stage": "client-runner-hard-timeout",
                            "message": timeout_result.get("statusMessage").cloned().unwrap_or_else(|| json!("ClientController-Runner Timeout.")),
                            "statusMessage": timeout_result.get("statusMessage").cloned().unwrap_or_else(|| json!("ClientController-Runner Timeout.")),
                            "isRunning": false,
                            "result": timeout_result.clone(),
                            "at": now_iso(),
                        }))
                        .map_err(|e| format!("serialize runner timeout status failed: {e}"))?,
                    )
                    .map_err(|e| format!("write runner timeout status failed: {e}"))?;
                    write_workflow_runner_diagnostics(
                        &run_dir,
                        json!({
                            "stage": "client-runner-hard-timeout",
                            "message": "Der Node-Runner hat kein result.json innerhalb des lokalen Sicherheits-Timeouts geschrieben.",
                            "jobUuid": job.job_uuid,
                            "runId": runtime.get("runId").cloned(),
                            "childProcessId": child_pid,
                            "elapsedSeconds": child_started_at.elapsed().as_secs(),
                            "hardTimeoutSeconds": hard_timeout_seconds,
                            "releaseOnFinishedResult": release_on_finished_result,
                            "runtimeManifest": runtime_manifest.clone(),
                            "status": read_json_file(&status_path),
                            "stdoutTail": read_file_tail(&stdout_path, 16_000),
                            "stderrTail": read_file_tail(&stderr_path, 16_000),
                        }),
                    );
                    let _ = queue_job_progress_delivery(
                        app,
                        job,
                        &status_path,
                        &preview_path,
                        live_preview_enabled,
                    );
                    let _ = flush_job_delivery_outbox(app, 10);
                    terminate_child_tree(&mut child);

                    return Ok(timeout_result);
                }

                if finished_result_seen_at.is_some() {
                    std::thread::sleep(Duration::from_millis(250));
                } else {
                    std::thread::sleep(Duration::from_secs(progress_interval_seconds));
                }
            }
        }
    };

    if let Err(error) =
        queue_job_progress_delivery(app, job, &status_path, &preview_path, live_preview_enabled)
    {
        let _ = queue_local_event(
            app,
            "job_progress_failed",
            json!({ "job_uuid": job.job_uuid, "error": error }),
        );
    }
    let _ = flush_job_delivery_outbox(app, 10);

    let result = fs::read_to_string(&result_path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());

    if let Some(result) = result {
        return Ok(enrich_workflow_task_result(result));
    }

    let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
    Err(format!(
        "workflow task returned no result (exit {:?}): {}",
        output_status.code(),
        preview_body(&stderr, 2000)
    ))
}

fn merge_workflow_context(context: &mut Value, result: &Value) {
    let Some(target) = context.as_object_mut() else {
        return;
    };

    let closed_browser = result
        .get("closedBrowser")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if closed_browser {
        for key in [
            "browser",
            "browser_runtime",
            "browserWsEndpoint",
            "browser_ws_endpoint",
            "browserWindows",
            "browser_windows",
        ] {
            target.remove(key);
        }
    }

    for key in [
        "account",
        "new_password",
        "generated_password",
        "generated-password",
        "verification_code",
        "verificationCode",
        "workflow_return",
        "workflowReturn",
        "workflow_return_ok",
        "workflow_variables",
        "workflowVariables",
        "browserWindows",
        "browserWsEndpoint",
        "browserIdentity",
        "webmailSessionFilePath",
        "remoteWebmailSessionPayload",
        "browserSessionFilePath",
        "remoteBrowserSessionPayload",
        "browserSessionDeleted",
        "deletedBrowserSession",
    ] {
        if closed_browser && matches!(key, "browserWindows" | "browserWsEndpoint") {
            continue;
        }

        if let Some(value) = result.get(key) {
            if !value.is_null() {
                target.insert(key.to_string(), value.clone());
            }
        }
    }

    if !closed_browser {
        if let Some(windows) = result.get("browserWindows") {
            target.insert("browser_windows".to_string(), windows.clone());
        }
    }
}

fn workflow_browser_window_name(context: &Value) -> Option<String> {
    for key in ["browserWindows", "browser_windows"] {
        let Some(windows) = context.get(key) else {
            continue;
        };

        if let Some(window) = windows.as_array().and_then(|items| items.first()) {
            if let Some(name) = window
                .get("key")
                .or_else(|| window.get("name"))
                .and_then(Value::as_str)
                .filter(|name| !name.trim().is_empty())
            {
                return Some(name.trim().to_string());
            }
        }

        if let Some((name, _)) = windows
            .as_object()
            .and_then(|items| items.iter().next())
            .filter(|(name, _)| !name.trim().is_empty())
        {
            return Some(name.trim().to_string());
        }
    }

    None
}

fn close_workflow_browser_for_run(
    app: &tauri::AppHandle,
    job: &RemoteJob,
    steps: &[Value],
    context: &mut Value,
) -> Option<Value> {
    let browser_window = workflow_browser_window_name(context)?;
    let browser_ws_endpoint = context
        .get("browserWsEndpoint")
        .or_else(|| context.get("browser_ws_endpoint"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();

    if browser_ws_endpoint.is_empty() {
        return None;
    }

    let mut runtime = steps
        .iter()
        .find_map(|step| step.get("runtime").filter(|value| value.is_object()))
        .cloned()?;
    let runtime_object = runtime.as_object_mut()?;
    runtime_object.insert("workflow".to_string(), context.clone());
    runtime_object.insert(
        "runId".to_string(),
        json!(format!("{}-browser-cleanup", job.job_uuid)),
    );
    runtime_object.insert("workflowBundleStep".to_string(), json!(true));
    runtime_object.insert("keepWorkflowBrowserAlive".to_string(), json!(false));
    runtime_object.insert("closeWorkflowBrowserAtEnd".to_string(), json!(true));
    runtime_object.insert(
        "tasks".to_string(),
        json!([{
            "key": "workflow-browser-lifecycle-close",
            "task_key": "browser.close",
            "title": "Workflow-Browser am Workflow-Ende schliessen",
            "kind": "browser",
            "runner": "node",
            "node_script": "node/workflows/tasks/browser/close.cjs",
            "browser_window": browser_window.clone(),
            "browser_window_name": browser_window,
            "timeout_seconds": 30
        }]),
    );

    let mut cleanup_job = job.clone();
    cleanup_job.job_type = "workflow_task".to_string();
    cleanup_job.payload = json!({"runtime": runtime});
    let result = execute_workflow_task_job(app, &cleanup_job)
        .unwrap_or_else(|error| json!({
            "ok": false,
            "status": "failed",
            "statusMessage": format!("Workflow-Browser konnte am Workflow-Ende nicht geschlossen werden: {error}"),
            "browserLifecycleCleanup": true,
        }));
    merge_workflow_context(context, &result);

    Some(result)
}

fn workflow_step_route(step: &Value, result: &Value, outcome: &str) -> Value {
    if result
        .get("routeRequested")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let completed_key = result
            .get("completedTaskKey")
            .or_else(|| result.get("completed_task_key"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if let Some(task) = step
            .get("runtime")
            .and_then(|runtime| runtime.get("tasks"))
            .and_then(Value::as_array)
            .and_then(|tasks| {
                tasks.iter().find(|task| {
                    task.get("key").and_then(Value::as_str).unwrap_or("") == completed_key
                })
            })
        {
            let route = if outcome == "success" {
                task.get("next")
            } else {
                task.get("on_error").or_else(|| {
                    task.get("status_routes")
                        .and_then(|routes| routes.get(outcome))
                })
            };
            if let Some(route) = route.filter(|route| route.is_object()) {
                return route.clone();
            }
        }
    }

    if let Some(routes) = step.get("routes") {
        if let Some(route) = routes
            .get(outcome)
            .or_else(|| routes.get("default"))
            .filter(|route| route.is_object())
        {
            return route.clone();
        }
    }

    if outcome == "success" {
        if let Some(next) = step.get("defaultNext").and_then(Value::as_str) {
            if !next.is_empty() {
                return json!({"type": "step", "action_key": next});
            }
        }
        json!({"type": "end"})
    } else {
        json!({"type": "fail"})
    }
}

fn route_type_and_target(route: &Value, current_action: &str) -> (String, String, String) {
    let mut route_type = route
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let target = route
        .get("action_key")
        .or_else(|| route.get("step"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let card = route
        .get("card_key")
        .or_else(|| route.get("card"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    if route_type.is_empty() {
        route_type = if !card.is_empty() { "card" } else { "step" }.to_string();
    }
    if matches!(target.as_str(), "end" | "fail") {
        route_type = target.clone();
    }
    let target = if route_type == "card" && target.is_empty() {
        current_action.to_string()
    } else {
        target
    };
    (route_type, target, card)
}

fn write_full_workflow_snapshot(
    app: &tauri::AppHandle,
    job: &RemoteJob,
    state: &str,
    message: &str,
    current_step: Option<&Value>,
    step_results: &[Value],
    context: &Value,
) -> Result<(), String> {
    let run_dir = ensure_runtime_dir(app)?
        .join("workflow-jobs")
        .join(&job.job_uuid);
    fs::create_dir_all(&run_dir)
        .map_err(|e| format!("create full workflow directory failed: {e}"))?;
    let status_path = run_dir.join("status.json");
    let preview_path = run_dir.join("live.png");
    let workflow_steps = read_json_file(&run_dir.join("workflow-bundle.json"))
        .get("steps")
        .cloned()
        .filter(Value::is_array)
        .unwrap_or_else(|| json!([]));
    let snapshot = json!({
        "runId": job.job_uuid,
        "state": state,
        "stage": "client-workflow-run",
        "message": message,
        "isRunning": state == "running",
        "currentStepId": current_step.and_then(|step| step.get("workflowStepId")).cloned(),
        "currentStepRunId": current_step.and_then(|step| step.get("workflowStepRunId")).cloned(),
        "steps": step_results,
        "workflowSteps": workflow_steps,
        "workflow": context,
        "browserWindows": context.get("browserWindows").cloned(),
        "browserWsEndpoint": context.get("browserWsEndpoint").cloned(),
        "browserIdentity": context.get("browserIdentity").cloned(),
        "livePreviewEnabled": true,
        "livePreviewIntervalSeconds": 3,
        "livePreviewPollIntervalSeconds": 3,
        "at": now_iso(),
    });
    fs::write(
        &status_path,
        serde_json::to_vec_pretty(&snapshot)
            .map_err(|e| format!("serialize full workflow status failed: {e}"))?,
    )
    .map_err(|e| format!("write full workflow status failed: {e}"))?;
    let _ = queue_job_progress_delivery(app, job, &status_path, &preview_path, true);
    let _ = flush_job_delivery_outbox(app, 10);
    Ok(())
}

fn execute_workflow_run_job(app: &tauri::AppHandle, job: &RemoteJob) -> Result<Value, String> {
    if job.payload_version < 2 {
        return Err("workflow_run requires payload version 2 or newer".to_string());
    }

    let bundle = job
        .payload
        .get("workflow_bundle")
        .cloned()
        .filter(Value::is_object)
        .ok_or_else(|| "workflow_run payload.workflow_bundle is missing".to_string())?;
    let steps = bundle
        .get("steps")
        .and_then(Value::as_array)
        .cloned()
        .filter(|steps| !steps.is_empty())
        .ok_or_else(|| "workflow bundle contains no steps".to_string())?;
    let max_transitions = bundle
        .get("maxTransitions")
        .and_then(Value::as_u64)
        .unwrap_or(100)
        .max(1);
    let run_dir = ensure_runtime_dir(app)?
        .join("workflow-jobs")
        .join(&job.job_uuid);
    fs::create_dir_all(&run_dir)
        .map_err(|e| format!("create workflow run directory failed: {e}"))?;
    fs::write(
        run_dir.join("workflow-bundle.json"),
        serde_json::to_vec_pretty(&bundle)
            .map_err(|e| format!("serialize workflow bundle failed: {e}"))?,
    )
    .map_err(|e| format!("write workflow bundle failed: {e}"))?;
    let checkpoint_path = run_dir.join("workflow-checkpoint.json");
    let workflow_status_path = run_dir.join("status.json");
    let workflow_preview_path = run_dir.join("live.png");
    let checkpoint = fs::read_to_string(&checkpoint_path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
    let mut context = checkpoint
        .as_ref()
        .and_then(|value| value.get("context"))
        .cloned()
        .unwrap_or_else(|| bundle.get("context").cloned().unwrap_or_else(|| json!({})));
    let mut step_results = checkpoint
        .as_ref()
        .and_then(|value| value.get("steps"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut current_action = checkpoint
        .as_ref()
        .and_then(|value| value.get("nextActionKey"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            bundle
                .get("startActionKey")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();
    let mut start_card = checkpoint
        .as_ref()
        .and_then(|value| value.get("nextTaskKey"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    let mut transitions = checkpoint
        .as_ref()
        .and_then(|value| value.get("transitions"))
        .and_then(Value::as_u64)
        .unwrap_or(0);

    while !current_action.is_empty() && transitions < max_transitions {
        transitions += 1;
        let Some(step) = steps.iter().find(|step| {
            step.get("actionKey").and_then(Value::as_str).unwrap_or("") == current_action
        }) else {
            let browser_cleanup = close_workflow_browser_for_run(app, job, &steps, &mut context);
            return Ok(json!({
                "ok": false,
                "status": "failed",
                "statusMessage": format!("Workflow route target was not found: {current_action}"),
                "steps": step_results,
                "workflow": context,
                "browserCleanup": browser_cleanup,
            }));
        };
        let interrupted_step_status = fs::read_to_string(run_dir.join("status.json"))
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .filter(|status| {
                status.get("workflowStepId").and_then(Value::as_i64)
                    == step.get("workflowStepId").and_then(Value::as_i64)
                    && status.get("state").and_then(Value::as_str) != Some("completed")
            });
        let mut resume_after_task_key = String::new();

        if let Some(checkpoint_status) = interrupted_step_status.as_ref() {
            if let Some(tasks) = checkpoint_status.get("tasks").and_then(Value::as_array) {
                for task in tasks.iter().filter(|task| {
                    matches!(
                        task.get("status").and_then(Value::as_str),
                        Some("success") | Some("completed")
                    )
                }) {
                    merge_workflow_context(&mut context, task);
                    if let Some(key) = task.get("key").and_then(Value::as_str) {
                        resume_after_task_key = key.to_string();
                    }
                }
            }
        }

        write_full_workflow_snapshot(
            app,
            job,
            "running",
            step.get("name")
                .and_then(Value::as_str)
                .unwrap_or("Workflow-Schritt wird ausgefuehrt."),
            Some(step),
            &step_results,
            &context,
        )?;
        if let Some(mut stopped) =
            acknowledge_pending_job_stop(app, job, &workflow_status_path, &workflow_preview_path)?
        {
            if let Some(object) = stopped.as_object_mut() {
                object.insert("steps".to_string(), json!(step_results));
                object.insert("workflow".to_string(), context.clone());
            }
            let browser_cleanup = close_workflow_browser_for_run(app, job, &steps, &mut context);
            if let Some(object) = stopped.as_object_mut() {
                object.insert("workflow".to_string(), context.clone());
                object.insert(
                    "browserCleanup".to_string(),
                    browser_cleanup.unwrap_or(Value::Null),
                );
            }
            return Ok(stopped);
        }

        let wait_seconds = step.get("waitSeconds").and_then(Value::as_u64).unwrap_or(0);
        let mut step_result = if wait_seconds > 0
            && step
                .get("runtime")
                .and_then(|runtime| runtime.get("tasks"))
                .and_then(Value::as_array)
                .is_some_and(|tasks| tasks.is_empty())
        {
            let mut remaining = wait_seconds;
            while remaining > 0 {
                let chunk = remaining.min(3);
                std::thread::sleep(Duration::from_secs(chunk));
                remaining -= chunk;
                write_full_workflow_snapshot(
                    app,
                    job,
                    "running",
                    &format!("Warteschritt: {remaining} Sekunden verbleibend."),
                    Some(step),
                    &step_results,
                    &context,
                )?;
                if let Some(mut stopped) = acknowledge_pending_job_stop(
                    app,
                    job,
                    &workflow_status_path,
                    &workflow_preview_path,
                )? {
                    if let Some(object) = stopped.as_object_mut() {
                        object.insert("steps".to_string(), json!(step_results));
                        object.insert("workflow".to_string(), context.clone());
                    }
                    let browser_cleanup =
                        close_workflow_browser_for_run(app, job, &steps, &mut context);
                    if let Some(object) = stopped.as_object_mut() {
                        object.insert("workflow".to_string(), context.clone());
                        object.insert(
                            "browserCleanup".to_string(),
                            browser_cleanup.unwrap_or(Value::Null),
                        );
                    }
                    return Ok(stopped);
                }
            }
            json!({"ok": true, "status": "success", "statusMessage": "Warteschritt abgeschlossen."})
        } else {
            let mut runtime = step.get("runtime").cloned().unwrap_or_else(|| json!({}));
            if let Some(runtime_object) = runtime.as_object_mut() {
                runtime_object.insert("workflow".to_string(), context.clone());
                runtime_object.insert(
                    "runId".to_string(),
                    json!(format!("{}-{}", job.job_uuid, transitions)),
                );
                runtime_object.insert("workflowBundleStep".to_string(), json!(true));
                runtime_object.insert("keepWorkflowBrowserAlive".to_string(), json!(false));

                if !start_card.is_empty() {
                    if let Some(tasks) = runtime_object
                        .get_mut("tasks")
                        .and_then(Value::as_array_mut)
                    {
                        if let Some(index) = tasks.iter().position(|task| {
                            task.get("key").and_then(Value::as_str).unwrap_or("") == start_card
                        }) {
                            *tasks = tasks[index..].to_vec();
                        }
                    }
                } else if !resume_after_task_key.is_empty() {
                    if let Some(tasks) = runtime_object
                        .get_mut("tasks")
                        .and_then(Value::as_array_mut)
                    {
                        if let Some(index) = tasks.iter().position(|task| {
                            task.get("key").and_then(Value::as_str).unwrap_or("")
                                == resume_after_task_key
                        }) {
                            *tasks = tasks.get(index + 1..).unwrap_or(&[]).to_vec();
                        }
                    }
                }
            }
            let mut step_job = job.clone();
            step_job.job_type = "workflow_task".to_string();
            step_job.payload = json!({"runtime": runtime});
            match execute_workflow_task_job(app, &step_job) {
                Ok(result) => result,
                Err(error) => json!({"ok": false, "status": "failed", "statusMessage": error}),
            }
        };

        if let Some(object) = step_result.as_object_mut() {
            object.insert(
                "workflowStepId".to_string(),
                step.get("workflowStepId").cloned().unwrap_or(Value::Null),
            );
            object.insert(
                "workflowStepRunId".to_string(),
                step.get("workflowStepRunId")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            object.insert("actionKey".to_string(), json!(current_action));
            object.insert(
                "state".to_string(),
                json!(
                    if object.get("ok").and_then(Value::as_bool).unwrap_or(false) {
                        "completed"
                    } else {
                        "failed"
                    }
                ),
            );
        }
        if matches!(
            step_result.get("status").and_then(Value::as_str),
            Some("cancelled") | Some("timed_out")
        ) {
            step_results.push(step_result.clone());
            if let Some(object) = step_result.as_object_mut() {
                object.insert("steps".to_string(), json!(step_results));
                object.insert("workflow".to_string(), context.clone());
            }
            let browser_cleanup = close_workflow_browser_for_run(app, job, &steps, &mut context);
            if let Some(object) = step_result.as_object_mut() {
                object.insert("workflow".to_string(), context.clone());
                object.insert(
                    "browserCleanup".to_string(),
                    browser_cleanup.unwrap_or(Value::Null),
                );
            }
            return Ok(step_result);
        }
        merge_workflow_context(&mut context, &step_result);
        step_results.push(step_result.clone());
        let outcome = step_result
            .get("routeOutcome")
            .and_then(Value::as_str)
            .unwrap_or_else(|| {
                if step_result
                    .get("ok")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    "success"
                } else {
                    "failed"
                }
            });
        let route = workflow_step_route(step, &step_result, outcome);
        let (route_type, target, card) = route_type_and_target(&route, &current_action);

        if route_type == "end" {
            current_action.clear();
        } else if route_type == "fail" {
            let browser_cleanup = close_workflow_browser_for_run(app, job, &steps, &mut context);
            let failed = json!({
                "ok": false,
                "status": "failed",
                "statusMessage": step_result.get("statusMessage").cloned().unwrap_or_else(|| json!("Workflow wurde ueber eine Fehlerroute beendet.")),
                "steps": step_results,
                "workflow": context,
                "browserCleanup": browser_cleanup,
                "finishedAt": now_iso(),
            });
            fs::write(
                &checkpoint_path,
                serde_json::to_vec_pretty(&failed)
                    .map_err(|e| format!("serialize failed workflow checkpoint failed: {e}"))?,
            )
            .map_err(|e| format!("write failed workflow checkpoint failed: {e}"))?;
            return Ok(failed);
        } else {
            current_action = if target == "next" {
                step.get("defaultNext")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            } else {
                target
            };
        }
        start_card = card;

        let wait_after = step
            .get("waitAfterSeconds")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if wait_after > 0 && !current_action.is_empty() {
            let mut remaining = wait_after;
            while remaining > 0 {
                let chunk = remaining.min(3);
                std::thread::sleep(Duration::from_secs(chunk));
                remaining -= chunk;
                write_full_workflow_snapshot(
                    app,
                    job,
                    "running",
                    &format!("Wartezeit zwischen Schritten: {remaining} Sekunden verbleibend."),
                    Some(step),
                    &step_results,
                    &context,
                )?;
                if let Some(mut stopped) = acknowledge_pending_job_stop(
                    app,
                    job,
                    &workflow_status_path,
                    &workflow_preview_path,
                )? {
                    if let Some(object) = stopped.as_object_mut() {
                        object.insert("steps".to_string(), json!(step_results));
                        object.insert("workflow".to_string(), context.clone());
                    }
                    let browser_cleanup =
                        close_workflow_browser_for_run(app, job, &steps, &mut context);
                    if let Some(object) = stopped.as_object_mut() {
                        object.insert("workflow".to_string(), context.clone());
                        object.insert(
                            "browserCleanup".to_string(),
                            browser_cleanup.unwrap_or(Value::Null),
                        );
                    }
                    return Ok(stopped);
                }
            }
        }

        let checkpoint = json!({
            "nextActionKey": current_action,
            "nextTaskKey": start_card,
            "transitions": transitions,
            "context": context,
            "steps": step_results,
            "updatedAt": now_iso(),
        });
        fs::write(
            &checkpoint_path,
            serde_json::to_vec_pretty(&checkpoint)
                .map_err(|e| format!("serialize workflow checkpoint failed: {e}"))?,
        )
        .map_err(|e| format!("write workflow checkpoint failed: {e}"))?;
        write_full_workflow_snapshot(
            app,
            job,
            if current_action.is_empty() {
                "completed"
            } else {
                "running"
            },
            "Workflow-Schritt wurde lokal abgeschlossen.",
            Some(step),
            &step_results,
            &context,
        )?;
    }

    if transitions >= max_transitions && !current_action.is_empty() {
        let browser_cleanup = close_workflow_browser_for_run(app, job, &steps, &mut context);
        return Ok(json!({
            "ok": false,
            "status": "failed",
            "statusMessage": "Zu viele Workflow-Routenwechsel. Moegliche Schleife.",
            "steps": step_results,
            "workflow": context,
            "browserCleanup": browser_cleanup,
        }));
    }

    let browser_cleanup = close_workflow_browser_for_run(app, job, &steps, &mut context);
    let final_result = json!({
        "ok": true,
        "status": "success",
        "statusMessage": "Workflow wurde vollstaendig auf dem ClientController ausgefuehrt.",
        "steps": step_results,
        "workflow": context,
        "browserCleanup": browser_cleanup,
        "workflow_variables": context.get("workflow_variables").cloned(),
        "workflowVariables": context.get("workflowVariables").cloned(),
        "workflow_return": context.get("workflow_return").cloned(),
        "workflowReturn": context.get("workflowReturn").cloned(),
        "workflow_return_ok": context.get("workflow_return_ok").cloned(),
        "account": context.get("account").cloned(),
        "new_password": context.get("new_password").cloned(),
        "generated_password": context.get("generated_password").cloned(),
        "generated-password": context.get("generated-password").cloned(),
        "verification_code": context.get("verification_code").cloned(),
        "verificationCode": context.get("verificationCode").cloned(),
        "browserWindows": context.get("browserWindows").cloned(),
        "browserWsEndpoint": context.get("browserWsEndpoint").cloned(),
        "browserIdentity": context.get("browserIdentity").cloned(),
        "webmailSessionFilePath": context.get("webmailSessionFilePath").cloned(),
        "remoteWebmailSessionPayload": context.get("remoteWebmailSessionPayload").cloned(),
        "browserSessionFilePath": context.get("browserSessionFilePath").cloned(),
        "remoteBrowserSessionPayload": context.get("remoteBrowserSessionPayload").cloned(),
        "browserSessionDeleted": context.get("browserSessionDeleted").cloned(),
        "deletedBrowserSession": context.get("deletedBrowserSession").cloned(),
        "finishedAt": now_iso(),
    });
    fs::write(
        run_dir.join("result.json"),
        serde_json::to_vec_pretty(&final_result)
            .map_err(|e| format!("serialize final workflow result failed: {e}"))?,
    )
    .map_err(|e| format!("write final workflow result failed: {e}"))?;
    write_full_workflow_snapshot(
        app,
        job,
        "completed",
        "Workflow wurde vollstaendig auf dem ClientController ausgefuehrt.",
        None,
        &step_results,
        &context,
    )?;

    Ok(final_result)
}

fn execute_node_control_job(app: &tauri::AppHandle, job_type: &str) -> Result<Value, String> {
    match job_type {
        "node_diagnostics" => {
            let status = get_client_status(app.clone())?;
            let outbox = pending_outbox_internal(app, 25)?;

            Ok(json!({
                "ok": true,
                "statusMessage": "Node-Diagnose wurde erfasst.",
                "capturedAt": now_iso(),
                "client": {
                    "nodeUuid": status.config.node_uuid,
                    "serverDomain": status.config.server_domain,
                    "lastSuccessfulServer": status.config.last_successful_server,
                    "environment": status.config.environment,
                    "allowServerRebind": status.config.allow_server_rebind,
                    "adbEnabled": status.config.adb_enabled,
                    "adbDeviceDiscoveryEnabled": status.config.adb_device_discovery_enabled,
                    "pendingEvents": status.pending_events,
                    "localDevices": status.local_devices,
                    "adbSource": status.adb_source,
                    "adbAvailable": status.adb_available,
                    "nodeAvailable": status.node_available,
                    "workflowRuntimeAvailable": status.workflow_runtime_available,
                    "workflowRuntimePath": status.workflow_runtime_path,
                },
                "outboxPreview": outbox,
            }))
        }
        "node_outbox_list" => {
            let events = pending_outbox_internal(app, 200)?;

            Ok(json!({
                "ok": true,
                "statusMessage": format!("{} ausstehende Outbox-Eintraege gelesen.", events.len()),
                "count": events.len(),
                "events": events,
                "capturedAt": now_iso(),
            }))
        }
        "node_outbox_clear" => {
            let deleted = clear_outbox_internal(app)?;

            Ok(json!({
                "ok": true,
                "statusMessage": format!("Lokale Outbox geleert: {} Eintraege.", deleted),
                "deleted": deleted,
                "completedAt": now_iso(),
            }))
        }
        "node_discover_devices" => {
            if !adb_device_discovery_enabled(app) {
                return Ok(json!({
                    "ok": true,
                    "statusMessage": "ADB-Geraetesuche ist lokal deaktiviert.",
                    "count": 0,
                    "devices": [],
                    "skipped": true,
                    "completedAt": now_iso(),
                }));
            }

            let devices = discover_android_devices_internal(app, true)?;

            Ok(json!({
                "ok": true,
                "statusMessage": format!("{} lokale Geraete erkannt.", devices.len()),
                "count": devices.len(),
                "devices": devices,
                "completedAt": now_iso(),
            }))
        }
        "node_sync" => {
            let discovery_enabled = adb_device_discovery_enabled(app);
            let devices = if discovery_enabled {
                discover_android_devices_internal(app, true)?
            } else {
                Vec::new()
            };
            let synced = if discovery_enabled {
                sync_devices_remote_internal(app)?
            } else {
                0
            };
            heartbeat_remote_internal(
                app,
                "online",
                Some(json!({
                    "source": "remote-node-sync",
                    "discovered_devices": devices.len(),
                    "synced_devices": synced,
                })),
            )?;

            Ok(json!({
                "ok": true,
                "statusMessage": if discovery_enabled { "Node-Synchronisierung abgeschlossen." } else { "Node-Synchronisierung ohne ADB-Geraetesuche abgeschlossen." },
                "discoveredDevices": devices.len(),
                "syncedDevices": synced,
                "adbDeviceDiscoveryEnabled": discovery_enabled,
                "completedAt": now_iso(),
            }))
        }
        _ => Err(format!("unsupported node control job type: {job_type}")),
    }
}

fn execute_node_update(app: &tauri::AppHandle, job: &RemoteJob) -> Result<Value, String> {
    let _ = (app, job);
    Err("node_update is currently disabled in this ClientController build".to_string())
}

struct WorkflowJobGuard(bool);

impl Drop for WorkflowJobGuard {
    fn drop(&mut self) {
        if self.0 {
            WORKFLOW_JOB_RUNNING.store(false, Ordering::SeqCst);
        }
    }
}

fn execute_remote_job(app: tauri::AppHandle, job: RemoteJob) {
    let is_workflow_job = matches!(job.job_type.as_str(), "workflow_task" | "workflow_run");
    let acquired_workflow_slot = !is_workflow_job
        || WORKFLOW_JOB_RUNNING
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
    let _workflow_guard = WorkflowJobGuard(is_workflow_job && acquired_workflow_slot);
    let execution_id = start_local_job_execution(&app, &job).ok();
    let _ = initialize_job_delivery(&app, &job);

    if !acquired_workflow_slot {
        let error = "another workflow job is already running on this node".to_string();
        let details = json!({ "ok": false, "status": "failed", "statusMessage": error });
        if let Some(execution_id) = execution_id {
            let _ = finish_local_job_execution(&app, execution_id, "failed", &details);
        }
        let _ = queue_job_result_delivery(&app, &job, "failed", details, Some(error));
        let _ = flush_job_delivery_outbox(&app, 10);
        return;
    }
    let cfg = match load_or_create_config(&app) {
        Ok(cfg) => cfg,
        Err(error) => {
            if let Some(execution_id) = execution_id {
                let _ = finish_local_job_execution(
                    &app,
                    execution_id,
                    "failed",
                    &json!({ "error": error }),
                );
            }
            return;
        }
    };

    let execution = verify_job_signature(&cfg, &job).and_then(|_| match job.job_type.as_str() {
        "workflow_task" => execute_workflow_task_job(&app, &job),
        "workflow_run" => execute_workflow_run_job(&app, &job),
        "ping" => Ok(json!({ "ok": true, "statusMessage": "ClientController node is reachable", "at": now_iso() })),
        "node_diagnostics" | "node_outbox_list" | "node_outbox_clear" | "node_discover_devices" | "node_sync" => {
            execute_node_control_job(&app, &job.job_type)
        }
        "node_update" => execute_node_update(&app, &job),
        other => Err(format!("unsupported ClientController job type: {other}")),
    });

    match execution {
        Ok(result) => {
            let execution_ok = result.get("ok").and_then(Value::as_bool).unwrap_or(true);
            let declared_status = result.get("status").and_then(Value::as_str).unwrap_or("");
            let remote_status = match declared_status {
                "cancelled" | "canceled" => "cancelled",
                "timed_out" | "timeout" => "timed_out",
                "failed" | "error" => "failed",
                _ if execution_ok => "success",
                _ => "failed",
            };
            let result_error = if execution_ok {
                None
            } else {
                result
                    .get("statusMessage")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            };
            if let Some(execution_id) = execution_id {
                let _ = finish_local_job_execution(&app, execution_id, remote_status, &result);
            }
            if let Err(error) =
                queue_job_result_delivery(&app, &job, remote_status, result, result_error)
            {
                let _ = queue_local_event(
                    &app,
                    "job_result_failed",
                    json!({ "job_uuid": job.job_uuid, "error": error }),
                );
            }
            let _ = flush_job_delivery_outbox(&app, 10);
        }
        Err(error) => {
            let details =
                json!({ "ok": false, "status": "failed", "statusMessage": error.clone() });
            if let Some(execution_id) = execution_id {
                let _ = finish_local_job_execution(&app, execution_id, "failed", &details);
            }
            if let Err(report_error) =
                queue_job_result_delivery(&app, &job, "failed", details, Some(error))
            {
                let _ = queue_local_event(
                    &app,
                    "job_result_failed",
                    json!({ "job_uuid": job.job_uuid, "error": report_error }),
                );
            }
            let _ = flush_job_delivery_outbox(&app, 10);
        }
    }
}

fn pull_and_start_jobs_remote_internal(app: &tauri::AppHandle) -> Result<usize, String> {
    if WORKFLOW_JOB_RUNNING.load(Ordering::SeqCst) {
        return Ok(0);
    }

    let cfg = load_or_create_config(app)?;
    let endpoint = format!(
        "{}/api/client-controller/pull-jobs",
        base_url(&cfg.server_domain)
    );
    let resume_job_uuids = interrupted_workflow_job_ids(app)?;
    let response = http_client()?
        .post(endpoint)
        .header("X-NODE-API-KEY", cfg.api_key.clone())
        .json(&json!({
            "api_key": cfg.api_key,
            "protocol_version": 2,
            "resume_job_uuids": resume_job_uuids,
        }))
        .send()
        .map_err(|e| format!("pull jobs request failed: {e}"))?;
    let status_code = response.status();
    let body: Value = response
        .json()
        .map_err(|e| format!("pull jobs response parse failed: {e}"))?;

    if !status_code.is_success()
        || !body
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Err(format!(
            "pull jobs failed: HTTP {} - {}",
            status_code.as_u16(),
            body
        ));
    }

    let jobs: Vec<RemoteJob> =
        serde_json::from_value(body.get("jobs").cloned().unwrap_or_else(|| json!([])))
            .map_err(|e| format!("parse pulled jobs failed: {e}"))?;
    let count = jobs.len();

    for job in jobs {
        let handle = app.clone();
        std::thread::spawn(move || execute_remote_job(handle, job));
    }

    Ok(count)
}

fn autopilot_cycle_internal(app: &tauri::AppHandle) -> Result<SyncSummary, String> {
    bootstrap_local_runtime(app.clone())?;

    let mut notes: Vec<String> = Vec::new();
    match flush_job_delivery_outbox(app, 50) {
        Ok(sent) if sent > 0 => notes.push(format!("deliveries:ok({sent})")),
        Ok(_) => {}
        Err(error) => notes.push(format!(
            "deliveries:deferred({})",
            preview_body(&error, 120)
        )),
    }
    let mut registered = false;
    let mut discovered_devices = 0usize;
    let mut synced_devices = 0usize;
    let mut heartbeat_sent = false;
    let mut jobs_started = 0usize;
    let discovery_enabled = adb_device_discovery_enabled(app);

    match load_or_create_config(app) {
        Ok(cfg) => {
            if cfg.api_key.trim().is_empty() || cfg.api_key == cfg.bootstrap_api_key {
                match register_node_remote_internal(app, None) {
                    Ok(_) => {
                        registered = true;
                        notes.push("register:ok".to_string());
                    }
                    Err(err) => {
                        let _ = queue_local_event(
                            app,
                            "register_node_failed",
                            json!({ "error": err.clone() }),
                        );
                        notes.push(format!("register:fail({})", preview_body(&err, 120)));
                    }
                }
            } else {
                notes.push("register:skip(existing api_key)".to_string());
            }
        }
        Err(err) => {
            notes.push(format!("config:fail({})", preview_body(&err, 120)));
        }
    }

    if discovery_enabled {
        match discover_android_devices_internal(app, true) {
            Ok(devices) => {
                discovered_devices = devices.iter().filter(|d| d.status == "online").count();
                notes.push(format!("discover:ok({})", discovered_devices));
            }
            Err(err) => {
                let _ = queue_local_event(
                    app,
                    "discover_devices_failed",
                    json!({ "error": err.clone() }),
                );
                notes.push(format!("discover:fail({})", preview_body(&err, 120)));
            }
        }
    } else {
        notes.push("discover:skip(disabled)".to_string());
    }

    if discovery_enabled {
        match sync_devices_remote_internal(app) {
            Ok(count) => {
                synced_devices = count;
                notes.push(format!("sync:ok({})", count));
            }
            Err(err) => match recover_node_registration(app, &err) {
                Ok(true) => {
                    registered = true;
                    notes.push("register:recovered(unauthorized)".to_string());

                    match sync_devices_remote_internal(app) {
                        Ok(count) => {
                            synced_devices = count;
                            notes.push(format!("sync:retry-ok({})", count));
                        }
                        Err(retry_err) => {
                            let _ = queue_local_event(
                                app,
                                "sync_devices_failed",
                                json!({ "error": retry_err.clone(), "after_reregister": true }),
                            );
                            notes.push(format!(
                                "sync:retry-fail({})",
                                preview_body(&retry_err, 120)
                            ));
                        }
                    }
                }
                Ok(false) => {
                    let _ = queue_local_event(
                        app,
                        "sync_devices_failed",
                        json!({ "error": err.clone() }),
                    );
                    notes.push(format!("sync:fail({})", preview_body(&err, 120)));
                }
                Err(register_err) => {
                    let _ = queue_local_event(
                        app,
                        "register_node_failed",
                        json!({ "error": register_err.clone(), "trigger": err }),
                    );
                    notes.push(format!(
                        "register:recovery-fail({})",
                        preview_body(&register_err, 120)
                    ));
                }
            },
        }
    } else {
        notes.push("sync:skip(disabled)".to_string());
    }

    match heartbeat_remote_internal(
        app,
        "online",
        Some(json!({
            "source": "autopilot_cycle",
            "discovered_devices": discovered_devices,
            "synced_devices": synced_devices,
        })),
    ) {
        Ok(_) => {
            heartbeat_sent = true;
            notes.push("heartbeat:ok".to_string());
        }
        Err(err) => match recover_node_registration(app, &err) {
            Ok(true) => {
                registered = true;
                notes.push("register:recovered-before-heartbeat".to_string());

                match heartbeat_remote_internal(
                    app,
                    "online",
                    Some(json!({ "source": "autopilot-retry" })),
                ) {
                    Ok(_) => {
                        heartbeat_sent = true;
                        notes.push("heartbeat:retry-ok".to_string());
                    }
                    Err(retry_err) => {
                        let _ = queue_local_event(
                            app,
                            "heartbeat_failed",
                            json!({ "error": retry_err.clone(), "after_reregister": true }),
                        );
                        notes.push(format!(
                            "heartbeat:retry-fail({})",
                            preview_body(&retry_err, 120)
                        ));
                    }
                }
            }
            Ok(false) => {
                let _ = queue_local_event(app, "heartbeat_failed", json!({ "error": err.clone() }));
                notes.push(format!("heartbeat:fail({})", preview_body(&err, 120)));
            }
            Err(register_err) => {
                let _ = queue_local_event(
                    app,
                    "register_node_failed",
                    json!({ "error": register_err.clone(), "trigger": err }),
                );
                notes.push(format!(
                    "register:heartbeat-recovery-fail({})",
                    preview_body(&register_err, 120)
                ));
            }
        },
    }

    match pull_and_start_jobs_remote_internal(app) {
        Ok(count) => {
            jobs_started = count;
            notes.push(format!("jobs:ok({})", count));
        }
        Err(err) => match recover_node_registration(app, &err) {
            Ok(true) => {
                registered = true;
                notes.push("register:recovered-before-jobs".to_string());

                match pull_and_start_jobs_remote_internal(app) {
                    Ok(count) => {
                        jobs_started = count;
                        notes.push(format!("jobs:retry-ok({})", count));
                    }
                    Err(retry_err) => {
                        let _ = queue_local_event(
                            app,
                            "pull_jobs_failed",
                            json!({ "error": retry_err.clone(), "after_reregister": true }),
                        );
                        notes.push(format!(
                            "jobs:retry-fail({})",
                            preview_body(&retry_err, 120)
                        ));
                    }
                }
            }
            Ok(false) => {
                let _ = queue_local_event(app, "pull_jobs_failed", json!({ "error": err.clone() }));
                notes.push(format!("jobs:fail({})", preview_body(&err, 120)));
            }
            Err(register_err) => {
                let _ = queue_local_event(
                    app,
                    "register_node_failed",
                    json!({ "error": register_err.clone(), "trigger": err }),
                );
                notes.push(format!(
                    "register:jobs-recovery-fail({})",
                    preview_body(&register_err, 120)
                ));
            }
        },
    }

    Ok(SyncSummary {
        registered,
        discovered_devices,
        synced_devices,
        heartbeat_sent,
        jobs_started,
        message: notes.join(" | "),
    })
}

#[tauri::command]
fn bootstrap_local_runtime(app: tauri::AppHandle) -> Result<GenericResult, String> {
    let cfg = load_or_create_config(&app)?;
    init_db(&app)?;
    save_config(&app, &cfg)?;
    stage_bundled_tooling_best_effort(&app);

    Ok(GenericResult {
        success: true,
        message: "Local runtime initialized (config + sqlite)".to_string(),
    })
}

#[tauri::command]
fn get_client_status(app: tauri::AppHandle) -> Result<ClientStatus, String> {
    let cfg = load_or_create_config(&app)?;
    init_db(&app)?;

    let conn = open_db(&app)?;
    let pending_events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM outbox_events WHERE status = 'pending'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("count pending events failed: {e}"))?;

    let local_devices: i64 = conn
        .query_row("SELECT COUNT(*) FROM local_devices", [], |row| row.get(0))
        .map_err(|e| format!("count local devices failed: {e}"))?;

    let running_processes: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM job_executions_local WHERE status = 'running'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("count running processes failed: {e}"))?;

    let (adb_source, adb_available) = detect_adb_source(&app);
    let workflow_runtime = resolve_workflow_runtime(&app);
    let node_available = workflow_runtime
        .as_deref()
        .and_then(bundled_workflow_node_binary)
        .is_some();

    Ok(ClientStatus {
        config: cfg,
        pending_events,
        local_devices,
        adb_source,
        adb_available,
        db_path: db_path(&app)?.to_string_lossy().to_string(),
        config_path: config_path(&app)?.to_string_lossy().to_string(),
        node_available,
        workflow_runtime_available: workflow_runtime.is_some(),
        workflow_runtime_path: workflow_runtime
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_default(),
        app_version: env!("CARGO_PKG_VERSION").to_string(),
        running_processes,
        cpu_load_percent: local_cpu_load_percent(),
        updater_available: false,
    })
}

#[tauri::command]
fn get_local_processes(
    app: tauri::AppHandle,
    limit: Option<i64>,
) -> Result<Vec<LocalProcess>, String> {
    local_processes_internal(&app, limit.unwrap_or(100))
}

#[tauri::command]
fn get_workflow_process_preview(
    app: tauri::AppHandle,
    job_uuid: String,
) -> Result<WorkflowProcessPreview, String> {
    workflow_process_preview_internal(&app, &job_uuid)
}

#[tauri::command]
fn export_workflow_process_debug(
    app: tauri::AppHandle,
    job_uuid: String,
) -> Result<String, String> {
    export_workflow_process_debug_internal(&app, &job_uuid)
}

#[tauri::command]
fn update_server_domain(
    app: tauri::AppHandle,
    server_domain: String,
) -> Result<GenericResult, String> {
    let mut cfg = load_or_create_config(&app)?;
    let normalized = canonical_server_domain(&server_domain);
    cfg.server_domain = normalized.clone();

    if cfg.last_successful_server.trim().is_empty() {
        cfg.last_successful_server = normalized.clone();
    }

    save_config(&app, &cfg)?;

    Ok(GenericResult {
        success: true,
        message: format!("server_domain updated to {normalized}"),
    })
}

#[tauri::command]
fn update_adb_settings(
    app: tauri::AppHandle,
    settings: AdbSettingsUpdate,
) -> Result<GenericResult, String> {
    let mut cfg = load_or_create_config(&app)?;
    cfg.adb_enabled = settings.adb_enabled;
    cfg.adb_device_discovery_enabled =
        settings.adb_enabled && settings.adb_device_discovery_enabled;
    save_config(&app, &cfg)?;

    Ok(GenericResult {
        success: true,
        message: if cfg.adb_enabled {
            "ADB-Einstellungen gespeichert.".to_string()
        } else {
            "ADB-Steuerung deaktiviert.".to_string()
        },
    })
}

#[tauri::command]
fn queue_event_local(
    app: tauri::AppHandle,
    event_type: String,
    payload: Value,
) -> Result<GenericResult, String> {
    queue_local_event(&app, &event_type, payload)?;

    Ok(GenericResult {
        success: true,
        message: "Event stored locally (pending sync)".to_string(),
    })
}

#[tauri::command]
fn get_pending_events(
    app: tauri::AppHandle,
    limit: Option<i64>,
) -> Result<Vec<OutboxEvent>, String> {
    init_db(&app)?;
    let conn = open_db(&app)?;
    let lim = limit.unwrap_or(50).max(1);

    let mut stmt = conn
        .prepare(
            "SELECT id, event_type, payload_json, created_at
             FROM outbox_events
             WHERE status = 'pending'
             ORDER BY id ASC
             LIMIT ?1",
        )
        .map_err(|e| format!("prepare query failed: {e}"))?;

    let rows = stmt
        .query_map(params![lim], |row| {
            Ok(OutboxEvent {
                id: row.get(0)?,
                event_type: row.get(1)?,
                payload_json: row.get(2)?,
                created_at: row.get(3)?,
            })
        })
        .map_err(|e| format!("query pending events failed: {e}"))?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row.map_err(|e| format!("read row failed: {e}"))?);
    }

    Ok(result)
}

#[tauri::command]
fn mark_event_sent(app: tauri::AppHandle, event_id: i64) -> Result<GenericResult, String> {
    init_db(&app)?;
    let conn = open_db(&app)?;

    let changed = conn
        .execute(
            "UPDATE outbox_events SET status = 'sent', sent_at = ?1 WHERE id = ?2",
            params![now_iso(), event_id],
        )
        .map_err(|e| format!("mark event sent failed: {e}"))?;

    if changed == 0 {
        return Ok(GenericResult {
            success: false,
            message: format!("No event found for id {event_id}"),
        });
    }

    Ok(GenericResult {
        success: true,
        message: format!("Event {event_id} marked as sent"),
    })
}

#[tauri::command]
fn log_heartbeat_local(
    app: tauri::AppHandle,
    status: String,
    details: Option<Value>,
) -> Result<GenericResult, String> {
    init_db(&app)?;
    let conn = open_db(&app)?;

    let details_json = details
        .map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string()))
        .unwrap_or_else(|| "{}".to_string());

    conn.execute(
        "INSERT INTO heartbeat_logs_local (status, details_json, created_at) VALUES (?1, ?2, ?3)",
        params![status, details_json, now_iso()],
    )
    .map_err(|e| format!("insert heartbeat log failed: {e}"))?;

    Ok(GenericResult {
        success: true,
        message: "Heartbeat logged locally".to_string(),
    })
}

#[tauri::command]
fn apply_rebind_request(
    app: tauri::AppHandle,
    request: RebindRequest,
) -> Result<GenericResult, String> {
    let mut cfg = load_or_create_config(&app)?;
    init_db(&app)?;
    let conn = open_db(&app)?;

    let old_server = cfg.server_domain.clone();
    let normalized_new_server = canonical_server_domain(&request.new_server_domain);

    if !cfg.allow_server_rebind {
        conn.execute(
            "INSERT INTO rebind_logs_local (old_server_domain, new_server_domain, status, reason, created_at)
             VALUES (?1, ?2, 'rejected', ?3, ?4)",
            params![old_server, normalized_new_server, "allow_server_rebind=false", now_iso()],
        )
        .map_err(|e| format!("log rejected rebind failed: {e}"))?;

        return Ok(GenericResult {
            success: false,
            message: "Rebind blocked: allow_server_rebind=false".to_string(),
        });
    }

    if request.signature.trim().is_empty() {
        conn.execute(
            "INSERT INTO rebind_logs_local (old_server_domain, new_server_domain, status, reason, created_at)
             VALUES (?1, ?2, 'rejected', ?3, ?4)",
            params![old_server, normalized_new_server, "missing signature", now_iso()],
        )
        .map_err(|e| format!("log rejected rebind failed: {e}"))?;

        return Ok(GenericResult {
            success: false,
            message: "Rebind blocked: missing signature".to_string(),
        });
    }

    let expires = DateTime::parse_from_rfc3339(&request.expires_at)
        .map_err(|e| format!("invalid expires_at format (RFC3339 expected): {e}"))?
        .with_timezone(&Utc);

    if Utc::now() > expires {
        conn.execute(
            "INSERT INTO rebind_logs_local (old_server_domain, new_server_domain, status, reason, created_at)
             VALUES (?1, ?2, 'rejected', ?3, ?4)",
            params![old_server, normalized_new_server, "request expired", now_iso()],
        )
        .map_err(|e| format!("log expired rebind failed: {e}"))?;

        return Ok(GenericResult {
            success: false,
            message: "Rebind blocked: request expired".to_string(),
        });
    }

    cfg.last_successful_server = cfg.server_domain.clone();
    cfg.server_domain = normalized_new_server.clone();
    save_config(&app, &cfg)?;

    conn.execute(
        "INSERT INTO rebind_logs_local (old_server_domain, new_server_domain, status, reason, created_at)
         VALUES (?1, ?2, 'applied', ?3, ?4)",
        params![old_server, normalized_new_server, "applied (mvp)", now_iso()],
    )
    .map_err(|e| format!("log successful rebind failed: {e}"))?;

    Ok(GenericResult {
        success: true,
        message: "Rebind applied (MVP validation)".to_string(),
    })
}

#[tauri::command]
fn register_node_remote(
    app: tauri::AppHandle,
    node_name: Option<String>,
) -> Result<GenericResult, String> {
    match register_node_remote_internal(&app, node_name) {
        Ok(_) => Ok(GenericResult {
            success: true,
            message: "Node successfully registered on server".to_string(),
        }),
        Err(err) => {
            let _ = queue_local_event(
                &app,
                "register_node_failed",
                json!({ "error": err.clone() }),
            );
            Err(err)
        }
    }
}

#[tauri::command]
fn send_heartbeat_remote(
    app: tauri::AppHandle,
    status: Option<String>,
    payload: Option<Value>,
) -> Result<GenericResult, String> {
    let hb_status = status.unwrap_or_else(|| "online".to_string());

    let _ = log_heartbeat_local(
        app.clone(),
        hb_status.clone(),
        Some(json!({ "source": "remote_heartbeat_attempt" })),
    );

    match heartbeat_remote_internal(&app, &hb_status, payload) {
        Ok(_) => Ok(GenericResult {
            success: true,
            message: "Heartbeat sent to server".to_string(),
        }),
        Err(err) => {
            let _ = queue_local_event(&app, "heartbeat_failed", json!({ "error": err.clone() }));
            Err(err)
        }
    }
}

#[tauri::command]
fn discover_android_devices(app: tauri::AppHandle) -> Result<Vec<LocalDevice>, String> {
    discover_android_devices_internal(&app, true)
}

#[tauri::command]
fn install_windows_usb_driver(app: tauri::AppHandle) -> Result<GenericResult, String> {
    match try_install_windows_usb_driver_best_effort(&app) {
        Ok(msg) => Ok(GenericResult {
            success: true,
            message: msg,
        }),
        Err(err) => Err(err),
    }
}

#[tauri::command]
fn get_local_devices(app: tauri::AppHandle) -> Result<Vec<LocalDevice>, String> {
    load_local_devices_internal(&app)
}

#[tauri::command]
fn sync_devices_remote(app: tauri::AppHandle) -> Result<GenericResult, String> {
    match sync_devices_remote_internal(&app) {
        Ok(count) => Ok(GenericResult {
            success: true,
            message: format!("{count} devices synced to server"),
        }),
        Err(err) => {
            let _ = queue_local_event(&app, "sync_devices_failed", json!({ "error": err.clone() }));
            Err(err)
        }
    }
}

#[tauri::command]
fn run_autopilot_cycle(app: tauri::AppHandle) -> Result<SyncSummary, String> {
    autopilot_cycle_internal(&app)
}

#[tauri::command]
fn run_full_sync(app: tauri::AppHandle) -> Result<SyncSummary, String> {
    autopilot_cycle_internal(&app)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let app = tauri::Builder::default()
        .setup(|app| {
            let handle = app.handle().clone();
            std::thread::spawn(move || {
                let _ = bootstrap_local_runtime(handle.clone());

                loop {
                    let _ = autopilot_cycle_internal(&handle);
                    std::thread::sleep(Duration::from_secs(30));
                }
            });
            Ok(())
        })
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            bootstrap_local_runtime,
            get_client_status,
            get_local_processes,
            get_workflow_process_preview,
            export_workflow_process_debug,
            update_server_domain,
            update_adb_settings,
            queue_event_local,
            get_pending_events,
            mark_event_sent,
            log_heartbeat_local,
            apply_rebind_request,
            register_node_remote,
            send_heartbeat_remote,
            discover_android_devices,
            install_windows_usb_driver,
            get_local_devices,
            sync_devices_remote,
            run_autopilot_cycle,
            run_full_sync,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application");

    app.run(|app_handle, event| {
        if matches!(event, tauri::RunEvent::Exit) {
            stop_bundled_adb_processes(app_handle);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        authentication_failed, chromium_no_sandbox_enabled, delivery_ack_from_response,
        merge_workflow_context, route_type_and_target, workflow_step_route,
    };
    use serde_json::json;

    #[test]
    fn detects_node_authentication_failures() {
        assert!(authentication_failed(
            "sync-devices failed: HTTP 401 - {\"message\":\"Unauthorized node.\"}"
        ));
        assert!(authentication_failed(
            "register failed: HTTP 403 - forbidden"
        ));
        assert!(!authentication_failed("request failed: connection refused"));
    }

    #[test]
    fn parses_stop_control_from_progress_acknowledgement() {
        let ack = delivery_ack_from_response(
            &json!({
                "acknowledged_sequence": 7,
                "control": {
                    "command": "stop",
                    "sequence": 2,
                    "payload": {"result_status": "timed_out"}
                }
            }),
            1,
        )
        .expect("control response should parse");

        assert_eq!(ack.acknowledged_sequence, 7);
        let control = ack.control.expect("stop control should exist");
        assert_eq!(control.command, "stop");
        assert_eq!(control.sequence, 2);
        assert_eq!(control.payload["result_status"], "timed_out");
    }

    #[test]
    fn chromium_sandbox_is_only_disabled_by_explicit_runtime_configuration() {
        assert!(!chromium_no_sandbox_enabled(&json!({})));
        assert!(!chromium_no_sandbox_enabled(
            &json!({"chromiumNoSandbox": false})
        ));
        assert!(chromium_no_sandbox_enabled(
            &json!({"chromiumNoSandbox": true})
        ));
    }

    #[test]
    fn closed_browser_is_removed_from_workflow_context() {
        let mut context = json!({
            "browserWsEndpoint": "ws://127.0.0.1/devtools/browser/test",
            "browser_ws_endpoint": "ws://127.0.0.1/devtools/browser/test",
            "browserWindows": [{"key": "main", "targetId": "target-1"}],
            "browser_windows": [{"key": "main", "targetId": "target-1"}],
            "workflow_variables": {"kept": true}
        });

        merge_workflow_context(
            &mut context,
            &json!({
                "closedBrowser": true,
                "browserWindows": [],
                "browserWsEndpoint": "ws://stale",
            }),
        );

        assert!(context.get("browserWsEndpoint").is_none());
        assert!(context.get("browser_ws_endpoint").is_none());
        assert!(context.get("browserWindows").is_none());
        assert!(context.get("browser_windows").is_none());
        assert_eq!(context["workflow_variables"]["kept"], true);
    }

    #[test]
    fn resolves_task_and_linear_workflow_routes() {
        let step = json!({
            "actionKey": "first",
            "defaultNext": "second",
            "routes": {},
            "runtime": {
                "tasks": [{
                    "key": "branch",
                    "next": {"type": "card", "card_key": "continue"}
                }]
            }
        });
        let task_route = workflow_step_route(
            &step,
            &json!({"routeRequested": true, "completedTaskKey": "branch"}),
            "success",
        );
        assert_eq!(
            route_type_and_target(&task_route, "first"),
            (
                "card".to_string(),
                "first".to_string(),
                "continue".to_string()
            )
        );

        let linear_route = workflow_step_route(&step, &json!({"ok": true}), "success");
        assert_eq!(
            route_type_and_target(&linear_route, "first"),
            ("step".to_string(), "second".to_string(), "".to_string())
        );
    }
}
