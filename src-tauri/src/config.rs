use crate::credentials::{
    delete as delete_secret, load as load_secret, store as store_secret, ClientSecret,
};
use chrono::Utc;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

use super::ensure_runtime_dir;

const DEFAULT_SERVER_DOMAIN: &str = "https://factory.follow-flow.de";

#[derive(Serialize, Deserialize, Clone)]
#[serde(default)]
pub(crate) struct ClientConfig {
    pub(crate) server_domain: String,
    pub(crate) node_uuid: String,
    #[serde(default, skip_serializing)]
    pub(crate) api_key: String,
    #[serde(default, skip_serializing)]
    pub(crate) bootstrap_api_key: String,
    pub(crate) environment: String,
    pub(crate) allow_server_rebind: bool,
    pub(crate) adb_enabled: bool,
    pub(crate) adb_device_discovery_enabled: bool,
    pub(crate) last_successful_server: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_domain: DEFAULT_SERVER_DOMAIN.to_string(),
            node_uuid: format!("node-{}", Utc::now().timestamp_millis()),
            api_key: String::new(),
            bootstrap_api_key: String::new(),
            environment: "production".to_string(),
            allow_server_rebind: false,
            adb_enabled: true,
            adb_device_discovery_enabled: true,
            last_successful_server: DEFAULT_SERVER_DOMAIN.to_string(),
        }
    }
}

pub(crate) fn config_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(ensure_runtime_dir(app)?.join("client.json"))
}

fn nonempty_secret(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn hydrate_secure_credentials(cfg: &mut ClientConfig) -> Result<(), String> {
    let legacy_api_key = nonempty_secret(std::mem::take(&mut cfg.api_key));
    let legacy_bootstrap_api_key = nonempty_secret(std::mem::take(&mut cfg.bootstrap_api_key));
    let mut api_key = load_secret(ClientSecret::NodeApiKey, &cfg.node_uuid)?;
    let mut bootstrap_api_key = load_secret(ClientSecret::BootstrapApiKey, &cfg.node_uuid)?;

    if api_key.is_none() {
        if let Some(legacy_api_key) = legacy_api_key {
            if legacy_bootstrap_api_key.as_deref() == Some(legacy_api_key.as_str()) {
                if bootstrap_api_key.is_none() {
                    store_secret(
                        ClientSecret::BootstrapApiKey,
                        &cfg.node_uuid,
                        &legacy_api_key,
                    )?;
                    bootstrap_api_key = Some(legacy_api_key);
                }
            } else {
                store_secret(ClientSecret::NodeApiKey, &cfg.node_uuid, &legacy_api_key)?;
                api_key = Some(legacy_api_key);
            }
        }
    }

    if api_key.is_none() && bootstrap_api_key.is_none() {
        if let Some(legacy_bootstrap_api_key) = legacy_bootstrap_api_key {
            store_secret(
                ClientSecret::BootstrapApiKey,
                &cfg.node_uuid,
                &legacy_bootstrap_api_key,
            )?;
            bootstrap_api_key = Some(legacy_bootstrap_api_key);
        }
    }

    if api_key.is_some() {
        delete_secret(ClientSecret::BootstrapApiKey, &cfg.node_uuid)?;
        bootstrap_api_key = None;
    }

    cfg.api_key = api_key.unwrap_or_default();
    cfg.bootstrap_api_key = bootstrap_api_key.unwrap_or_default();

    Ok(())
}

fn persisted_config_contains_secrets(raw: &str) -> bool {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .is_some_and(|object| {
            object.contains_key("api_key")
                || object.contains_key("bootstrap_api_key")
                || object.contains_key("node_key")
        })
}

fn serialize_persisted_config(cfg: &ClientConfig) -> Result<String, String> {
    serde_json::to_string_pretty(cfg).map_err(|e| format!("serialize config failed: {e}"))
}

pub(crate) fn load_or_create_config(app: &tauri::AppHandle) -> Result<ClientConfig, String> {
    let path = config_path(app)?;

    if !path.exists() {
        let mut cfg = ClientConfig::default();
        hydrate_secure_credentials(&mut cfg)?;
        let content = serialize_persisted_config(&cfg)?;
        fs::write(&path, content).map_err(|e| format!("write default config failed: {e}"))?;
        return Ok(cfg);
    }

    let raw = fs::read_to_string(&path).map_err(|e| format!("read config failed: {e}"))?;
    let mut cfg: ClientConfig =
        serde_json::from_str(&raw).map_err(|e| format!("parse config failed: {e}"))?;
    let contains_legacy_secrets = persisted_config_contains_secrets(&raw);
    let normalized_domains = normalize_config_domains(&mut cfg)?;
    hydrate_secure_credentials(&mut cfg)?;

    if contains_legacy_secrets || normalized_domains {
        save_config(app, &cfg)?;
    }

    Ok(cfg)
}

pub(crate) fn save_config(app: &tauri::AppHandle, cfg: &ClientConfig) -> Result<(), String> {
    let path = config_path(app)?;
    let content = serialize_persisted_config(cfg)?;
    fs::write(path, content).map_err(|e| format!("save config failed: {e}"))
}

pub(crate) fn adb_enabled(app: &tauri::AppHandle) -> bool {
    load_or_create_config(app)
        .map(|cfg| cfg.adb_enabled)
        .unwrap_or(true)
}

pub(crate) fn adb_device_discovery_enabled(app: &tauri::AppHandle) -> bool {
    load_or_create_config(app)
        .map(|cfg| cfg.adb_enabled && cfg.adb_device_discovery_enabled)
        .unwrap_or(true)
}

pub(crate) fn base_url(domain: &str) -> String {
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

pub(crate) fn validated_server_domain(input: &str) -> Result<String, String> {
    let domain = canonical_server_domain(input);
    let parsed = Url::parse(&domain).map_err(|error| format!("invalid server domain: {error}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "server domain must contain a host".to_string())?;
    let is_debug_loopback = cfg!(debug_assertions)
        && parsed.scheme() == "http"
        && matches!(host, "localhost" | "127.0.0.1" | "::1");

    if parsed.scheme() != "https" && !is_debug_loopback {
        return Err("server domain must use HTTPS".to_string());
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("server domain must not contain credentials".to_string());
    }

    if parsed.path() != "/" || parsed.query().is_some() || parsed.fragment().is_some() {
        return Err("server domain must be an origin without path, query, or fragment".to_string());
    }

    Ok(base_url(parsed.as_str()))
}

fn normalize_config_domains(cfg: &mut ClientConfig) -> Result<bool, String> {
    let mut changed = false;

    let normalized_server = validated_server_domain(&cfg.server_domain)?;
    if normalized_server != cfg.server_domain {
        cfg.server_domain = normalized_server;
        changed = true;
    }

    if cfg.last_successful_server.trim().is_empty() {
        cfg.last_successful_server = cfg.server_domain.clone();
        changed = true;
    } else {
        match validated_server_domain(&cfg.last_successful_server) {
            Ok(normalized_last) if normalized_last != cfg.last_successful_server => {
                cfg.last_successful_server = normalized_last;
                changed = true;
            }
            Err(_) => {
                cfg.last_successful_server = cfg.server_domain.clone();
                changed = true;
            }
            _ => {}
        }
    }

    if cfg.allow_server_rebind {
        cfg.allow_server_rebind = false;
        changed = true;
    }

    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_config_domains, persisted_config_contains_secrets, serialize_persisted_config,
        validated_server_domain, ClientConfig, DEFAULT_SERVER_DOMAIN,
    };
    use serde_json::json;
    use std::collections::BTreeSet;

    #[test]
    fn persisted_client_config_redacts_legacy_credentials() {
        let raw = json!({
            "server_domain": "https://factory.follow-flow.de",
            "node_uuid": "node-test",
            "node_key": "legacy-node-key",
            "api_key": "legacy-api-key",
            "bootstrap_api_key": "legacy-bootstrap-key",
            "environment": "production",
            "allow_server_rebind": true,
            "adb_enabled": true,
            "adb_device_discovery_enabled": true,
            "last_successful_server": "https://factory.follow-flow.de"
        })
        .to_string();
        let config: ClientConfig =
            serde_json::from_str(&raw).expect("legacy configuration should deserialize");

        assert_eq!(config.api_key, "legacy-api-key");
        assert_eq!(config.bootstrap_api_key, "legacy-bootstrap-key");
        assert!(persisted_config_contains_secrets(&raw));

        let persisted =
            serialize_persisted_config(&config).expect("configuration should serialize");
        assert!(!persisted_config_contains_secrets(&persisted));
        assert!(!persisted.contains("legacy-api-key"));
        assert!(!persisted.contains("legacy-bootstrap-key"));
        assert!(!persisted.contains("legacy-node-key"));
    }

    #[test]
    fn persisted_client_config_keeps_its_public_field_contract() {
        let persisted = serialize_persisted_config(&ClientConfig::default())
            .expect("default configuration should serialize");
        let object = serde_json::from_str::<serde_json::Value>(&persisted)
            .expect("persisted configuration should be valid JSON");
        let keys = object
            .as_object()
            .expect("persisted configuration should be an object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();

        assert_eq!(
            keys,
            BTreeSet::from([
                "adb_device_discovery_enabled",
                "adb_enabled",
                "allow_server_rebind",
                "environment",
                "last_successful_server",
                "node_uuid",
                "server_domain",
            ])
        );
    }

    #[test]
    fn server_domain_validation_requires_a_secure_origin() {
        assert_eq!(
            validated_server_domain("https://factory.follow-flow.de/")
                .expect("HTTPS origin should be accepted"),
            "https://factory.follow-flow.de"
        );
        assert!(validated_server_domain("http://factory.follow-flow.de").is_err());
        assert!(validated_server_domain("https://user:secret@example.org").is_err());
        assert!(validated_server_domain("https://example.org/api").is_err());

        if cfg!(debug_assertions) {
            assert!(validated_server_domain("http://127.0.0.1:8000").is_ok());
        }
    }

    #[test]
    fn config_normalization_canonicalizes_domains_and_disables_rebinding() {
        let mut config = ClientConfig {
            server_domain: "https://factory.followflow.de/".to_string(),
            last_successful_server: "https://invalid.example.org/path".to_string(),
            allow_server_rebind: true,
            ..ClientConfig::default()
        };

        assert!(
            normalize_config_domains(&mut config).expect("legacy config domains should normalize")
        );
        assert_eq!(config.server_domain, DEFAULT_SERVER_DOMAIN);
        assert_eq!(config.last_successful_server, DEFAULT_SERVER_DOMAIN);
        assert!(!config.allow_server_rebind);
    }
}
