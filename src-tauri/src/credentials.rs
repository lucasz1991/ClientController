use sha2::{Digest, Sha256};
#[cfg(target_os = "windows")]
use std::sync::Mutex;

#[cfg(target_os = "windows")]
const CREDENTIAL_SERVICE: &str = "de.followflow.clientcontroller";
#[cfg(target_os = "windows")]
static CREDENTIAL_STORE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy)]
pub(crate) enum ClientSecret {
    NodeApiKey,
    BootstrapApiKey,
}

impl ClientSecret {
    fn account_prefix(self) -> &'static str {
        match self {
            Self::NodeApiKey => "node-api-key",
            Self::BootstrapApiKey => "bootstrap-api-key",
        }
    }
}

fn credential_account(secret: ClientSecret, node_uuid: &str) -> String {
    let digest = Sha256::digest(node_uuid.as_bytes());
    format!("{}:{}", secret.account_prefix(), hex::encode(&digest[..16]))
}

#[cfg(target_os = "windows")]
pub(crate) fn load(secret: ClientSecret, node_uuid: &str) -> Result<Option<String>, String> {
    let _guard = CREDENTIAL_STORE_LOCK
        .lock()
        .map_err(|_| "Windows Credential Manager lock is poisoned".to_string())?;
    let entry = keyring::Entry::new(CREDENTIAL_SERVICE, &credential_account(secret, node_uuid))
        .map_err(|error| format!("initialize Windows Credential Manager entry failed: {error}"))?;

    match entry.get_password() {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(error) => Err(format!(
            "read secret from Windows Credential Manager failed: {error}"
        )),
    }
}

#[cfg(not(target_os = "windows"))]
pub(crate) fn load(_secret: ClientSecret, _node_uuid: &str) -> Result<Option<String>, String> {
    Err("OS credential storage is not configured for this platform".to_string())
}

#[cfg(target_os = "windows")]
pub(crate) fn store(secret: ClientSecret, node_uuid: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err("refusing to store an empty credential".to_string());
    }

    let _guard = CREDENTIAL_STORE_LOCK
        .lock()
        .map_err(|_| "Windows Credential Manager lock is poisoned".to_string())?;
    let entry = keyring::Entry::new(CREDENTIAL_SERVICE, &credential_account(secret, node_uuid))
        .map_err(|error| format!("initialize Windows Credential Manager entry failed: {error}"))?;
    entry
        .set_password(value)
        .map_err(|error| format!("write secret to Windows Credential Manager failed: {error}"))
}

#[cfg(not(target_os = "windows"))]
pub(crate) fn store(_secret: ClientSecret, _node_uuid: &str, _value: &str) -> Result<(), String> {
    Err("OS credential storage is not configured for this platform".to_string())
}

#[cfg(target_os = "windows")]
pub(crate) fn delete(secret: ClientSecret, node_uuid: &str) -> Result<(), String> {
    let _guard = CREDENTIAL_STORE_LOCK
        .lock()
        .map_err(|_| "Windows Credential Manager lock is poisoned".to_string())?;
    let entry = keyring::Entry::new(CREDENTIAL_SERVICE, &credential_account(secret, node_uuid))
        .map_err(|error| format!("initialize Windows Credential Manager entry failed: {error}"))?;

    match entry.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(error) => Err(format!(
            "delete secret from Windows Credential Manager failed: {error}"
        )),
    }
}

#[cfg(not(target_os = "windows"))]
pub(crate) fn delete(_secret: ClientSecret, _node_uuid: &str) -> Result<(), String> {
    Err("OS credential storage is not configured for this platform".to_string())
}

#[cfg(test)]
mod tests {
    use super::{credential_account, ClientSecret};

    #[test]
    fn credential_accounts_are_stable_and_separated_by_secret_kind() {
        let node_api = credential_account(ClientSecret::NodeApiKey, "node-test");
        let bootstrap = credential_account(ClientSecret::BootstrapApiKey, "node-test");

        assert_eq!(
            node_api,
            credential_account(ClientSecret::NodeApiKey, "node-test")
        );
        assert_ne!(node_api, bootstrap);
        assert!(!node_api.contains("node-test"));
    }
}
