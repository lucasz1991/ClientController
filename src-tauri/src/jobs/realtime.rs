use reqwest::{
    blocking::Client,
    header::{HeaderValue, CONTENT_TYPE},
    redirect::Policy,
    Url,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::ErrorKind;
use std::net::TcpStream;
use std::time::{Duration, Instant};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{client::connect_with_config, Error as WebSocketError, Message, WebSocket};

const CONFIG_PATH: &str = "/api/client-controller/realtime/config";
const AUTH_PATH: &str = "/api/client-controller/realtime/auth";
const NODE_API_KEY_HEADER: &str = "X-NODE-API-KEY";
const JOB_POLL_EVENT: &str = "client-controller.job-poll";
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SILENCE: Duration = Duration::from_secs(65);

pub(crate) struct RealtimeCredentials {
    server_domain: String,
    node_uuid: String,
    api_key: String,
}

impl RealtimeCredentials {
    pub(crate) fn new(server_domain: String, node_uuid: String, api_key: String) -> Self {
        Self {
            server_domain,
            node_uuid,
            api_key,
        }
    }
}

pub(crate) enum SessionOutcome {
    Disabled,
    Disconnected { established: bool },
}

#[derive(Deserialize)]
struct RealtimeConfigResponse {
    enabled: bool,
    #[serde(default)]
    websocket_url: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    event: String,
}

struct ActiveRealtimeConfig {
    websocket_url: Url,
    channel: String,
}

#[derive(Deserialize)]
struct RealtimeAuthResponse {
    auth: String,
}

#[derive(Deserialize)]
struct PusherEnvelope {
    event: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    data: Value,
}

#[derive(Deserialize)]
struct ConnectionEstablishedData {
    socket_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobPollPayload {
    node_uuid: String,
    #[serde(rename = "signaled_at")]
    _signaled_at: String,
    #[serde(rename = "reason")]
    _reason: String,
}

#[derive(Debug, Eq, PartialEq)]
enum JobSignalDecision {
    Ignore,
    Signal,
}

fn secure_http_client() -> Result<Client, String> {
    Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(Policy::none())
        .build()
        .map_err(|_| "realtime HTTP client initialization failed".to_string())
}

fn parse_server_origin(server_domain: &str) -> Result<Url, String> {
    let parsed = Url::parse(server_domain)
        .map_err(|_| "realtime server domain is not a valid URL".to_string())?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "realtime server domain has no host".to_string())?;
    let debug_loopback_http =
        cfg!(debug_assertions) && parsed.scheme() == "http" && is_loopback_host(host);

    if parsed.scheme() != "https" && !debug_loopback_http {
        return Err("realtime server domain must use HTTPS".to_string());
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("realtime server domain must not contain credentials".to_string());
    }

    if parsed.path() != "/" || parsed.query().is_some() || parsed.fragment().is_some() {
        return Err("realtime server domain must be an origin".to_string());
    }

    Ok(parsed)
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn validate_channel(channel: &str) -> Result<(), String> {
    let valid = channel.starts_with("private-")
        && channel.len() > "private-".len()
        && channel.len() <= 200
        && channel.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '_' | '-' | '=' | '@' | ',' | '.' | ';')
        });

    if valid {
        Ok(())
    } else {
        Err("realtime channel is not a valid private Pusher channel".to_string())
    }
}

fn validate_websocket_url(
    raw_url: &str,
    server_origin: &Url,
    api_key: &str,
) -> Result<Url, String> {
    let parsed =
        Url::parse(raw_url).map_err(|_| "realtime websocket_url is not a valid URL".to_string())?;
    let websocket_host = parsed
        .host_str()
        .ok_or_else(|| "realtime websocket_url has no host".to_string())?;
    let server_host = server_origin
        .host_str()
        .ok_or_else(|| "realtime server domain has no host".to_string())?;
    let debug_loopback_ws = cfg!(debug_assertions)
        && parsed.scheme() == "ws"
        && is_loopback_host(websocket_host)
        && websocket_host.eq_ignore_ascii_case(server_host);

    if parsed.scheme() != "wss" && !debug_loopback_ws {
        return Err("realtime websocket_url must use WSS".to_string());
    }

    if !websocket_host.eq_ignore_ascii_case(server_host) {
        return Err("realtime websocket host must match the configured server host".to_string());
    }

    if !parsed.username().is_empty() || parsed.password().is_some() || parsed.fragment().is_some() {
        return Err("realtime websocket_url contains forbidden URL components".to_string());
    }

    let app_key = parsed
        .path()
        .strip_prefix("/app/")
        .filter(|value| !value.is_empty() && !value.contains('/'))
        .ok_or_else(|| "realtime websocket_url must contain /app/{key}".to_string())?;
    if !app_key
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err("realtime websocket app key contains invalid characters".to_string());
    }

    let mut protocol_7 = false;
    for (name, value) in parsed.query_pairs() {
        let normalized_name = name.to_ascii_lowercase();
        if name == "protocol" && value == "7" {
            protocol_7 = true;
        }
        if normalized_name.contains("api_key")
            || normalized_name.contains("token")
            || normalized_name.contains("secret")
            || normalized_name.contains("authorization")
            || (!api_key.is_empty() && value == api_key)
        {
            return Err("realtime websocket_url contains a forbidden secret parameter".to_string());
        }
    }

    if !protocol_7 {
        return Err("realtime websocket_url must request Pusher protocol 7".to_string());
    }

    if !api_key.is_empty() && raw_url.contains(api_key) {
        return Err("realtime websocket_url must not contain the node API key".to_string());
    }

    Ok(parsed)
}

fn fetch_realtime_config(
    client: &Client,
    credentials: &RealtimeCredentials,
) -> Result<Option<ActiveRealtimeConfig>, String> {
    let server_origin = parse_server_origin(&credentials.server_domain)?;
    let endpoint = server_origin
        .join(CONFIG_PATH)
        .map_err(|_| "realtime config endpoint could not be constructed".to_string())?;
    let mut api_key = HeaderValue::from_str(&credentials.api_key)
        .map_err(|_| "node API key is not a valid HTTP header value".to_string())?;
    api_key.set_sensitive(true);
    let response = client
        .get(endpoint)
        .header(NODE_API_KEY_HEADER, api_key)
        .send()
        .map_err(|_| "realtime config request failed".to_string())?;

    if !response.status().is_success() {
        return Err(format!(
            "realtime config request failed with HTTP {}",
            response.status().as_u16()
        ));
    }

    let config: RealtimeConfigResponse = response
        .json()
        .map_err(|_| "realtime config response is invalid".to_string())?;
    if !config.enabled {
        return Ok(None);
    }

    if config.event != JOB_POLL_EVENT {
        return Err("realtime config returned an unsupported event".to_string());
    }
    validate_channel(&config.channel)?;
    let websocket_url =
        validate_websocket_url(&config.websocket_url, &server_origin, &credentials.api_key)?;

    Ok(Some(ActiveRealtimeConfig {
        websocket_url,
        channel: config.channel,
    }))
}

fn request_channel_auth(
    client: &Client,
    credentials: &RealtimeCredentials,
    socket_id: &str,
    channel: &str,
) -> Result<String, String> {
    if socket_id.is_empty()
        || socket_id.len() > 100
        || !socket_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '-'))
    {
        return Err("Pusher returned an invalid socket_id".to_string());
    }

    let server_origin = parse_server_origin(&credentials.server_domain)?;
    let endpoint = server_origin
        .join(AUTH_PATH)
        .map_err(|_| "realtime auth endpoint could not be constructed".to_string())?;
    let mut api_key = HeaderValue::from_str(&credentials.api_key)
        .map_err(|_| "node API key is not a valid HTTP header value".to_string())?;
    api_key.set_sensitive(true);
    let response = client
        .post(endpoint)
        .header(NODE_API_KEY_HEADER, api_key)
        .header(CONTENT_TYPE, "application/json")
        .json(&json!({
            "socket_id": socket_id,
            "channel": channel,
        }))
        .send()
        .map_err(|_| "realtime auth request failed".to_string())?;

    if !response.status().is_success() {
        return Err(format!(
            "realtime auth request failed with HTTP {}",
            response.status().as_u16()
        ));
    }

    let auth: RealtimeAuthResponse = response
        .json()
        .map_err(|_| "realtime auth response is invalid".to_string())?;
    if auth.auth.trim().is_empty() || auth.auth.len() > 4096 {
        return Err("realtime auth response did not contain a valid auth token".to_string());
    }

    Ok(auth.auth)
}

fn decode_data<T: for<'de> Deserialize<'de>>(data: &Value) -> Result<T, String> {
    match data {
        Value::String(serialized) => serde_json::from_str(serialized),
        value => serde_json::from_value(value.clone()),
    }
    .map_err(|_| "Pusher event data is invalid".to_string())
}

fn parse_envelope(raw: &str) -> Result<PusherEnvelope, String> {
    serde_json::from_str(raw).map_err(|_| "Pusher message is invalid".to_string())
}

fn job_signal_decision(
    envelope: &PusherEnvelope,
    expected_channel: &str,
    expected_node_uuid: &str,
) -> Result<JobSignalDecision, String> {
    if envelope.event != JOB_POLL_EVENT {
        return Ok(JobSignalDecision::Ignore);
    }

    if envelope.channel.as_deref() != Some(expected_channel) {
        return Ok(JobSignalDecision::Ignore);
    }

    let payload: JobPollPayload = decode_data(&envelope.data)?;
    if payload.node_uuid != expected_node_uuid {
        return Ok(JobSignalDecision::Ignore);
    }

    Ok(JobSignalDecision::Signal)
}

fn set_socket_timeouts(websocket: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> Result<(), String> {
    let stream = match websocket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream,
        MaybeTlsStream::Rustls(stream) => stream.get_mut(),
        _ => return Err("unsupported WebSocket TLS transport".to_string()),
    };
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|_| "could not configure WebSocket read timeout".to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|_| "could not configure WebSocket write timeout".to_string())?;
    stream
        .set_nodelay(true)
        .map_err(|_| "could not configure WebSocket transport".to_string())?;
    Ok(())
}

fn send_json(
    websocket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    value: Value,
) -> Result<(), String> {
    let serialized = serde_json::to_string(&value)
        .map_err(|_| "Pusher message serialization failed".to_string())?;
    websocket
        .send(Message::Text(serialized.into()))
        .map_err(|_| "Pusher message send failed".to_string())
}

fn is_timeout(error: &WebSocketError) -> bool {
    matches!(
        error,
        WebSocketError::Io(io_error)
            if matches!(io_error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock)
    )
}

pub(crate) fn listen_once<F>(
    credentials: &RealtimeCredentials,
    mut signal: F,
) -> Result<SessionOutcome, String>
where
    F: FnMut() -> Result<(), String>,
{
    if credentials.api_key.trim().is_empty() || credentials.node_uuid.trim().is_empty() {
        return Err("realtime listener requires registered node credentials".to_string());
    }

    let client = secure_http_client()?;
    let Some(config) = fetch_realtime_config(&client, credentials)? else {
        return Ok(SessionOutcome::Disabled);
    };

    let (mut websocket, _) = connect_with_config(config.websocket_url.as_str(), None, 0)
        .map_err(|_| "realtime WebSocket connection failed".to_string())?;
    set_socket_timeouts(&mut websocket)?;

    let mut subscribed = false;
    let mut last_activity = Instant::now();

    loop {
        let message = match websocket.read() {
            Ok(message) => message,
            Err(error) if is_timeout(&error) => {
                if last_activity.elapsed() >= MAX_SILENCE {
                    return Ok(SessionOutcome::Disconnected {
                        established: subscribed,
                    });
                }
                if websocket.send(Message::Ping(Vec::new().into())).is_err() {
                    return Ok(SessionOutcome::Disconnected {
                        established: subscribed,
                    });
                }
                continue;
            }
            Err(_) => {
                return Ok(SessionOutcome::Disconnected {
                    established: subscribed,
                });
            }
        };
        last_activity = Instant::now();

        let raw = match message {
            Message::Text(text) => text.to_string(),
            Message::Ping(payload) => {
                if websocket.send(Message::Pong(payload)).is_err() {
                    return Ok(SessionOutcome::Disconnected {
                        established: subscribed,
                    });
                }
                continue;
            }
            Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => continue,
            Message::Close(_) => {
                return Ok(SessionOutcome::Disconnected {
                    established: subscribed,
                });
            }
        };

        let envelope = match parse_envelope(&raw) {
            Ok(envelope) => envelope,
            Err(_) => continue,
        };

        match envelope.event.as_str() {
            "pusher:connection_established" => {
                let connection: ConnectionEstablishedData = decode_data(&envelope.data)?;
                let auth = request_channel_auth(
                    &client,
                    credentials,
                    &connection.socket_id,
                    &config.channel,
                )?;
                if send_json(
                    &mut websocket,
                    json!({
                        "event": "pusher:subscribe",
                        "data": {
                            "auth": auth,
                            "channel": config.channel,
                        }
                    }),
                )
                .is_err()
                {
                    return Ok(SessionOutcome::Disconnected {
                        established: subscribed,
                    });
                }
            }
            "pusher_internal:subscription_succeeded"
                if envelope.channel.as_deref() == Some(config.channel.as_str()) =>
            {
                subscribed = true;
            }
            "pusher:ping" => {
                if send_json(
                    &mut websocket,
                    json!({ "event": "pusher:pong", "data": {} }),
                )
                .is_err()
                {
                    return Ok(SessionOutcome::Disconnected {
                        established: subscribed,
                    });
                }
            }
            "pusher:error" => {
                return Ok(SessionOutcome::Disconnected {
                    established: subscribed,
                });
            }
            JOB_POLL_EVENT if subscribed => {
                if job_signal_decision(&envelope, &config.channel, &credentials.node_uuid)?
                    == JobSignalDecision::Signal
                    && signal().is_err()
                {
                    return Ok(SessionOutcome::Disconnected { established: true });
                }
            }
            _ => {}
        }
    }
}

pub(crate) fn reconnect_delay(attempt: u32, jitter_seed: u64) -> Duration {
    let base_seconds = if attempt >= 5 { 24 } else { 1_u64 << attempt };
    let base_millis = base_seconds * 1_000;
    let jitter_window = (base_millis / 4).max(1);
    let jitter_millis = jitter_seed % jitter_window;

    Duration::from_millis((base_millis + jitter_millis).min(30_000))
}

#[cfg(test)]
mod tests {
    use super::{
        job_signal_decision, parse_envelope, reconnect_delay, validate_websocket_url,
        JobSignalDecision,
    };
    use reqwest::Url;
    use std::time::Duration;

    #[test]
    fn parses_protocol_string_payload() {
        let envelope = parse_envelope(
            r#"{"event":"client-controller.job-poll","channel":"private-node-1","data":"{\"node_uuid\":\"node-1\",\"signaled_at\":\"2026-08-22T12:00:00Z\",\"reason\":\"queued\"}"}"#,
        )
        .expect("Pusher envelope should parse");

        assert_eq!(
            job_signal_decision(&envelope, "private-node-1", "node-1")
                .expect("payload should parse"),
            JobSignalDecision::Signal
        );
    }

    #[test]
    fn parses_protocol_object_payload() {
        let envelope = parse_envelope(
            r#"{"event":"client-controller.job-poll","channel":"private-node-1","data":{"node_uuid":"node-1","signaled_at":"2026-08-22T12:00:00Z","reason":"queued"}}"#,
        )
        .expect("Pusher envelope should parse");

        assert_eq!(
            job_signal_decision(&envelope, "private-node-1", "node-1")
                .expect("payload should parse"),
            JobSignalDecision::Signal
        );
    }

    #[test]
    fn ignores_node_mismatch() {
        let envelope = parse_envelope(
            r#"{"event":"client-controller.job-poll","channel":"private-node-1","data":{"node_uuid":"another-node","signaled_at":"2026-08-22T12:00:00Z","reason":"queued"}}"#,
        )
        .expect("Pusher envelope should parse");

        assert_eq!(
            job_signal_decision(&envelope, "private-node-1", "node-1")
                .expect("payload should parse"),
            JobSignalDecision::Ignore
        );
    }

    #[test]
    fn reconnect_backoff_is_bounded_and_jittered() {
        assert!(reconnect_delay(0, 249) > Duration::from_secs(1));
        assert!(reconnect_delay(1, 499) >= Duration::from_secs(2));
        assert!(reconnect_delay(4, 3_999) < Duration::from_secs(20));
        assert!(reconnect_delay(5, 5_999) < Duration::from_secs(30));
        assert!(reconnect_delay(99, u64::MAX) <= Duration::from_secs(30));
    }

    #[test]
    fn websocket_url_requires_same_host_and_never_accepts_api_key() {
        let server = Url::parse("https://factory.follow-flow.de").expect("server URL");
        let valid = validate_websocket_url(
            "wss://factory.follow-flow.de/app/public-app-key?protocol=7&client=rust",
            &server,
            "node-secret",
        );
        assert!(valid.is_ok());

        assert!(validate_websocket_url(
            "wss://foreign.example/app/public-app-key?protocol=7",
            &server,
            "node-secret",
        )
        .is_err());
        assert!(validate_websocket_url(
            "wss://factory.follow-flow.de/app/node-secret?protocol=7",
            &server,
            "node-secret",
        )
        .is_err());
    }
}
