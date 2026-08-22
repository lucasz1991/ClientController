# Tauri + Vue + TypeScript

This template should help get you started developing with Vue 3 and TypeScript in Vite. The template uses Vue 3 `<script setup>` SFCs, check out the [script setup docs](https://v3.vuejs.org/api/sfc-script-setup.html#sfc-script-setup) to learn more.

## Secure node enrollment

The desktop client ships without a bootstrap key. An administrator must enter a one-time,
server-issued enrollment credential under **Einstellungen > Sicheres Enrollment** before the
node can register. The credential and the resulting node API key are stored in Windows
Credential Manager, never in `client.json`. The enrollment credential is deleted after a
successful exchange.

Existing installations are migrated on first start: legacy secrets are copied to Windows
Credential Manager and then removed from `client.json`. If secure storage is unavailable, the
migration fails closed and leaves the legacy file untouched so the credentials are not lost.

Changing the server domain removes all credentials and requires a new one-time enrollment.
Production domains must use HTTPS; debug builds additionally accept loopback HTTP origins.

All authenticated API requests send the node key only in `X-NODE-API-KEY`. The server must
accept header-only authentication and issue single-use enrollment credentials with sufficient
entropy and a short expiry.

## Real-time job notification

The native client requests `GET /api/client-controller/realtime/config` with
`X-NODE-API-KEY`. When enabled, the response must contain a complete Pusher protocol 7
`websocket_url`, a ready-to-use `private-...` channel, and the exact event
`client-controller.job-poll`. The WebSocket must use WSS and the same host as the configured
server. Debug builds additionally permit `ws://` on the configured loopback host.

After Pusher supplies its `socket_id`, the client requests
`POST /api/client-controller/realtime/auth` with the same header and the JSON fields
`socket_id` and `channel`. The response must contain `auth`. The node API key is never added to
the WebSocket URL or subscription payload, redirects are rejected, and neither credential is
exposed to the Vue renderer.

An accepted `client-controller.job-poll` payload contains exactly `node_uuid`, `signaled_at`,
and `reason`. The client checks both the configured private channel and its own `node_uuid`
before signalling the native job-poll doorbell. Reconnects use bounded exponential backoff with
jitter. Configuration, authentication, protocol, or transport failures remain fail-closed; the
unchanged 30-second watchdog continues polling as the fallback.

## Signed updates

Updates are built by `.github/workflows/release.yml`. Configure the repository secrets
`TAURI_SIGNING_PRIVATE_KEY` and `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`, then keep the matching
public key in the AiUserFactory ClientController update settings.

For a release, update the version consistently in `package.json`, `src-tauri/Cargo.toml` and
`src-tauri/tauri.conf.json`, commit it, and push a matching tag such as `v0.2.0`. GitHub Actions
publishes the signed Windows installer and `latest.json`; nodes install it only after an explicit
update job from AiUserFactory.

## Befristete Cargo-Audit-Wartungsausnahme

Stand **2026-08-22** meldet `cargo audit` keine bekannte Vulnerability, aber **18 nicht
unterdrueckte Warnungen** im transitiven Tauri-Abhaengigkeitsbaum. Diese Ausnahme gilt nur bis
einschliesslich **2026-09-22**. Sie ist keine `cargo-audit`-Ignore-Liste: Die Advisory-IDs bleiben
sichtbar, und eine echte Vulnerability muss den Audit weiterhin fehlschlagen lassen.

| Transitive Ursache | Warnungen | Bewertung und Upgrade-Trigger |
| --- | --- | --- |
| `tauri 2.11.2` / `tauri-runtime-wry 2.11.2` / `wry 0.55.1` ziehen fuer Linux die nicht mehr gepflegten GTK3-Bindings ein. | `RUSTSEC-2024-0411` bis `RUSTSEC-2024-0420`, zusaetzlich `proc-macro-error` (`RUSTSEC-2024-0370`) und die `glib 0.18.5`-Unsoundness (`RUSTSEC-2024-0429`). | Der aktuelle Release-Workflow baut Windows; die GTK3-Kette wird dort nicht kompiliert. Vor jeder Linux-Auslieferung ist die Ausnahme ungueltig. Sofort pruefen, sobald Tauri/Wry auf eine gepflegte Linux-Backend-/gtk-rs-Kette wechseln oder RustSec eine ausnutzbare Vulnerability fuer diesen Pfad veroeffentlicht. |
| `tauri-utils 2.9.2 -> urlpattern 0.3.0` zieht die nicht mehr gepflegte `unic 0.9`-Kette ein. | `RUSTSEC-2025-0075`, `RUSTSEC-2025-0080`, `RUSTSEC-2025-0081`, `RUSTSEC-2025-0098`, `RUSTSEC-2025-0100`. | Nach jedem Tauri-/`urlpattern`-Update erneut `cargo tree --locked --target all` und `cargo audit` ausfuehren; Ausnahme entfernen, sobald der Pfad auf gepflegte Unicode-Crates migriert. |
| Tauri, `tauri-build` und `tauri-utils` verwenden transitiv `anyhow 1.0.102`. | Unsoundness `RUSTSEC-2026-0190` in `Error::downcast_mut()`. | Der Client ruft diese API nicht direkt auf; transitive Nichterreichbarkeit wird jedoch nicht als bewiesen angenommen. Sobald RustSec eine korrigierte Version nennt und Tauri sie zulaesst, Lockfile aktualisieren und alle Rust-Gates ausfuehren. Eine Hochstufung oder neue Reachability-Evidenz beendet die Ausnahme sofort. |

Spaetestens am **2026-09-22** werden Tauri, Wry, `urlpattern`, `anyhow` und die Advisory-Datenbank
erneut geprueft. Verlaengerung erfordert eine neue datierte Bewertung mit aktuellen
Abhaengigkeitspfaden; stilles Weiterlaufen oder pauschales Ignorieren der IDs ist nicht erlaubt.

## Recommended IDE Setup

- [VS Code](https://code.visualstudio.com/) + [Vue - Official](https://marketplace.visualstudio.com/items?itemName=Vue.volar) + [Tauri](https://marketplace.visualstudio.com/items?itemName=tauri-apps.tauri-vscode) + [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)
