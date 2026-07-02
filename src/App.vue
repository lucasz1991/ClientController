<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";

type ClientConfig = {
  server_domain: string;
  node_uuid: string;
  node_key: string;
  api_key: string;
  bootstrap_api_key: string;
  environment: string;
  allow_server_rebind: boolean;
  last_successful_server: string;
};

type ClientStatus = {
  config: ClientConfig;
  pending_events: number;
  local_devices: number;
  adb_source: string;
  adb_available: boolean;
  db_path: string;
  config_path: string;
  node_available: boolean;
  workflow_runtime_available: boolean;
  workflow_runtime_path: string;
};

type OutboxEvent = {
  id: number;
  event_type: string;
  payload_json: string;
  created_at: string;
};

type LocalDevice = {
  id: number;
  device_uuid: string;
  name: string;
  platform: string;
  adb_serial?: string | null;
  status: string;
  last_seen_at: string;
  raw_json: string;
};

type SyncSummary = {
  registered: boolean;
  discovered_devices: number;
  synced_devices: number;
  heartbeat_sent: boolean;
  jobs_started: number;
  message: string;
};

type FeedbackType = "info" | "success" | "error";
type TabKey = "overview" | "sync" | "outbox" | "settings";

const CORRECT_SERVER_URL = "https://factory.follow-flow.de";
const AUTOPILOT_INTERVAL_MS = 30_000;

const status = ref<ClientStatus | null>(null);
const pendingEvents = ref<OutboxEvent[]>([]);
const localDevices = ref<LocalDevice[]>([]);

const feedback = ref("Bereit.");
const feedbackType = ref<FeedbackType>("info");

const busy = ref<Record<string, boolean>>({});
const isBusy = computed(() => Object.values(busy.value).some(Boolean));
const scansActive = computed(() => Boolean(autopilotActive.value || busy.value.discover || busy.value.autopilot));

const activeTab = ref<TabKey>("overview");

const newServerDomain = ref(CORRECT_SERVER_URL);
const testEventType = ref("network_test");
const testPayload = ref('{"target":"https://example.com","result":"ok"}');
const rebindNewDomain = ref("https://factory.follow-flow.de");
const rebindExpiresAt = ref("2026-06-10T18:00:00Z");
const rebindSignature = ref("followflow-default-node-key-change-me");

const autopilotActive = ref(false);
const autopilotLastRunAt = ref<string>("-");
const autopilotLastMessage = ref<string>("Noch kein Lauf");
const nextScanInSeconds = ref<number | null>(null);

let autopilotTimer: number | null = null;
let countdownTimer: number | null = null;
let nextAutopilotRunAtMs: number | null = null;

const hasLegacyServerUrl = computed(() => {
  const current = (status.value?.config.server_domain || "").toLowerCase();
  return current.includes("https://factory.followflow.de");
});

function setFeedback(message: string, type: FeedbackType = "info") {
  feedback.value = message;
  feedbackType.value = type;
}

async function runAction(key: string, action: () => Promise<void>) {
  busy.value[key] = true;
  try {
    await action();
  } finally {
    busy.value[key] = false;
  }
}

function syncCountdownValue() {
  if (!autopilotActive.value || nextAutopilotRunAtMs === null) {
    nextScanInSeconds.value = null;
    return;
  }

  const diffMs = nextAutopilotRunAtMs - Date.now();
  nextScanInSeconds.value = Math.max(0, Math.ceil(diffMs / 1000));
}

function startCountdownTicker() {
  if (countdownTimer !== null) return;
  countdownTimer = window.setInterval(syncCountdownValue, 1_000);
  syncCountdownValue();
}

function stopCountdownTicker() {
  if (countdownTimer !== null) {
    window.clearInterval(countdownTimer);
    countdownTimer = null;
  }
  nextAutopilotRunAtMs = null;
  nextScanInSeconds.value = null;
}

async function refreshStatus() {
  await runAction("refresh", async () => {
    try {
      await invoke("bootstrap_local_runtime");
      status.value = await invoke<ClientStatus>("get_client_status");
      newServerDomain.value = CORRECT_SERVER_URL;
      pendingEvents.value = await invoke<OutboxEvent[]>("get_pending_events", { limit: 20 });
      localDevices.value = await invoke<LocalDevice[]>("get_local_devices");
    } catch (err) {
      setFeedback(`Fehler beim Laden: ${String(err)}`, "error");
    }
  });
}

async function runAutopilotCycle(showFeedback = false) {
  if (busy.value.autopilot) return;

  await runAction("autopilot", async () => {
    try {
      const result = await invoke<SyncSummary>("run_autopilot_cycle");
      autopilotLastRunAt.value = new Date().toLocaleTimeString();
      autopilotLastMessage.value = result.message;
      await refreshStatus();

      if (showFeedback) {
        setFeedback(
          `Autopilot-Lauf: discovered=${result.discovered_devices}, synced=${result.synced_devices}, heartbeat=${result.heartbeat_sent}`,
          "success"
        );
      }
    } catch (err) {
      autopilotLastRunAt.value = new Date().toLocaleTimeString();
      autopilotLastMessage.value = `Fehler: ${String(err)}`;
      if (showFeedback) setFeedback(`Autopilot-Fehler: ${String(err)}`, "error");
    }
  });
}

function startAutopilot() {
  if (autopilotActive.value) return;
  autopilotActive.value = true;

  nextAutopilotRunAtMs = Date.now() + AUTOPILOT_INTERVAL_MS;
  startCountdownTicker();
  runAutopilotCycle(true);

  autopilotTimer = window.setInterval(() => {
    nextAutopilotRunAtMs = Date.now() + AUTOPILOT_INTERVAL_MS;
    syncCountdownValue();
    runAutopilotCycle(false);
  }, AUTOPILOT_INTERVAL_MS);
}

function stopAutopilot() {
  autopilotActive.value = false;
  if (autopilotTimer !== null) {
    window.clearInterval(autopilotTimer);
    autopilotTimer = null;
  }
  stopCountdownTicker();
}

async function saveServerDomain() {
  await runAction("saveDomain", async () => {
    try {
      await invoke("update_server_domain", { server_domain: newServerDomain.value.trim() });
      await refreshStatus();
      setFeedback("Server-Domain gespeichert.", "success");
    } catch (err) {
      setFeedback(`Fehler beim Speichern: ${String(err)}`, "error");
    }
  });
}

async function applyCorrectServerUrl() {
  newServerDomain.value = CORRECT_SERVER_URL;
  await saveServerDomain();
}

async function registerNodeRemote() {
  await runAction("register", async () => {
    try {
      await invoke("register_node_remote", { node_name: null });
      await refreshStatus();
      setFeedback("Node am Server registriert.", "success");
    } catch (err) {
      setFeedback(`Register-Fehler: ${String(err)}`, "error");
    }
  });
}

async function sendHeartbeatRemote() {
  await runAction("heartbeat", async () => {
    try {
      await invoke("send_heartbeat_remote", {
        status: "online",
        payload: { source: "ui", note: "manual heartbeat" },
      });
      setFeedback("Heartbeat an Server gesendet.", "success");
    } catch (err) {
      setFeedback(`Heartbeat-Fehler: ${String(err)}`, "error");
    }
  });
}

async function discoverDevices() {
  await runAction("discover", async () => {
    try {
      localDevices.value = await invoke<LocalDevice[]>("discover_android_devices");
      await refreshStatus();
      setFeedback(`${localDevices.value.length} Geräte lokal erkannt.`, "success");
    } catch (err) {
      setFeedback(`ADB-Erkennung fehlgeschlagen: ${String(err)}`, "error");
    }
  });
}

async function installWindowsDriver() {
  await runAction("installDriver", async () => {
    try {
      const result = await invoke<{ success: boolean; message: string }>("install_windows_usb_driver");
      setFeedback(`Treiber-Installationsversuch: ${result.message}`, "success");
      await discoverDevices();
    } catch (err) {
      setFeedback(`Treiber-Installation fehlgeschlagen: ${String(err)}`, "error");
    }
  });
}

async function syncDevicesRemote() {
  await runAction("syncDevices", async () => {
    try {
      await invoke("sync_devices_remote");
      setFeedback("Geräte an Laravel synchronisiert.", "success");
    } catch (err) {
      setFeedback(`Device-Sync-Fehler: ${String(err)}`, "error");
    }
  });
}

async function queueTestEvent() {
  await runAction("queueEvent", async () => {
    try {
      const payloadObj = JSON.parse(testPayload.value);
      await invoke("queue_event_local", {
        event_type: testEventType.value,
        payload: payloadObj,
      });
      await refreshStatus();
      setFeedback("Event lokal gespeichert (Outbox).", "success");
    } catch (err) {
      setFeedback(`Fehler beim Event-Queueing: ${String(err)}`, "error");
    }
  });
}

async function markAsSent(eventId: number) {
  await runAction(`mark-${eventId}`, async () => {
    try {
      await invoke("mark_event_sent", { event_id: eventId });
      await refreshStatus();
      setFeedback(`Event ${eventId} als gesendet markiert.`, "success");
    } catch (err) {
      setFeedback(`Fehler beim Markieren: ${String(err)}`, "error");
    }
  });
}

async function logHeartbeatLocal() {
  await runAction("heartbeatLocal", async () => {
    try {
      await invoke("log_heartbeat_local", {
        status: "ok",
        details: { source: "ui", note: "manual local heartbeat" },
      });
      setFeedback("Heartbeat lokal protokolliert.", "success");
    } catch (err) {
      setFeedback(`Heartbeat-Fehler: ${String(err)}`, "error");
    }
  });
}

async function applyRebind() {
  await runAction("rebind", async () => {
    try {
      await invoke("apply_rebind_request", {
        request: {
          new_server_domain: rebindNewDomain.value,
          expires_at: rebindExpiresAt.value,
          signature: rebindSignature.value,
        },
      });
      await refreshStatus();
      setFeedback("Rebind-Request verarbeitet.", "success");
    } catch (err) {
      setFeedback(`Rebind-Fehler: ${String(err)}`, "error");
    }
  });
}

onMounted(async () => {
  await refreshStatus();
  startAutopilot();
});

onUnmounted(() => {
  stopAutopilot();
});
</script>

<template>
  <main class="container">
    <header class="header">
      <div>
        <h1>FollowFlow ClientController</h1>
        <p class="subtitle">Klarere Oberfläche mit Autopilot für Registrierung, Scan, Sync und Heartbeat</p>
      </div>
      <div class="row">
        <button class="btn secondary" @click="refreshStatus" :disabled="isBusy">
          <span v-if="busy.refresh" class="spinner tiny"></span>
          Neu laden
        </button>
        <button v-if="!autopilotActive" class="btn" @click="startAutopilot">Autopilot starten</button>
        <button v-else class="btn danger" @click="stopAutopilot">Autopilot stoppen</button>
      </div>
    </header>

    <section class="autopilot-strip">
      <span><strong>Status:</strong> {{ autopilotActive ? "Aktiv" : "Inaktiv" }}</span>
      <span><strong>Letzter Lauf:</strong> {{ autopilotLastRunAt }}</span>
      <span><strong>Ergebnis:</strong> {{ autopilotLastMessage }}</span>
    </section>

    <section v-if="status" class="stats-grid">
      <article class="stat-card">
        <span>Server</span>
        <strong>{{ status.config.server_domain }}</strong>
      </article>
      <article class="stat-card">
        <span>Node UUID</span>
        <strong class="mono">{{ status.config.node_uuid }}</strong>
      </article>
      <article class="stat-card">
        <span>Pending Outbox</span>
        <strong>{{ status.pending_events }}</strong>
      </article>
      <article class="stat-card">
        <span>Lokale Geräte</span>
        <strong>{{ status.local_devices }}</strong>
      </article>
      <article class="stat-card">
        <span>ADB</span>
        <strong :class="status.adb_available ? 'ok' : 'bad'">
          {{ status.adb_available ? "Verfügbar" : "Nicht gefunden" }}
        </strong>
      </article>
      <article class="stat-card">
        <span>Node.js / Workflow-Runtime</span>
        <strong :class="status.node_available && status.workflow_runtime_available ? 'ok' : 'bad'">
          {{ status.node_available && status.workflow_runtime_available ? "Bereit" : "Nicht bereit" }}
        </strong>
      </article>
    </section>

    <section v-if="hasLegacyServerUrl" class="legacy-warning">
      <p>
        ⚠️ In der Konfiguration steht noch eine alte URL (<code>{{ status?.config.server_domain }}</code>).
      </p>
      <button class="btn danger" @click="applyCorrectServerUrl" :disabled="isBusy">
        Auf {{ CORRECT_SERVER_URL }} korrigieren
      </button>
    </section>

    <nav class="tabs">
      <button :class="['tab', { active: activeTab === 'overview' }]" @click="activeTab = 'overview'">Übersicht</button>
      <button :class="['tab', { active: activeTab === 'sync' }]" @click="activeTab = 'sync'">Sync & Geräte</button>
      <button :class="['tab', { active: activeTab === 'outbox' }]" @click="activeTab = 'outbox'">Outbox</button>
      <button :class="['tab', { active: activeTab === 'settings' }]" @click="activeTab = 'settings'">Einstellungen</button>
    </nav>

    <section v-if="activeTab === 'overview'" class="card">
      <h2>Schnellaktionen (optional)</h2>
      <div class="actions-grid">
        <button class="btn" @click="runAutopilotCycle(true)" :disabled="isBusy">
          <span v-if="busy.autopilot" class="spinner"></span>
          Autopilot-Lauf jetzt
        </button>
        <button class="btn" @click="registerNodeRemote" :disabled="isBusy">
          <span v-if="busy.register" class="spinner"></span>
          Node registrieren
        </button>
        <button class="btn" @click="sendHeartbeatRemote" :disabled="isBusy">
          <span v-if="busy.heartbeat" class="spinner"></span>
          Heartbeat senden
        </button>
        <button class="btn secondary" @click="logHeartbeatLocal" :disabled="isBusy">
          <span v-if="busy.heartbeatLocal" class="spinner"></span>
          Heartbeat lokal loggen
        </button>
      </div>

      <div v-if="status" class="meta-list">
        <p><strong>Last successful:</strong> {{ status.config.last_successful_server }}</p>
        <p><strong>Environment:</strong> {{ status.config.environment }}</p>
        <p><strong>Rebind erlaubt:</strong> {{ status.config.allow_server_rebind ? "Ja" : "Nein" }}</p>
        <p><strong>API-Key vorhanden:</strong> {{ status.config.api_key ? "Ja" : "Nein" }}</p>
        <p><strong>ADB Quelle:</strong> {{ status.adb_source }}</p>
        <p><strong>Config Pfad:</strong> {{ status.config_path }}</p>
      </div>
    </section>

    <section v-if="activeTab === 'sync'" class="card">
      <h2>Geräte & Server-Sync</h2>
      <div class="actions-grid">
        <button class="btn subtle" @click="discoverDevices" :disabled="isBusy">
          <span v-if="busy.discover" class="spinner"></span>
          Profile erfassen (ADB-Scan)
        </button>
        <button class="btn" @click="syncDevicesRemote" :disabled="isBusy">
          <span v-if="busy.syncDevices" class="spinner"></span>
          Geräte zu Server syncen
        </button>
        <button class="btn secondary" @click="installWindowsDriver" :disabled="isBusy">
          <span v-if="busy.installDriver" class="spinner"></span>
          Windows ADB-Treiber installieren
        </button>
      </div>

      <ul v-if="localDevices.length > 0" class="device-list">
        <li v-for="d in localDevices" :key="d.device_uuid" class="device-item">
          <div class="device-icon-wrap">
            <span class="device-icon">📱</span>
            <span v-if="scansActive && nextScanInSeconds !== null" class="countdown-badge">{{ nextScanInSeconds }}s</span>
          </div>

          <div class="device-content">
            <div class="device-title-row">
              <strong>{{ d.name || "Unbenanntes Gerät" }}</strong>
              <span class="status-pill">{{ d.status }}</span>
            </div>
            <div class="device-meta">
              <span><strong>UUID:</strong> <code>{{ d.device_uuid }}</code></span>
              <span><strong>ADB:</strong> {{ d.adb_serial || "-" }}</span>
              <span><strong>Platform:</strong> {{ d.platform || "-" }}</span>
              <span><strong>Last Seen:</strong> {{ d.last_seen_at }}</span>
            </div>
          </div>
        </li>
      </ul>
      <p v-else>Keine lokalen Geräte erkannt.</p>
    </section>

    <section v-if="activeTab === 'outbox'" class="card">
      <h2>Lokale Outbox (Store-and-Forward)</h2>
      <div class="column">
        <input v-model="testEventType" placeholder="event type" />
        <textarea v-model="testPayload" rows="4"></textarea>
        <button class="btn" @click="queueTestEvent" :disabled="isBusy">
          <span v-if="busy.queueEvent" class="spinner"></span>
          Test-Event lokal speichern
        </button>
      </div>

      <table v-if="pendingEvents.length > 0" class="events-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Typ</th>
            <th>Payload</th>
            <th>Zeit</th>
            <th>Aktion</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="evt in pendingEvents" :key="evt.id">
            <td>{{ evt.id }}</td>
            <td>{{ evt.event_type }}</td>
            <td><code>{{ evt.payload_json }}</code></td>
            <td>{{ evt.created_at }}</td>
            <td>
              <button class="btn secondary" @click="markAsSent(evt.id)" :disabled="isBusy">
                <span v-if="busy[`mark-${evt.id}`]" class="spinner tiny"></span>
                Als gesendet markieren
              </button>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-else>Keine pending Events.</p>
    </section>

    <section v-if="activeTab === 'settings'" class="card">
      <h2>Server-Bindung & Rebind</h2>

      <div class="column block">
        <label>Server-Domain</label>
        <input v-model="newServerDomain" placeholder="https://factory.follow-flow.de" />
        <button class="btn" @click="saveServerDomain" :disabled="isBusy">
          <span v-if="busy.saveDomain" class="spinner"></span>
          Domain speichern
        </button>
      </div>

      <div class="column block">
        <label>Rebind (MVP-Test)</label>
        <input v-model="rebindNewDomain" placeholder="https://factory.follow-flow.de" />
        <input v-model="rebindExpiresAt" placeholder="2026-06-10T18:00:00Z" />
        <input v-model="rebindSignature" placeholder="signature" />
        <button class="btn secondary" @click="applyRebind" :disabled="isBusy">
          <span v-if="busy.rebind" class="spinner"></span>
          Rebind anwenden
        </button>
      </div>
    </section>

    <p :class="['feedback', feedbackType]">{{ feedback }}</p>

    <div v-if="isBusy" class="loading-overlay">
      <div class="loading-box">
        <span class="spinner large"></span>
        <p>Bitte warten…</p>
      </div>
    </div>
  </main>
</template>

<style scoped>
.container {
  max-width: 1080px;
  margin: 0 auto;
  padding: 1.4rem;
  font-family: Inter, Avenir, Helvetica, Arial, sans-serif;
  color: #20222a;
}

.header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 1rem;
  background: #ffffff;
  border: 1px solid #e7ecf6;
  border-radius: 12px;
  padding: 0.95rem;
}

.row {
  display: flex;
  gap: 0.5rem;
}

h1 {
  margin: 0;
}

.subtitle {
  margin: 0.2rem 0 0;
  color: #5f6372;
}

.autopilot-strip {
  margin: 0.85rem 0;
  border: 1px solid #dce5ff;
  background: #f7f9ff;
  border-radius: 10px;
  padding: 0.65rem 0.8rem;
  display: flex;
  flex-wrap: wrap;
  gap: 1rem;
  font-size: 0.92rem;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 0.75rem;
  margin: 1rem 0;
}

.stat-card {
  background: #fff;
  border: 1px solid #e5e9f3;
  border-radius: 10px;
  padding: 0.75rem;
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
}

.stat-card span {
  color: #6a7082;
  font-size: 0.82rem;
}

.stat-card strong {
  font-size: 0.95rem;
}

.ok {
  color: #0b8f4f;
}

.bad {
  color: #b42318;
}

.legacy-warning {
  margin: 0.5rem 0 1rem;
  border: 1px solid #fedf89;
  background: #fffaeb;
  color: #7a2e0e;
  border-radius: 10px;
  padding: 0.75rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 0.75rem;
}

.tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin-bottom: 0.8rem;
}

.tab {
  border-radius: 999px;
  border: 1px solid #d6dcef;
  background: #f6f8ff;
  color: #394057;
  padding: 0.45rem 0.9rem;
  cursor: pointer;
}

.tab.active {
  background: #355ad4;
  border-color: #355ad4;
  color: #fff;
}

.card {
  border: 1px solid #dfe5f2;
  border-radius: 12px;
  padding: 1rem;
  background: #fff;
  margin-bottom: 1rem;
}

.actions-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 0.55rem;
  margin-bottom: 0.85rem;
}

.meta-list p {
  margin: 0.25rem 0;
  font-size: 0.92rem;
}

.column {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.block {
  margin-bottom: 1rem;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}

input,
textarea,
button {
  border-radius: 10px;
  border: 1px solid #cfd6e6;
  padding: 0.58rem 0.8rem;
  font-size: 0.94rem;
}

textarea {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}

.btn {
  cursor: pointer;
  background: #2f55d4;
  border-color: #2f55d4;
  color: #fff;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 0.45rem;
}

.btn.secondary {
  background: #eef2ff;
  border-color: #cdd8ff;
  color: #243b84;
}

.btn.subtle {
  background: #f3f4f8;
  border-color: #dde2ec;
  color: #2f3b59;
}

.btn.danger {
  background: #e5484d;
  border-color: #e5484d;
  color: #fff;
}

button:disabled {
  opacity: 0.65;
  cursor: not-allowed;
}

.device-list {
  list-style: none;
  margin: 0.7rem 0 0;
  padding: 0;
  display: grid;
  gap: 0.65rem;
}

.device-item {
  border: 1px solid #e8edf7;
  border-radius: 12px;
  background: #fbfcff;
  padding: 0.7rem;
  display: flex;
  gap: 0.75rem;
}

.device-icon-wrap {
  position: relative;
  width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  background: #eef2ff;
  border: 1px solid #d6def8;
  border-radius: 10px;
  flex-shrink: 0;
}

.device-icon {
  font-size: 1.1rem;
  line-height: 1;
}

.countdown-badge {
  position: absolute;
  right: -8px;
  top: -8px;
  background: #2f55d4;
  color: #fff;
  border-radius: 999px;
  padding: 0.08rem 0.35rem;
  font-size: 0.66rem;
  font-weight: 700;
  border: 1px solid #fff;
}

.device-content {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}

.device-title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.6rem;
}

.status-pill {
  border: 1px solid #d5dcf3;
  background: #f3f6ff;
  color: #304a92;
  border-radius: 999px;
  padding: 0.12rem 0.5rem;
  font-size: 0.75rem;
  white-space: nowrap;
}

.device-meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 0.35rem 0.8rem;
  font-size: 0.87rem;
  color: #4a5062;
}

.events-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 0.7rem;
}

.events-table th,
.events-table td {
  border: 1px solid #e9edf6;
  padding: 0.5rem;
  vertical-align: top;
  text-align: left;
  font-size: 0.9rem;
}

.feedback {
  margin-top: 0.2rem;
  font-weight: 600;
  border-radius: 8px;
  padding: 0.55rem 0.7rem;
}

.feedback.info {
  background: #eef4ff;
  color: #274690;
}

.feedback.success {
  background: #ecfdf3;
  color: #067647;
}

.feedback.error {
  background: #fef3f2;
  color: #b42318;
}

.loading-overlay {
  position: fixed;
  inset: 0;
  background: rgba(20, 24, 34, 0.22);
  display: grid;
  place-items: center;
  z-index: 50;
}

.loading-box {
  background: #fff;
  border: 1px solid #dfe5f2;
  border-radius: 12px;
  padding: 1rem 1.2rem;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.6rem;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.08);
}

.spinner {
  width: 14px;
  height: 14px;
  border: 2px solid rgba(255, 255, 255, 0.45);
  border-top-color: #fff;
  border-radius: 50%;
  display: inline-block;
  animation: spin 0.8s linear infinite;
}

.spinner.tiny {
  width: 12px;
  height: 12px;
}

.spinner.large {
  width: 24px;
  height: 24px;
  border-width: 3px;
  border-color: #d0d9f6;
  border-top-color: #2f55d4;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>


