<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";

type PageKey = "overview" | "devices" | "processes" | "outbox" | "settings";
type FeedbackType = "info" | "success" | "error";

type ClientConfig = {
  server_domain: string;
  node_uuid: string;
  api_key: string;
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
  app_version: string;
  running_processes: number;
  updater_available: boolean;
};

type LocalDevice = {
  id: number;
  device_uuid: string;
  name: string;
  platform: string;
  adb_serial?: string | null;
  status: string;
  last_seen_at: string;
};

type OutboxEvent = {
  id: number;
  event_type: string;
  payload_json: string;
  created_at: string;
};

type LocalProcess = {
  id: number;
  job_id?: string | null;
  job_type: string;
  status: string;
  details_json: string;
  created_at: string;
};

type WorkflowProcessPreview = {
  job_uuid: string;
  status: Record<string, unknown>;
  result: Record<string, unknown>;
  checkpoint: Record<string, unknown>;
  workflow_steps: Array<Record<string, unknown>>;
  screenshot_data_url?: string | null;
  stdout_tail: string;
  stderr_tail: string;
  run_directory: string;
};

type WorkflowRoutePreview = {
  outcome: string;
  target: string;
  label: string;
};

type WorkflowTaskPreview = {
  key: string;
  title: string;
  status: string;
};

type WorkflowTimelineItem = {
  key: string;
  index: number;
  name: string;
  actionKey: string;
  type: string;
  status: string;
  message: string;
  tasks: WorkflowTaskPreview[];
  routes: WorkflowRoutePreview[];
};

type SyncSummary = {
  registered: boolean;
  discovered_devices: number;
  synced_devices: number;
  heartbeat_sent: boolean;
  jobs_started: number;
  message: string;
};

const pages: Array<{ key: PageKey; label: string; icon: string }> = [
  { key: "overview", label: "Übersicht", icon: "⌂" },
  { key: "devices", label: "Geräte", icon: "▣" },
  { key: "processes", label: "Prozesse", icon: "◫" },
  { key: "outbox", label: "Outbox", icon: "⇄" },
  { key: "settings", label: "Einstellungen", icon: "⚙" },
];

const activePage = ref<PageKey>("overview");
const status = ref<ClientStatus | null>(null);
const devices = ref<LocalDevice[]>([]);
const processes = ref<LocalProcess[]>([]);
const events = ref<OutboxEvent[]>([]);
const workflowPreview = ref<WorkflowProcessPreview | null>(null);
const workflowPreviewJob = ref<string | null>(null);
const serverDomain = ref("https://factory.follow-flow.de");
const eventType = ref("network_test");
const eventPayload = ref('{"source":"client-ui"}');
const busy = ref<string | null>(null);
const feedback = ref("Node-Dienst wird initialisiert …");
const feedbackType = ref<FeedbackType>("info");
const lastCycleAt = ref("–");
const lastCycleSummary = ref("Noch kein manueller Lauf");
let refreshTimer: number | null = null;

const isBusy = computed(() => busy.value !== null);
const online = computed(() => Boolean(status.value?.config.api_key));
const runtimeReady = computed(() => Boolean(status.value?.node_available && status.value?.workflow_runtime_available));
const workflowStepTimeline = computed<WorkflowTimelineItem[]>(() => {
  const preview = workflowPreview.value;
  if (!preview) return [];

  const definitions = asRecordArray(preview.workflow_steps).length
    ? asRecordArray(preview.workflow_steps)
    : asRecordArray(preview.status.workflowSteps);
  const snapshots = [
    ...asRecordArray(preview.checkpoint.steps),
    ...asRecordArray(preview.status.steps),
    ...asRecordArray(preview.result.steps),
  ];
  const snapshotsByStep = new Map<string, Record<string, unknown>>();
  const snapshotsByAction = new Map<string, Record<string, unknown>>();

  for (const snapshot of snapshots) {
    const stepId = workflowStepId(snapshot);
    const actionKey = workflowActionKey(snapshot);
    if (stepId) snapshotsByStep.set(stepId, snapshot);
    if (actionKey) snapshotsByAction.set(actionKey, snapshot);
  }

  const source = definitions.length ? definitions : uniqueWorkflowSnapshots(snapshots);
  const currentStepId = stringValue(preview.status, ["currentStepId", "workflowStepId"]);
  const nextActionKey = stringValue(preview.checkpoint, ["nextActionKey"]);

  return source.map((step, index) => {
    const actionKey = workflowActionKey(step);
    const stepId = workflowStepId(step);
    const snapshot = (stepId ? snapshotsByStep.get(stepId) : undefined) || (actionKey ? snapshotsByAction.get(actionKey) : undefined) || step;
    const rawStatus = stringValue(snapshot, ["state", "status"]);
    const status = normalizedWorkflowStatus(rawStatus, stepId === currentStepId, Boolean(actionKey && actionKey === nextActionKey));
    const config = workflowStepConfig(step);
    const taskSnapshots = asRecordArray(snapshot.tasks);
    const taskStatusByKey = new Map(taskSnapshots.map((task) => [stringValue(task, ["key", "task_key"]), stringValue(task, ["status", "state"])]));

    return {
      key: stepId || actionKey || `step-${index}`,
      index: index + 1,
      name: stringValue(step, ["name", "title"], `Schritt ${index + 1}`),
      actionKey,
      type: stringValue(step, ["type"], stringValue(config, ["type"])),
      status,
      message: stringValue(snapshot, ["statusMessage", "message"], stringValue(config, ["description"])),
      tasks: workflowStepTasks(step).map((task) => {
        const key = stringValue(task, ["key", "task_key"]);
        return {
          key,
          title: stringValue(task, ["title", "name"], key || "Task"),
          status: taskStatusByKey.get(key) || stringValue(task, ["status", "state"], "configured"),
        };
      }),
      routes: workflowStepRoutes(step),
    };
  });
});

function setFeedback(message: string, type: FeedbackType = "info") {
  feedback.value = message;
  feedbackType.value = type;
}

async function runAction(key: string, callback: () => Promise<void>) {
  if (isBusy.value) return;
  busy.value = key;
  try {
    await callback();
  } catch (error) {
    setFeedback(String(error), "error");
  } finally {
    busy.value = null;
  }
}

async function refresh(silent = false) {
  try {
    await invoke("bootstrap_local_runtime");
    const [clientStatus, localDevices, localProcesses, pendingEvents] = await Promise.all([
      invoke<ClientStatus>("get_client_status"),
      invoke<LocalDevice[]>("get_local_devices"),
      invoke<LocalProcess[]>("get_local_processes", { limit: 100 }),
      invoke<OutboxEvent[]>("get_pending_events", { limit: 100 }),
    ]);
    status.value = clientStatus;
    devices.value = localDevices;
    processes.value = localProcesses;
    events.value = pendingEvents;
    if (workflowPreviewJob.value) {
      workflowPreview.value = await invoke<WorkflowProcessPreview>("get_workflow_process_preview", { jobUuid: workflowPreviewJob.value });
    }
    serverDomain.value = clientStatus.config.server_domain;
    if (!silent) setFeedback("Lokaler Status wurde aktualisiert.", "success");
  } catch (error) {
    if (!silent) setFeedback(`Status konnte nicht geladen werden: ${String(error)}`, "error");
  }
}

async function runAutopilot() {
  await runAction("cycle", async () => {
    const summary = await invoke<SyncSummary>("run_autopilot_cycle");
    lastCycleAt.value = new Date().toLocaleTimeString("de-DE");
    lastCycleSummary.value = summary.message;
    await refresh(true);
    setFeedback(`Synchronisierung abgeschlossen, ${summary.jobs_started} Job(s) übernommen.`, "success");
  });
}

async function discoverDevices() {
  await runAction("discover", async () => {
    devices.value = await invoke<LocalDevice[]>("discover_android_devices");
    await refresh(true);
    setFeedback(`${devices.value.length} Gerät(e) erkannt.`, "success");
  });
}

async function syncDevices() {
  await runAction("sync", async () => {
    await invoke("sync_devices_remote");
    await refresh(true);
    setFeedback("Geräte wurden mit AiUserFactory synchronisiert.", "success");
  });
}

async function registerNode() {
  await runAction("register", async () => {
    await invoke("register_node_remote", { node_name: null });
    await refresh(true);
    setFeedback("Node wurde registriert.", "success");
  });
}

async function sendHeartbeat() {
  await runAction("heartbeat", async () => {
    await invoke("send_heartbeat_remote", { status: "online", payload: { source: "client-ui" } });
    setFeedback("Heartbeat wurde gesendet.", "success");
  });
}

async function saveServerDomain() {
  await runAction("domain", async () => {
    await invoke("update_server_domain", { server_domain: serverDomain.value.trim() });
    await refresh(true);
    setFeedback("Server-Domain wurde gespeichert.", "success");
  });
}

async function queueEvent() {
  await runAction("event", async () => {
    await invoke("queue_event_local", { event_type: eventType.value.trim(), payload: JSON.parse(eventPayload.value) });
    await refresh(true);
    setFeedback("Event wurde in der lokalen Outbox gespeichert.", "success");
  });
}

async function markEventSent(id: number) {
  await runAction(`event-${id}`, async () => {
    await invoke("mark_event_sent", { event_id: id });
    await refresh(true);
  });
}

function readableDetails(raw: string) {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function asRecordArray(value: unknown): Array<Record<string, unknown>> {
  return Array.isArray(value) ? value.map(asRecord).filter((item): item is Record<string, unknown> => Boolean(item)) : [];
}

function stringValue(record: Record<string, unknown> | null | undefined, keys: string[], fallback = "") {
  if (!record) return fallback;
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim() !== "") return value;
    if (typeof value === "number" && Number.isFinite(value)) return String(value);
  }
  return fallback;
}

function workflowStepId(step: Record<string, unknown>) {
  return stringValue(step, ["workflowStepId", "workflow_step_id", "id"]);
}

function workflowActionKey(step: Record<string, unknown>) {
  return stringValue(step, ["actionKey", "action_key"]);
}

function workflowStepConfig(step: Record<string, unknown>): Record<string, unknown> {
  return asRecord(step.config) || asRecord(step.config_json) || {};
}

function workflowStepTasks(step: Record<string, unknown>) {
  const configTasks = asRecordArray(workflowStepConfig(step)["tasks"]);
  return configTasks.length ? configTasks : asRecordArray(step.tasks);
}

function workflowStepRoutes(step: Record<string, unknown>): WorkflowRoutePreview[] {
  const routes = asRecord(workflowStepConfig(step)["routes"]) || asRecord(step.routes);
  if (!routes) return [];

  return Object.entries(routes).map(([outcome, route]) => {
    const record = asRecord(route) || {};
    const target = stringValue(record, ["action_key", "step", "card", "target", "type"]);
    return {
      outcome,
      target,
      label: stringValue(record, ["label"], target),
    };
  }).filter((route) => route.target || route.label);
}

function uniqueWorkflowSnapshots(snapshots: Array<Record<string, unknown>>) {
  const seen = new Set<string>();
  return snapshots.filter((snapshot, index) => {
    const key = workflowStepId(snapshot) || workflowActionKey(snapshot) || `snapshot-${index}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function normalizedWorkflowStatus(status: string, isCurrent: boolean, isNext: boolean) {
  const normalized = status.toLowerCase();
  if (["completed", "success"].includes(normalized)) return "completed";
  if (["failed", "error", "timed_out", "timeout", "cancelled", "canceled", "lost"].includes(normalized)) return normalized === "timeout" ? "timed_out" : normalized;
  if (isCurrent) return "running";
  if (isNext) return "waiting";
  return normalized || "queued";
}

function isWorkflowProcess(process: LocalProcess) {
  return Boolean(process.job_id && ["workflow_task", "workflow_run"].includes(process.job_type));
}

async function openWorkflowPreview(process: LocalProcess) {
  if (!process.job_id) return;
  await runAction(`preview-${process.id}`, async () => {
    workflowPreviewJob.value = process.job_id || null;
    workflowPreview.value = await invoke<WorkflowProcessPreview>("get_workflow_process_preview", { jobUuid: process.job_id });
  });
}

function closeWorkflowPreview() {
  workflowPreview.value = null;
  workflowPreviewJob.value = null;
}

async function exportWorkflowDebug(jobUuid?: string | null) {
  if (!jobUuid) return;
  await runAction(`debug-${jobUuid}`, async () => {
    const path = await invoke<string>("export_workflow_process_debug", { jobUuid });
    setFeedback(`Debug-Export erstellt: ${path}`, "success");
  });
}

onMounted(async () => {
  await refresh(true);
  setFeedback("Hintergrunddienst aktiv. Updateaufträge werden beim nächsten Serverkontakt geprüft.");
  refreshTimer = window.setInterval(() => refresh(true), 5_000);
});

onUnmounted(() => {
  if (refreshTimer !== null) window.clearInterval(refreshTimer);
});
</script>

<template>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">FF</div>
        <div><strong>FollowFlow</strong><span>ClientController</span></div>
      </div>

      <div class="node-state">
        <span :class="['state-dot', { online }]"></span>
        <div><strong>{{ online ? "Verbunden" : "Nicht registriert" }}</strong><span>v{{ status?.app_version || "–" }}</span></div>
      </div>

      <nav class="menu">
        <button v-for="page in pages" :key="page.key" :class="['menu-item', { active: activePage === page.key }]" @click="activePage = page.key">
          <span class="menu-icon">{{ page.icon }}</span><span>{{ page.label }}</span>
          <span v-if="page.key === 'processes' && status?.running_processes" class="menu-badge">{{ status.running_processes }}</span>
          <span v-if="page.key === 'outbox' && status?.pending_events" class="menu-badge">{{ status.pending_events }}</span>
        </button>
      </nav>

      <div class="sidebar-footer">
        <span>Node UUID</span><code>{{ status?.config.node_uuid || "–" }}</code>
      </div>
    </aside>

    <main class="workspace">
      <header class="topbar">
        <div><p class="eyebrow">Node Console</p><h1>{{ pages.find((page) => page.key === activePage)?.label }}</h1></div>
        <div class="top-actions"><button class="button ghost" :disabled="isBusy" @click="refresh()">Aktualisieren</button><button class="button primary" :disabled="isBusy" @click="runAutopilot"><span v-if="busy === 'cycle'" class="spinner"></span>Jetzt synchronisieren</button></div>
      </header>

      <div :class="['feedback', feedbackType]">{{ feedback }}</div>

      <template v-if="activePage === 'overview'">
        <section class="hero-card">
          <div><p class="eyebrow light">Autopilot</p><h2>Node ist bereit für Remote-Aufträge</h2><p>Registrierung, Heartbeat, Geräte-Sync und freigegebene Updates laufen über den Hintergrunddienst.</p></div>
          <div class="hero-status"><span>Letzter manueller Lauf</span><strong>{{ lastCycleAt }}</strong><small>{{ lastCycleSummary }}</small></div>
        </section>

        <section class="stats-grid">
          <article class="metric"><span>Client-Version</span><strong>v{{ status?.app_version || "–" }}</strong><small>{{ status?.updater_available ? "Signierter Updater aktiv" : "Updater nicht verfügbar" }}</small></article>
          <article class="metric"><span>Lokale Geräte</span><strong>{{ status?.local_devices ?? 0 }}</strong><small>{{ status?.adb_available ? `ADB: ${status.adb_source}` : "ADB nicht verfügbar" }}</small></article>
          <article class="metric"><span>Aktive Prozesse</span><strong>{{ status?.running_processes ?? 0 }}</strong><small>{{ runtimeReady ? "Workflow-Runtime bereit" : "Runtime unvollständig" }}</small></article>
          <article class="metric"><span>Outbox</span><strong>{{ status?.pending_events ?? 0 }}</strong><small>Ausstehende Ereignisse</small></article>
        </section>

        <section class="panel">
          <div class="panel-heading"><div><h2>Schnellaktionen</h2><p>Manuelle Diagnose- und Synchronisationsaktionen.</p></div></div>
          <div class="action-grid"><button class="action-card" @click="registerNode" :disabled="isBusy"><span>01</span><strong>Node registrieren</strong><small>API-Schlüssel vom Server beziehen</small></button><button class="action-card" @click="sendHeartbeat" :disabled="isBusy"><span>02</span><strong>Heartbeat senden</strong><small>Version und Fähigkeiten melden</small></button><button class="action-card" @click="discoverDevices" :disabled="isBusy"><span>03</span><strong>Geräte erkennen</strong><small>Lokalen ADB-Scan starten</small></button><button class="action-card" @click="syncDevices" :disabled="isBusy"><span>04</span><strong>Geräte synchronisieren</strong><small>Inventar an AiUserFactory senden</small></button></div>
        </section>
      </template>

      <section v-else-if="activePage === 'devices'" class="panel">
        <div class="panel-heading"><div><h2>Android-Geräte</h2><p>Direkt am Node erkanntes Geräte-Inventar.</p></div><div class="top-actions"><button class="button ghost" @click="discoverDevices" :disabled="isBusy">ADB-Scan</button><button class="button primary" @click="syncDevices" :disabled="isBusy">Zum Server syncen</button></div></div>
        <div class="table-wrap"><table><thead><tr><th>Gerät</th><th>Plattform</th><th>ADB</th><th>Status</th><th>Zuletzt gesehen</th></tr></thead><tbody><tr v-for="device in devices" :key="device.device_uuid"><td><strong>{{ device.name }}</strong><code>{{ device.device_uuid }}</code></td><td>{{ device.platform }}</td><td><code>{{ device.adb_serial || "–" }}</code></td><td><span :class="['pill', device.status]">{{ device.status }}</span></td><td>{{ device.last_seen_at }}</td></tr><tr v-if="!devices.length"><td colspan="5" class="empty">Keine Geräte erkannt.</td></tr></tbody></table></div>
      </section>

      <section v-else-if="activePage === 'processes'" class="panel">
        <div class="panel-heading"><div><h2>Prozesse auf diesem Node</h2><p>ClientController-verwaltete Remote-Jobs und Workflow-Prozesse. Die Liste aktualisiert sich alle fünf Sekunden.</p></div><span class="live-indicator"><i></i> Live</span></div>
        <div class="process-list">
          <article v-for="process in processes" :key="process.id" class="process-row">
            <div :class="['process-icon', process.status]">{{ process.status === 'running' ? '▶' : process.status === 'success' ? '✓' : '!' }}</div>
            <div class="process-main">
              <div class="process-title"><strong>{{ process.job_type }}</strong><span :class="['pill', process.status]">{{ process.status }}</span></div>
              <code>{{ process.job_id || `local-${process.id}` }}</code>
              <div v-if="isWorkflowProcess(process)" class="process-actions">
                <button class="button ghost small" @click="openWorkflowPreview(process)">Vorschau</button>
                <button class="button ghost small" @click="exportWorkflowDebug(process.job_id)">Debug exportieren</button>
              </div>
              <details><summary>Details</summary><pre>{{ readableDetails(process.details_json) }}</pre></details>
            </div>
            <time>{{ process.created_at }}</time>
          </article>
          <div v-if="!processes.length" class="empty-card">Noch keine verwalteten Prozesse vorhanden.</div>
        </div>
      </section>

      <section v-else-if="activePage === 'outbox'" class="panel">
        <div class="panel-heading"><div><h2>Store-and-Forward Outbox</h2><p>Lokale Ereignisse, die noch nicht bestätigt wurden.</p></div></div>
        <div class="form-grid"><input v-model="eventType" placeholder="Event-Typ"><textarea v-model="eventPayload" rows="3"></textarea><button class="button primary" @click="queueEvent" :disabled="isBusy">Event speichern</button></div>
        <div class="event-list"><article v-for="event in events" :key="event.id"><div><strong>{{ event.event_type }}</strong><span>#{{ event.id }} · {{ event.created_at }}</span></div><code>{{ event.payload_json }}</code><button class="button ghost small" @click="markEventSent(event.id)">Als gesendet markieren</button></article><div v-if="!events.length" class="empty-card">Die Outbox ist leer.</div></div>
      </section>

      <section v-else class="settings-grid">
        <article class="panel"><div class="panel-heading"><div><h2>Server-Bindung</h2><p>Zentrale AiUserFactory-Instanz für diesen Node.</p></div></div><label>Server-Domain</label><input v-model="serverDomain" placeholder="https://factory.follow-flow.de"><button class="button primary" @click="saveServerDomain" :disabled="isBusy">Domain speichern</button></article>
        <article class="panel"><div class="panel-heading"><div><h2>Lokale Laufzeit</h2><p>Diagnosepfade und verfügbare Komponenten.</p></div></div><dl><div><dt>Umgebung</dt><dd>{{ status?.config.environment || "–" }}</dd></div><div><dt>Datenbank</dt><dd><code>{{ status?.db_path || "–" }}</code></dd></div><div><dt>Konfiguration</dt><dd><code>{{ status?.config_path || "–" }}</code></dd></div><div><dt>Workflow-Runtime</dt><dd><code>{{ status?.workflow_runtime_path || "nicht gefunden" }}</code></dd></div><div><dt>Rebind erlaubt</dt><dd>{{ status?.config.allow_server_rebind ? "Ja" : "Nein" }}</dd></div></dl></article>
      </section>

      <div v-if="workflowPreview" class="preview-overlay" @click.self="closeWorkflowPreview">
        <section class="preview-dialog">
          <header><div><p class="eyebrow">Workflow-Vorschau</p><h2>{{ workflowPreview.job_uuid }}</h2></div><button class="button ghost" @click="closeWorkflowPreview">Schliessen</button></header>
          <div class="preview-toolbar"><span :class="['pill', String(workflowPreview.status.state || '')]">{{ workflowPreview.status.state || 'unbekannt' }}</span><button class="button primary small" @click="exportWorkflowDebug(workflowPreview.job_uuid)">Debug exportieren</button></div>
          <section class="workflow-map">
            <div class="workflow-map-heading">
              <div><h3>Ablaufdiagramm</h3><p>{{ workflowStepTimeline.length }} Schritt(e) im lokalen Workflow-Kontext.</p></div>
              <span :class="['pill', String(workflowPreview.checkpoint.nextActionKey ? 'waiting' : workflowPreview.status.state || '')]">{{ workflowPreview.checkpoint.nextActionKey ? `Nächster Schritt: ${workflowPreview.checkpoint.nextActionKey}` : workflowPreview.status.state || 'unbekannt' }}</span>
            </div>
            <div v-if="workflowStepTimeline.length" class="workflow-map-track">
              <article v-for="step in workflowStepTimeline" :key="step.key" :class="['workflow-step-card', step.status]">
                <div class="workflow-step-head">
                  <span class="workflow-step-index">{{ step.index }}</span>
                  <div><strong>{{ step.name }}</strong><small>{{ step.actionKey || step.type || "ohne Action-Key" }}</small></div>
                  <span :class="['pill', step.status]">{{ step.status }}</span>
                </div>
                <p v-if="step.message">{{ step.message }}</p>
                <div v-if="step.tasks.length" class="workflow-task-list">
                  <span v-for="task in step.tasks" :key="task.key || task.title" :class="['workflow-task-chip', task.status]">{{ task.title }}</span>
                </div>
                <div v-if="step.routes.length" class="workflow-route-list">
                  <span v-for="route in step.routes" :key="`${step.key}-${route.outcome}`"><b>{{ route.outcome }}</b>{{ route.label || route.target }}</span>
                </div>
              </article>
            </div>
            <div v-else class="empty-card">Für diesen Prozess wurden noch keine Workflow-Schritte gespeichert.</div>
          </section>
          <img v-if="workflowPreview.screenshot_data_url" :src="workflowPreview.screenshot_data_url" alt="Live Workflow Screenshot" class="preview-image">
          <div v-else class="empty-card">Noch kein Screenshot vorhanden.</div>
          <div class="preview-columns">
            <details open><summary>Status</summary><pre>{{ JSON.stringify(workflowPreview.status, null, 2) }}</pre></details>
            <details><summary>Checkpoint</summary><pre>{{ JSON.stringify(workflowPreview.checkpoint, null, 2) }}</pre></details>
            <details><summary>Ergebnis</summary><pre>{{ JSON.stringify(workflowPreview.result, null, 2) }}</pre></details>
            <details v-if="workflowPreview.stderr_tail"><summary>Fehlerausgabe</summary><pre>{{ workflowPreview.stderr_tail }}</pre></details>
          </div>
          <code class="preview-path">{{ workflowPreview.run_directory }}</code>
        </section>
      </div>

      <div v-if="isBusy" class="busy-overlay"><div><span class="spinner large"></span><p>Aktion wird ausgeführt …</p></div></div>
    </main>
  </div>
</template>

<style scoped>
:global(*){box-sizing:border-box}:global(body){margin:0;background:#f4f7fb;color:#172033;font-family:Inter,"Segoe UI",sans-serif}:global(button),:global(input),:global(textarea){font:inherit}.app-shell{min-height:100vh;display:grid;grid-template-columns:260px minmax(0,1fr)}.sidebar{position:fixed;inset:0 auto 0 0;width:260px;display:flex;flex-direction:column;padding:22px 16px;background:#0b1220;color:#fff;border-right:1px solid #1d293b}.brand{display:flex;align-items:center;gap:12px;padding:0 8px 22px}.brand-mark{display:grid;width:42px;height:42px;place-items:center;border-radius:12px;background:linear-gradient(135deg,#06b6d4,#2563eb);font-weight:900}.brand strong,.brand span{display:block}.brand span{margin-top:2px;font-size:12px;color:#8794aa}.node-state{display:flex;align-items:center;gap:10px;margin:0 4px 20px;padding:12px;border:1px solid #243044;border-radius:12px;background:#111b2d}.node-state strong,.node-state span{display:block;font-size:12px}.node-state span{color:#8b98ad}.state-dot{width:9px;height:9px;border-radius:50%;background:#64748b;box-shadow:0 0 0 4px #64748b22}.state-dot.online{background:#34d399;box-shadow:0 0 0 4px #34d39922}.menu{display:grid;gap:5px}.menu-item{position:relative;display:flex;align-items:center;gap:12px;width:100%;padding:11px 13px;border:0;border-radius:10px;background:transparent;color:#9ba8bc;text-align:left;cursor:pointer}.menu-item:hover{background:#152136;color:#fff}.menu-item.active{background:linear-gradient(90deg,#1d4ed8,#2563eb);color:#fff;box-shadow:0 8px 20px #1d4ed833}.menu-icon{width:20px;text-align:center;font-size:18px}.menu-badge{margin-left:auto!important;display:grid!important;min-width:20px;height:20px;place-items:center;border-radius:10px;background:#ffffff24;color:#fff!important;font-size:10px!important;font-weight:800}.sidebar-footer{margin-top:auto;padding:16px 8px 4px;border-top:1px solid #223047}.sidebar-footer span,.sidebar-footer code{display:block}.sidebar-footer span{margin-bottom:5px;font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:#64748b}.sidebar-footer code{overflow:hidden;color:#94a3b8;font-size:10px;text-overflow:ellipsis}.workspace{grid-column:2;min-width:0;padding:26px 30px 40px}.topbar{display:flex;align-items:center;justify-content:space-between;gap:20px;margin-bottom:18px}.eyebrow{margin:0 0 5px;color:#2563eb;font-size:10px;font-weight:800;letter-spacing:.19em;text-transform:uppercase}.eyebrow.light{color:#67e8f9}.topbar h1{margin:0;font-size:26px}.top-actions{display:flex;gap:9px}.button{display:inline-flex;align-items:center;justify-content:center;gap:8px;border:0;border-radius:9px;padding:10px 15px;font-size:13px;font-weight:700;cursor:pointer}.button:disabled{opacity:.55;cursor:not-allowed}.button.primary{background:#2563eb;color:#fff;box-shadow:0 7px 15px #2563eb25}.button.ghost{border:1px solid #d8e0eb;background:#fff;color:#475569}.button.small{padding:7px 10px;font-size:11px}.feedback{margin-bottom:18px;padding:11px 14px;border:1px solid #dbe4ef;border-radius:9px;background:#fff;color:#475569;font-size:12px}.feedback.success{border-color:#a7f3d0;background:#ecfdf5;color:#047857}.feedback.error{border-color:#fecaca;background:#fef2f2;color:#b91c1c}.hero-card{display:flex;align-items:center;justify-content:space-between;gap:30px;padding:28px;border-radius:17px;background:radial-gradient(circle at 85% 15%,#164e63 0,transparent 35%),linear-gradient(135deg,#0f172a,#111827);color:#fff;box-shadow:0 18px 35px #0f172a20}.hero-card h2{margin:4px 0 8px;font-size:24px}.hero-card p{max-width:680px;margin:0;color:#aebbd0;font-size:13px;line-height:1.6}.hero-status{width:250px;padding:15px;border:1px solid #ffffff1f;border-radius:12px;background:#ffffff0d}.hero-status span,.hero-status strong,.hero-status small{display:block}.hero-status span{font-size:10px;color:#93a4ba;text-transform:uppercase}.hero-status strong{margin:4px 0;font-size:20px}.hero-status small{overflow:hidden;color:#93a4ba;font-size:10px;text-overflow:ellipsis;white-space:nowrap}.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:18px 0}.metric,.panel{border:1px solid #dfe6ef;border-radius:14px;background:#fff;box-shadow:0 6px 18px #17203308}.metric{padding:18px}.metric span,.metric strong,.metric small{display:block}.metric span{color:#64748b;font-size:11px;font-weight:700}.metric strong{margin:7px 0 4px;font-size:25px}.metric small{color:#94a3b8;font-size:10px}.panel{padding:20px}.panel-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;margin-bottom:18px}.panel-heading h2{margin:0;font-size:17px}.panel-heading p{margin:4px 0 0;color:#738096;font-size:12px}.action-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.action-card{padding:16px;border:1px solid #e1e7ef;border-radius:11px;background:#f9fbfd;text-align:left;cursor:pointer}.action-card:hover{border-color:#93c5fd;background:#eff6ff;transform:translateY(-1px)}.action-card span,.action-card strong,.action-card small{display:block}.action-card span{margin-bottom:18px;color:#2563eb;font:700 11px monospace}.action-card strong{font-size:13px}.action-card small{margin-top:5px;color:#8390a4;font-size:10px}.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse}th{padding:10px 12px;background:#f8fafc;color:#64748b;font-size:10px;text-align:left;text-transform:uppercase}td{padding:13px 12px;border-top:1px solid #edf1f6;font-size:12px}td strong,td code{display:block}td code{margin-top:4px;color:#7c8ba1;font-size:10px}.pill{display:inline-flex;border-radius:99px;padding:4px 8px;background:#e2e8f0;color:#475569;font-size:9px;font-weight:800;text-transform:uppercase}.pill.online,.pill.success{background:#d1fae5;color:#047857}.pill.running,.pill.busy{background:#dbeafe;color:#1d4ed8}.pill.failed,.pill.error{background:#fee2e2;color:#b91c1c}.empty{padding:35px;text-align:center;color:#94a3b8}.process-list,.event-list{display:grid;gap:9px}.process-row{display:grid;grid-template-columns:38px minmax(0,1fr) auto;gap:13px;align-items:start;padding:14px;border:1px solid #e4eaf2;border-radius:11px}.process-icon{display:grid;width:38px;height:38px;place-items:center;border-radius:10px;background:#e2e8f0;color:#475569;font-weight:900}.process-icon.running{background:#dbeafe;color:#2563eb}.process-icon.success{background:#d1fae5;color:#059669}.process-icon.failed{background:#fee2e2;color:#dc2626}.process-title{display:flex;align-items:center;gap:9px}.process-main>code{display:block;margin:4px 0;color:#8996aa;font-size:10px}.process-row time{color:#94a3b8;font-size:10px}.process-row details{margin-top:7px}.process-row summary{color:#64748b;font-size:10px;cursor:pointer}.process-row pre{max-height:180px;overflow:auto;padding:10px;border-radius:8px;background:#0f172a;color:#cbd5e1;font-size:10px;white-space:pre-wrap}.live-indicator{display:flex;align-items:center;gap:6px;color:#059669;font-size:11px;font-weight:700}.live-indicator i{width:7px;height:7px;border-radius:50%;background:#10b981;box-shadow:0 0 0 4px #10b9811f}.empty-card{padding:35px;border:1px dashed #cbd5e1;border-radius:10px;color:#94a3b8;text-align:center;font-size:12px}.form-grid{display:grid;grid-template-columns:1fr 2fr auto;gap:10px;margin-bottom:17px}.event-list article{display:grid;grid-template-columns:180px minmax(0,1fr) auto;gap:12px;align-items:center;padding:12px;border:1px solid #e4eaf2;border-radius:10px}.event-list strong,.event-list span{display:block}.event-list strong{font-size:12px}.event-list span{margin-top:3px;color:#94a3b8;font-size:9px}.event-list code{overflow:hidden;color:#64748b;font-size:10px;text-overflow:ellipsis;white-space:nowrap}.settings-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}label{display:block;margin:8px 0 6px;color:#475569;font-size:11px;font-weight:700}input,textarea{width:100%;border:1px solid #d5deea;border-radius:9px;padding:10px 12px;background:#fff;color:#1e293b;font-size:12px;outline:none}input:focus,textarea:focus{border-color:#60a5fa;box-shadow:0 0 0 3px #60a5fa20}.settings-grid .button{margin-top:12px}.settings-grid dl{display:grid;gap:12px;margin:0}.settings-grid dl div{display:grid;grid-template-columns:130px minmax(0,1fr);gap:12px}.settings-grid dt{color:#64748b;font-size:11px}.settings-grid dd{min-width:0;margin:0;font-size:11px}.settings-grid dd code{display:block;overflow:hidden;color:#475569;text-overflow:ellipsis;white-space:nowrap}.busy-overlay{position:fixed;inset:0 0 0 260px;display:grid;z-index:50;place-items:center;background:#0f172a30;backdrop-filter:blur(2px)}.busy-overlay>div{padding:22px 30px;border-radius:13px;background:#fff;box-shadow:0 20px 50px #0f172a30;text-align:center}.busy-overlay p{margin:10px 0 0;font-size:12px}.spinner{width:14px;height:14px;border:2px solid #ffffff55;border-top-color:#fff;border-radius:50%;animation:spin .7s linear infinite}.spinner.large{display:inline-block;width:26px;height:26px;border-color:#dbeafe;border-top-color:#2563eb}@keyframes spin{to{transform:rotate(360deg)}}@media(max-width:1000px){.app-shell{grid-template-columns:210px minmax(0,1fr)}.sidebar{width:210px}.workspace{padding:20px}.stats-grid,.action-grid{grid-template-columns:repeat(2,1fr)}.busy-overlay{left:210px}.settings-grid{grid-template-columns:1fr}}@media(max-width:720px){.app-shell{display:block}.sidebar{position:static;width:auto;min-height:auto}.menu{grid-template-columns:repeat(5,1fr)}.menu-item{justify-content:center;padding:9px}.menu-item>span:not(.menu-icon){display:none}.sidebar-footer,.node-state{display:none}.workspace{grid-column:auto;padding:15px}.topbar,.hero-card{align-items:flex-start;flex-direction:column}.stats-grid,.action-grid{grid-template-columns:1fr 1fr}.form-grid,.event-list article{grid-template-columns:1fr}.busy-overlay{left:0}}
.process-actions{display:flex;gap:8px;margin-top:9px}.preview-overlay{position:fixed;inset:0;display:grid;z-index:70;place-items:center;padding:24px;background:#0f172a99;backdrop-filter:blur(3px)}.preview-dialog{width:min(1100px,96vw);max-height:92vh;overflow:auto;padding:22px;border-radius:15px;background:#fff;box-shadow:0 25px 70px #02061766}.preview-dialog>header,.preview-toolbar{display:flex;align-items:center;justify-content:space-between;gap:15px}.preview-dialog h2{max-width:760px;margin:0;overflow:hidden;font-size:17px;text-overflow:ellipsis;white-space:nowrap}.preview-toolbar{margin:14px 0}.workflow-map{margin:0 0 14px;padding:14px;border:1px solid #dbe3ee;border-radius:10px;background:#f8fafc}.workflow-map-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px}.workflow-map-heading h3{margin:0;font-size:15px}.workflow-map-heading p{margin:3px 0 0;color:#64748b;font-size:11px}.workflow-map-track{display:flex;gap:10px;overflow-x:auto;padding-bottom:4px}.workflow-step-card{position:relative;flex:0 0 270px;min-height:154px;padding:12px;border:1px solid #d7e0ec;border-radius:9px;background:#fff}.workflow-step-card:after{content:"";position:absolute;top:30px;right:-10px;width:10px;height:2px;background:#cbd5e1}.workflow-step-card:last-child:after{display:none}.workflow-step-card.completed{border-color:#86efac;background:#f0fdf4}.workflow-step-card.running,.workflow-step-card.waiting{border-color:#93c5fd;background:#eff6ff}.workflow-step-card.failed,.workflow-step-card.timed_out,.workflow-step-card.cancelled{border-color:#fecaca;background:#fef2f2}.workflow-step-head{display:grid;grid-template-columns:28px minmax(0,1fr) auto;gap:9px;align-items:start}.workflow-step-index{display:grid;width:28px;height:28px;place-items:center;border-radius:8px;background:#0f172a;color:#fff;font-size:11px;font-weight:800}.workflow-step-head strong,.workflow-step-head small{display:block}.workflow-step-head strong{overflow:hidden;font-size:12px;text-overflow:ellipsis;white-space:nowrap}.workflow-step-head small{margin-top:2px;overflow:hidden;color:#64748b;font-size:10px;text-overflow:ellipsis;white-space:nowrap}.workflow-step-card>p{display:-webkit-box;min-height:32px;margin:9px 0;color:#475569;font-size:11px;line-height:1.45;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.workflow-task-list,.workflow-route-list{display:flex;flex-wrap:wrap;gap:5px}.workflow-task-chip,.workflow-route-list span{max-width:100%;overflow:hidden;border-radius:7px;padding:4px 6px;background:#e2e8f0;color:#475569;font-size:9px;font-weight:700;text-overflow:ellipsis;white-space:nowrap}.workflow-task-chip.success,.workflow-task-chip.completed{background:#bbf7d0;color:#047857}.workflow-task-chip.failed,.workflow-task-chip.timed_out{background:#fecaca;color:#b91c1c}.workflow-route-list{margin-top:8px}.workflow-route-list span{background:#eef2ff;color:#3730a3}.workflow-route-list b{margin-right:4px;color:#1e293b}.preview-image{display:block;width:100%;max-height:520px;object-fit:contain;border:1px solid #dbe3ee;border-radius:10px;background:#0f172a}.preview-columns{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:13px}.preview-columns details{min-width:0;padding:10px;border:1px solid #e2e8f0;border-radius:9px}.preview-columns summary{cursor:pointer;color:#475569;font-size:11px;font-weight:700}.preview-columns pre{max-height:260px;overflow:auto;padding:10px;border-radius:7px;background:#0f172a;color:#cbd5e1;font-size:10px;white-space:pre-wrap}.preview-path{display:block;margin-top:12px;overflow:hidden;color:#64748b;font-size:10px;text-overflow:ellipsis;white-space:nowrap}@media(max-width:720px){.preview-columns{grid-template-columns:1fr}.preview-overlay{padding:8px}.workflow-map-heading{display:block}.workflow-step-card{flex-basis:235px}}
</style>
