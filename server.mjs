import { execFile } from "node:child_process";
import { createServer } from "node:http";
import { access, readFile, mkdir, writeFile, rename, stat } from "node:fs/promises";
import { promisify } from "node:util";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import os from "node:os";
import { readdir } from "node:fs/promises";
import { gzip } from "node:zlib";

const execFileAsync = promisify(execFile);
const gzipAsync = promisify(gzip);
const host = "127.0.0.1";
const port = Number.parseInt(process.env.MOBILEWORK_TRACE_PORT || "8770", 10);
const appVersion = "v0.0.35";
const appDirectory = path.dirname(fileURLToPath(import.meta.url));
const home = os.homedir();
const mobileworkRoot = process.env.MOBILEWORK_HOME || path.join(home, ".mobilework");
const configuredDatabase = process.env.MOBILEWORK_OPENCODE_DB || "";
let database = configuredDatabase || path.join(mobileworkRoot, "xdg", "data", "opencode", "opencode.db");
const sqliteExecutable = process.env.MOBILEWORK_SQLITE || (process.platform === "win32" ? path.join(appDirectory, "runtime", "sqlite3.exe") : "sqlite3");
const htmlPath = path.join(appDirectory, "index.html");
const snapshotPath = process.env.MOBILEWORK_TRACE_SNAPSHOTS || path.join(home, ".mobilework-trace-dashboard", "capability-snapshots.json");
const nativeSnapshotPath = path.join(home, ".mobilework-trace-dashboard", "native-capability-snapshots.json");
const timingPath = path.join(home, ".mobilework-trace-dashboard", "stream-timings.json");
const routedModelPath = path.join(home, ".mobilework-trace-dashboard", "routed-models.json");
const modelRawRoot = path.join(mobileworkRoot, "plugin-reporting", "model-raw");
const reportTracePath = path.join(mobileworkRoot, "plugin-reporting", "report-trace.log");
const runtimeStatePath = path.join(mobileworkRoot, "electron", "openwork-server-state.json");
const runtimeTokensPath = path.join(mobileworkRoot, "electron", "openwork-server-tokens.json");
const secretPattern = /(?:authorization|api[-_]?key|access[_-]?token|refresh[_-]?token|cookie|password|secret)/i;
const secretValuePattern = /(?:Bearer\s+[A-Za-z0-9._~+/=-]{12,}|\b(?:sk|rk|pk)_[A-Za-z0-9_-]{12,})/gi;
const monitorStartedAt = Date.now();

let html = "";
let snapshotStore = {};
let nativeSnapshotStore = {};
let nativeSnapshotRevision = '';
let snapshotWrite = Promise.resolve();
let timingStore = { format: "mobilework-stream-timings/v4", collectorStartedAt: Date.now(), steps: {} };
const liveMessages = new Set();
const liveClients = new Set();
let liveNotifyTimer = null;
function notifyLiveClients() {
  if (liveNotifyTimer) return;
  liveNotifyTimer = setTimeout(() => {
    liveNotifyTimer = null;
    for (const client of liveClients) client.write('data: {}\n\n');
  }, 500);
}
let timingSaveTimer = null;
const timingRevisions = new Map();
const timingConnections = new Map();
const recentTimingEvents = new Set();
let runtimeWorkspaceCache = { expiresAt: 0, targets: [] };
let previousCpuSample = null;
let previousNetworkSample = null;
const routedModelCache = new Map();
let routedModelStore = { format: "mobilework-routed-models/v2", sessions: {} };
let routedModelWrite = Promise.resolve();
let reportTraceCache = { signature: "", sessions: new Map() };

async function resolveDatabase() {
  const candidates = configuredDatabase ? [configuredDatabase] : [
    path.join(mobileworkRoot, "xdg", "data", "opencode", "opencode.db"),
    path.join(mobileworkRoot, "data", "opencode", "opencode.db"),
    path.join(mobileworkRoot, "opencode.db"),
  ];
  for (const candidate of candidates) {
    try {
      await access(candidate);
      database = candidate;
      return candidate;
    } catch {}
  }
  throw new Error(`找不到 MobileWork 数据库。已检查：${candidates.join("；")}`);
}

function safeJson(value, fallback = null) {
  try { return JSON.parse(value); } catch { return fallback; }
}

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function redact(value, key = "") {
  if (secretPattern.test(key)) return "[redacted]";
  if (typeof value === "string") return value.replace(secretValuePattern, "[redacted]");
  if (Array.isArray(value)) return value.map((item) => redact(item));
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([name, item]) => [name, redact(item, name)]));
}

async function query(sql) {
  const { stdout } = await execFileAsync(sqliteExecutable, ["-readonly", "-json", database, sql], { maxBuffer: 32 * 1024 * 1024 });
  return safeJson(stdout, []);
}

async function readJsonFile(file, fallback) {
  try { return safeJson(await readFile(file, "utf8"), fallback); } catch { return fallback; }
}

export async function refreshNativeSnapshots(file = nativeSnapshotPath) {
  // A writer may temporarily leave incomplete JSON. Keep the last valid snapshot.
  const value = await readJsonFile(file, null);
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    nativeSnapshotStore = redact(value);
    nativeSnapshotRevision = createHash('sha256').update(JSON.stringify(nativeSnapshotStore)).digest('hex');
  }
  return nativeSnapshotRevision;
}

function eventTime(properties, receiptAt) {
  const value = Number(properties?.timestamp);
  return Number.isFinite(value) && value > 0 ? value : receiptAt;
}

function createTimingAttempt(timestamp, { partial = false, source = "runtime-step" } = {}) {
  return {
    measurementVersion: 4,
    startedAt: partial ? null : timestamp,
    startObserved: !partial,
    startSource: partial ? null : source,
    firstChunkAt: null,
    firstChunkKind: null,
    lastChunkAt: null,
    lastChunkKey: null,
    lastChunkKind: null,
    lastChunkAtByStream: {},
    streamGaps: { text: [], reasoning: [], toolInput: [] },
    chunkCount: 0,
    openStreams: [],
    endedAt: null,
    outcome: null,
  };
}

function timingAttempt(record, timestamp, partial = false) {
  let attempt = record.attempts.at(-1);
  if (!attempt || attempt.endedAt != null || attempt.measurementVersion !== 4) {
    attempt = createTimingAttempt(timestamp, { partial });
    record.attempts.push(attempt);
  }
  return attempt;
}

export function applyTimingEvent(store, event, receiptAt = Date.now()) {
  const type = event?.type?.replace(/\.\d+$/, '');
  const properties = event?.properties || {};
  const sessionId = properties.sessionID;
  const messageId = properties.assistantMessageID;
  if (typeof sessionId !== "string" || typeof messageId !== "string") return null;
  const timestamp = eventTime(properties, receiptAt);
  let record = store.steps[messageId];
  if (!record) record = store.steps[messageId] = { sessionId, messageId, attempts: [], updatedAt: timestamp };
  const streamKind = type?.includes(".text.") ? "text" : type?.includes(".reasoning.") ? "reasoning" : type?.includes(".tool.input.") ? "tool-input" : null;
  const streamId = properties.textID || properties.reasoningID || properties.callID || null;
  const streamKey = streamKind && streamId ? `${streamKind}:${streamId}` : null;
  if (type === "session.next.step.started") {
    record.requestedModel = properties.model ? { id: properties.model.id || null, providerID: properties.model.providerID || null } : record.requestedModel;
    record.agent = typeof properties.agent === 'string' ? properties.agent : record.agent;
    const previous = record.attempts.at(-1);
    if (!previous || previous.startedAt !== timestamp) record.attempts.push(createTimingAttempt(timestamp));
  } else if (type === "session.next.retried") {
    const previous = record.attempts.at(-1);
    if (previous && previous.endedAt == null) {
      previous.endedAt = timestamp;
      previous.outcome = "retry";
      previous.openStreams = [];
    }
    record.attempts.push(createTimingAttempt(timestamp, { source: "runtime-retry" }));
  } else if (streamKey && type.endsWith(".started")) {
    const attempt = timingAttempt(record, timestamp, true);
    attempt.openStreams ||= [];
    if (!attempt.openStreams.includes(streamKey)) attempt.openStreams.push(streamKey);
  } else if (["session.next.text.delta", "session.next.reasoning.delta", "session.next.tool.input.delta"].includes(type)) {
    const attempt = timingAttempt(record, timestamp, true);
    attempt.openStreams ||= [];
    if (streamKey && !attempt.openStreams.includes(streamKey)) attempt.openStreams.push(streamKey);
    if (!streamKey || !(Number(properties.timestamp) > 0)) attempt.capturePartial = true;
    attempt.lastChunkAtByStream ||= {};
    attempt.streamGaps ||= { text: [], reasoning: [], toolInput: [] };
    const previousChunkAt = Number(attempt.lastChunkAtByStream[streamKey]);
    if (streamKey && Number.isFinite(previousChunkAt) && timestamp >= previousChunkAt) {
      const category = streamKind === "tool-input" ? "toolInput" : streamKind;
      attempt.streamGaps[category] ||= [];
      attempt.streamGaps[category].push({ streamKey, startedAt: previousChunkAt, endedAt: timestamp, durationMs: timestamp - previousChunkAt });
    }
    if (attempt.firstChunkAt == null) {
      attempt.firstChunkAt = timestamp;
      attempt.firstChunkKind = streamKind;
      attempt.firstChunkPartial = Boolean(attempt.capturePartial);
    }
    attempt.lastChunkAt = timestamp;
    attempt.lastChunkKey = streamKey;
    attempt.lastChunkKind = streamKind;
    if (streamKey) attempt.lastChunkAtByStream[streamKey] = timestamp;
    attempt.chunkCount += 1;
  } else if (streamKey && type.endsWith(".ended")) {
    const attempt = timingAttempt(record, timestamp, true);
    attempt.openStreams = (attempt.openStreams || []).filter(key => key !== streamKey);
    if (attempt.lastChunkAtByStream) delete attempt.lastChunkAtByStream[streamKey];
  } else if (type === "session.next.step.ended" || type === "session.next.step.failed") {
    const attempt = timingAttempt(record, timestamp, true);
    attempt.openStreams = [];
    attempt.endedAt = timestamp;
    attempt.outcome = type.endsWith("failed") ? "failed" : "completed";
  } else return null;
  record.updatedAt = Math.max(Number(record.updatedAt) || 0, timestamp);
  return record;
}

function timingForMessage(messageId, traceCompletedAt = null) {
  const record = timingStore.steps[messageId];
  if (!record) return null;
  const traceClosed = Number.isFinite(Number(traceCompletedAt)) && Number(traceCompletedAt) > 0;
  return {
    collectorStartedAt: timingStore.collectorStartedAt,
    attempts: record.attempts.map(attempt => ({
      startedAt: attempt.startedAt,
      measurementVersion: attempt.measurementVersion || 1,
      startObserved: attempt.startObserved,
      capturePartial: Boolean(attempt.capturePartial),
      startSource: attempt.startSource || null,
      firstChunkAt: attempt.firstChunkAt,
      firstChunkPartial: attempt.firstChunkPartial ?? (!attempt.startObserved || Boolean(attempt.capturePartial)),
      firstChunkKind: attempt.firstChunkKind || null,
      lastChunkAt: attempt.lastChunkAt,
      lastChunkKey: attempt.lastChunkKey || null,
      lastChunkKind: attempt.lastChunkKind || null,
      lastChunkAtByStream: attempt.lastChunkAtByStream || {},
      streamGaps: attempt.streamGaps || null,
      legacyStallGapsMs: Number(attempt.measurementVersion) < 4 ? (attempt.stallGapsMs || []) : [],
      chunkCount: attempt.chunkCount,
      streamActive: Boolean(attempt.openStreams?.length) && !traceClosed,
      openStreams: traceClosed ? [] : (attempt.openStreams || []),
      lifecycleClosedBy: traceClosed && attempt.endedAt == null ? "session-message-completed" : null,
      endedAt: attempt.endedAt,
      outcome: attempt.outcome,
    })),
  };
}

function scheduleTimingSave() {
  if (timingSaveTimer) return;
  timingSaveTimer = setTimeout(async () => {
    timingSaveTimer = null;
    try {
      await mkdir(path.dirname(timingPath), { recursive: true });
      const temporary = `${timingPath}.tmp`;
      await writeFile(temporary, JSON.stringify(timingStore), "utf8");
      await rename(temporary, timingPath);
    } catch {}
  }, 250);
}

function ingestTimingEvent(event, receiptAt = Date.now(), sourceKey = null) {
  if (event?.id) {
    if (recentTimingEvents.has(event.id)) return;
    recentTimingEvents.add(event.id);
    if (recentTimingEvents.size > 10000) recentTimingEvents.delete(recentTimingEvents.values().next().value);
  }
  const record = applyTimingEvent(timingStore, event, receiptAt);
  if (!record) return;
  record.collectorSource = sourceKey;
  liveMessages.add(record.messageId);
  timingRevisions.set(record.sessionId, String(record.updatedAt));
  scheduleTimingSave();
  const type = event?.type?.replace(/\.\d+$/, '');
  const firstChunk = ["session.next.text.delta", "session.next.reasoning.delta", "session.next.tool.input.delta"].includes(type)
    && record.attempts?.at(-1)?.chunkCount === 1;
  if (firstChunk || ["session.next.step.started", "session.next.retried", "session.next.step.ended", "session.next.step.failed"].includes(type)) notifyLiveClients();
}

export function mergeLiveSteps(timeline, records, observed, sessionId, now = Date.now()) {
  const byId = new Map(timeline.filter(item => item.type === 'assistant').map(item => [item.id, item]));
  const warnings = [];
  for (const record of Object.values(records)) {
    if (record.sessionId !== sessionId || !observed.has(record.messageId)) continue;
    const attempt = record.attempts?.at(-1);
    if (!attempt) continue;
    let step = byId.get(record.messageId);
    if (!step && attempt.endedAt != null && now - attempt.endedAt > 30000) {
      warnings.push({ sessionId, reason: `Trace 落库超时：${record.messageId}` });
      continue;
    }
    if (!step) {
      step = { id: record.messageId, type: 'assistant', provisional: true, time: { created: attempt.startedAt || attempt.firstChunkAt || record.updatedAt }, finish: null, tools: [], tokens: {}, routedModels: [], modelCalls: [] };
      timeline.push(step);
      byId.set(step.id, step);
    }
    step.estimatedStartedAt = attempt.startObserved ? attempt.startedAt : null;
    step.requestedModel = record.requestedModel || null;
    const traceCompleted = Number.isFinite(Number(step.time?.completed)) && Number(step.time.completed) > 0;
    step.waitTiming = { attempts: record.attempts.map(item => ({ ...item, streamActive: Boolean(item.openStreams?.length) && !traceCompleted, lifecycleClosedBy: traceCompleted && item.endedAt == null ? "session-message-completed" : null })) };
    if (!step.finish && traceCompleted) {
      step.livePhase = "ended-incomplete";
    } else if (!step.finish) {
      step.livePhase = attempt.endedAt != null ? 'syncing' : attempt.firstChunkAt != null ? 'streaming' : 'awaiting-first-chunk';
      if (attempt.endedAt != null) {
        step.time.completed = attempt.endedAt;
        step.finish = attempt.outcome === 'failed' ? 'error' : 'collected-end';
      }
    }
  }
  return { timeline, warnings };
}

async function consumeTimingEvents(port, workspaceId, token, signal) {
  const response = await fetch(`http://127.0.0.1:${port}/workspace/${encodeURIComponent(workspaceId)}/opencode/event`, {
    headers: { authorization: `Bearer ${token}`, accept: "text/event-stream" },
    signal,
  });
  if (!response.ok || !response.body) throw new Error(`runtime-event-http-${response.status}`);
  const decoder = new TextDecoder();
  let pending = "";
  for await (const chunk of response.body) {
    pending += decoder.decode(chunk, { stream: true });
    for (;;) {
      const match = pending.match(/\r?\n\r?\n/);
      if (!match || match.index == null) break;
      const block = pending.slice(0, match.index);
      pending = pending.slice(match.index + match[0].length);
      const data = block.split(/\r?\n/).filter(line => line.startsWith("data:")).map(line => line.slice(5).trim()).join("\n");
      if (data) ingestTimingEvent(safeJson(data, null), Date.now(), `${port}:${workspaceId}`);
    }
  }
}

async function runTimingConnection(key, port, workspaceId, token, controller) {
  while (!controller.signal.aborted && timingConnections.get(key) === controller) {
    try { await consumeTimingEvents(port, workspaceId, token, controller.signal); } catch {}
    for (const record of Object.values(timingStore.steps)) if (record.collectorSource === key) invalidateOpenTiming(record);
    scheduleTimingSave();
    if (controller.signal.aborted) break;
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
}

function invalidateOpenTiming(record) {
  for (const attempt of record.attempts || []) if (attempt.endedAt == null) {
    attempt.capturePartial = true;
    attempt.lastChunkAtByStream = {};
    attempt.openStreams = [];
  }
}

async function refreshTimingConnections() {
  const [state, tokens] = await Promise.all([readJsonFile(runtimeStatePath, {}), readJsonFile(runtimeTokensPath, {})]);
  const desired = new Map();
  for (const [directory, value] of Object.entries(state?.workspacePorts || {})) {
    const port = Number(value), token = tokens?.workspaces?.[directory]?.clientToken;
    if (!Number.isInteger(port) || typeof token !== "string") continue;
    try {
      const response = await fetch(`http://127.0.0.1:${port}/workspaces`, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(5000) });
      const body = await response.json();
      const workspaces = Array.isArray(body?.items) ? body.items : Array.isArray(body?.workspaces) ? body.workspaces : [];
      for (const workspace of workspaces) if (typeof workspace?.id === "string") desired.set(`${port}:${workspace.id}`, { port, workspaceId: workspace.id, token });
    } catch {}
  }
  for (const [key, controller] of timingConnections) if (!desired.has(key)) { controller.abort(); timingConnections.delete(key); }
  for (const [key, target] of desired) if (!timingConnections.has(key)) {
    const controller = new AbortController();
    timingConnections.set(key, controller);
    runTimingConnection(key, target.port, target.workspaceId, target.token, controller).catch(() => {});
  }
}

async function startTimingCollector() {
  const saved = await readJsonFile(timingPath, null);
  if (["mobilework-stream-timings/v1", "mobilework-stream-timings/v2", "mobilework-stream-timings/v3", "mobilework-stream-timings/v4"].includes(saved?.format) && saved.steps && typeof saved.steps === "object") {
    timingStore = { ...saved, format: "mobilework-stream-timings/v4" };
    for (const record of Object.values(timingStore.steps)) invalidateOpenTiming(record);
    if (saved.format !== timingStore.format) scheduleTimingSave();
  }
  for (const record of Object.values(timingStore.steps)) if (record?.sessionId) timingRevisions.set(record.sessionId, String(record.updatedAt || 0));
  await refreshTimingConnections();
  setInterval(() => refreshTimingConnections().catch(() => {}), 10000);
}

function comparableDirectory(value) {
  return String(value || "").replaceAll("\\", "/").replace(/\/+$/, "").toLowerCase();
}

async function runtimeWorkspaceTargets() {
  if (runtimeWorkspaceCache.expiresAt > Date.now()) return runtimeWorkspaceCache.targets;
  const [state, tokens] = await Promise.all([readJsonFile(runtimeStatePath, {}), readJsonFile(runtimeTokensPath, {})]);
  const targets = new Map();
  for (const [directory, value] of Object.entries(state?.workspacePorts || {})) {
    const port = Number(value), token = tokens?.workspaces?.[directory]?.clientToken;
    if (!Number.isInteger(port) || typeof token !== "string") continue;
    try {
      const response = await fetch(`http://127.0.0.1:${port}/workspaces`, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(5000) });
      if (!response.ok) continue;
      const body = await response.json();
      const workspaces = Array.isArray(body?.items) ? body.items : Array.isArray(body?.workspaces) ? body.workspaces : [];
      for (const workspace of workspaces) if (typeof workspace?.id === "string") {
        const workspaceDirectory = workspace.directory || workspace.path;
        targets.set(`${port}:${workspace.id}`, { port, token, workspaceId: workspace.id, directory: workspaceDirectory, comparableDirectory: comparableDirectory(workspaceDirectory) });
      }
    } catch {}
  }
  runtimeWorkspaceCache = { expiresAt: Date.now() + 5000, targets: [...targets.values()] };
  return runtimeWorkspaceCache.targets;
}

async function runtimeCapabilityInventory(directory) {
  const sessionDirectory = comparableDirectory(directory);
  const targets = await runtimeWorkspaceTargets();
  const target = targets.filter(item => sessionDirectory === item.comparableDirectory || sessionDirectory.startsWith(`${item.comparableDirectory}/`)).sort((a, b) => b.comparableDirectory.length - a.comparableDirectory.length)[0];
  if (!target) return { runtimeAvailable: false, toolIds: [], mcp: [] };
  try {
    const base = `http://127.0.0.1:${target.port}/workspace/${encodeURIComponent(target.workspaceId)}/opencode`;
    const options = { headers: { authorization: `Bearer ${target.token}` }, signal: AbortSignal.timeout(5000) };
    const [toolResponse, mcpResponse] = await Promise.all([fetch(`${base}/experimental/tool/ids`, options), fetch(`${base}/mcp`, options)]);
    if (!toolResponse.ok || !mcpResponse.ok) return { runtimeAvailable: false, toolIds: [], mcp: [] };
    const [toolBody, mcpBody] = await Promise.all([toolResponse.json(), mcpResponse.json()]);
    const toolIds = [...new Set((Array.isArray(toolBody) ? toolBody : []).filter(item => typeof item === "string"))].sort();
    const mcp = Object.entries(mcpBody && typeof mcpBody === "object" && !Array.isArray(mcpBody) ? mcpBody : {}).map(([id, item]) => ({ id, name: id, status: item?.status || null })).sort((a, b) => a.name.localeCompare(b.name));
    return { runtimeAvailable: true, runtimeWorkspaceId: target.workspaceId, toolIds, mcp };
  } catch {
    return { runtimeAvailable: false, toolIds: [], mcp: [] };
  }
}

async function capabilityInventory(directory) {
  const account = await readJsonFile(path.join(mobileworkRoot, "current-account.json"), {});
  const accountId = account?.account?.id;
  const registryFiles = [path.join(mobileworkRoot, "data", "default", "skills", "registry.json")];
  if (accountId) registryFiles.push(path.join(mobileworkRoot, "data", accountId, "skills", "registry.json"));
  const registries = await Promise.all(registryFiles.map(file => readJsonFile(file, {})));
  const skills = new Map();
  for (const registry of registries) for (const [id, item] of Object.entries(registry || {})) {
    const custom = item?.custom || item;
    skills.set(id, { id, name: custom?.name || id, enabled: item?.enabled !== false && custom?.defaultEnabled !== false, category: custom?.category || null, source: custom?.source || null });
  }
  const runtime = await runtimeCapabilityInventory(directory);
  return { skills: [...skills.values()].sort((a, b) => a.name.localeCompare(b.name)), ...runtime, source: runtime.runtimeAvailable ? "mobilework-runtime-api" : "mobilework-skills-registry", capturedAt: Date.now() };
}

async function saveSnapshots() {
  snapshotWrite = snapshotWrite.then(async () => { await mkdir(path.dirname(snapshotPath), { recursive: true }); await writeFile(snapshotPath, JSON.stringify(snapshotStore, null, 2), "utf8"); }).catch(() => {});
  return snapshotWrite;
}

async function captureSnapshot(session, { allowBackfill = false } = {}) {
  if (!session) return null;
  const existing = snapshotStore[session.id] || null;
  const createdAfterMonitor = Number(session.time_created) >= monitorStartedAt;
  const hasCorruptedToolId = existing?.toolIds?.some(toolId => String(toolId).includes("[redacted]"));
  const runtimeCaptureRecorded = existing?.runtimeAvailable && !hasCorruptedToolId && ["runtime-first-observed", "runtime-backfill"].includes(existing.captureMode);
  if ((!createdAfterMonitor && !allowBackfill && !existing) || runtimeCaptureRecorded) return existing;
  const inventory = await capabilityInventory(session.directory);
  if (existing && !inventory.runtimeAvailable) return existing;
  const snapshot = { ...(existing || {}), sessionId: session.id, sessionCreatedAt: session.time_created, firstObservedAt: existing?.firstObservedAt || Date.now(), captureMode: existing || !createdAfterMonitor ? "runtime-backfill" : "runtime-first-observed", ...inventory };
  snapshotStore[session.id] = redact(snapshot);
  await saveSnapshots();
  return snapshotStore[session.id];
}

async function captureNewSessions(rows) {
  const newRows = rows.filter(row => Number(row.time_created) >= monitorStartedAt && !snapshotStore[row.id]?.runtimeAvailable);
  if (newRows.length) await Promise.all(newRows.map(captureSnapshot));
}

function modelLabel(value) {
  const model = safeJson(value, {});
  return { id: model?.id || null, provider: model?.providerID || null, variant: model?.variant || null };
}

export function responseModelNames(responseRaw) {
  const names = new Set();
  const add = value => { if (typeof value === "string" && value.trim()) names.add(value.trim()); };
  for (const line of String(responseRaw || "").split(/\r?\n/)) {
    if (!line.startsWith("data:")) continue;
    const payload = line.slice(5).trim();
    if (!payload || payload === "[DONE]") continue;
    const chunk = safeJson(payload, null);
    add(chunk?.model);
    add(chunk?.response?.model);
  }
  if (!names.size) {
    const response = safeJson(String(responseRaw || ""), null);
    add(response?.model);
    add(response?.response?.model);
  }
  return [...names];
}

async function reportingTraceCallsForSession(sessionId) {
  let info;
  try { info = await stat(reportTracePath); } catch { return []; }
  const signature = `${info.size}:${info.mtimeMs}`;
  if (reportTraceCache.signature !== signature) {
    const sessions = new Map();
    const content = await readFile(reportTracePath, "utf8");
    for (const line of content.split(/\r?\n/)) {
      if (!line.includes("CONVERSATION_EVENT") || !line.includes("modelCallDetailList")) continue;
      const traceEntry = safeJson(line, null);
      const request = safeJson(traceEntry?.request, null);
      for (const batch of request?.batchEventList || []) {
        if (batch?.eventType !== "CONVERSATION_EVENT" || typeof batch.sessionId !== "string") continue;
        const calls = sessions.get(batch.sessionId) || [];
        for (const conversation of batch.conversationList || []) for (const call of conversation.modelCallDetailList || []) {
          const startedAt = Number(call.startTime) || null, finishedAt = Number(call.completeTime) || null;
          if (typeof call.modelCallId !== "string" || !startedAt) continue;
          calls.push({
            modelCallId: call.modelCallId,
            messageId: typeof call.msgId === "string" ? call.msgId : conversation.msgId,
            startedAt,
            finishedAt,
            status: finishedAt ? "completed" : conversation.qaStatus || null,
            requestModelRef: call.modelId || call.modelName || null,
            effectiveModelRef: call.providerId || null,
            actualModels: responseModelNames(call.modelResponseRaw),
            evidenceSource: "plugin-reporting/report-trace.log",
          });
        }
        sessions.set(batch.sessionId, calls);
      }
    }
    reportTraceCache = { signature, sessions };
  }
  return reportTraceCache.sessions.get(sessionId) || [];
}

async function routedModelsForSession(sessionId) {
  const directory = path.join(modelRawRoot, sessionId);
  let files = [];
  try { files = (await readdir(directory)).filter(name => /^\d+-\d+\.json$/.test(name)).sort((a, b) => Number(a.split("-")[0]) - Number(b.split("-")[0])); }
  catch {}
  const stored = routedModelStore.sessions?.[sessionId];
  let reportInfo = null;
  try { reportInfo = await stat(reportTracePath); } catch {}
  const reportSignature = reportInfo ? `${reportInfo.size}:${reportInfo.mtimeMs}` : "missing";
  const signature = `${files.join("|")}|${stored?.updatedAt || 0}|${reportSignature}|${Math.floor(Date.now()/2000)}`;
  const cached = routedModelCache.get(sessionId);
  if (cached?.signature === signature) return cached.value;
  const freshCalls = [];
  for (const file of files) {
    const record = await readJsonFile(path.join(directory, file), null);
    if (!record || record.sessionId !== sessionId || typeof record.msgId !== "string") continue;
    const actualModels = responseModelNames(record.responseRaw);
    freshCalls.push({
      modelCallId: record.modelCallId || file,
      messageId: record.msgId,
      startedAt: Number(record.startedAt) || null,
      finishedAt: Number(record.finishedAt) || null,
      status: record.status || null,
      requestModelRef: record.requestModelRef || null,
      effectiveModelRef: record.modelRef || null,
      actualModels,
    });
  }
  const reportingCalls = await reportingTraceCallsForSession(sessionId);
  freshCalls.push(...reportingCalls);
  if (freshCalls.length && mergeRoutedModelCalls(sessionId, freshCalls)) await saveRoutedModelStore();
  const calls = (routedModelStore.sessions?.[sessionId]?.calls || freshCalls).map(call => ({ ...call, actualModels: [...(call.actualModels || [])] }));
  const byMessage = new Map();
  for (const call of calls) {
    const values = byMessage.get(call.messageId) || [];
    for (const name of call.actualModels) if (!values.includes(name)) values.push(name);
    byMessage.set(call.messageId, values);
  }
  const all = [];
  for (const call of calls) for (const name of call.actualModels) if (!all.includes(name)) all.push(name);
  const value = { byMessage, all, calls };
  routedModelCache.set(sessionId, { signature: `${files.join("|")}|${routedModelStore.sessions?.[sessionId]?.updatedAt || 0}|${reportSignature}|${Math.floor(Date.now()/2000)}`, value });
  return value;
}

export function attachRoutedModels(timeline, routedModels) {
  const assistants = timeline.filter(item => item.type === "assistant");
  const byId = new Map(assistants.map(item => [item.id, item]));
  for (const assistant of assistants) {
    assistant.routedModels = [];
    assistant.modelCalls = [];
  }
  for (const call of routedModels.calls || []) {
    const point = Number(call.startedAt);
    const callEnd = call.finishedAt == null ? NaN : Number(call.finishedAt);
    let target = null;
    let association = "time-within-session";
    const exact = byId.get(call.messageId);
    const eligible = assistants.filter(step => !(Number(step.time?.completed) > 0 && point > Number(step.time.completed)));
    if (exact && eligible.includes(exact)) { target = exact; association = 'message-id'; }
    else if (Number.isFinite(point) && point > 0 && Number.isFinite(callEnd)) {
      // Reporting msgId can identify a user turn. Use stream evidence before Trace creation time.
      const streamMatches = eligible.filter(step => (step.waitTiming?.attempts || []).some(attempt =>
        Number(attempt.firstChunkAt) >= point && Number(attempt.firstChunkAt) <= callEnd + 1000
        && Number(attempt.lastChunkAt || attempt.firstChunkAt) <= callEnd + 1000));
      if (streamMatches.length === 1) { target = streamMatches[0]; association = 'stream-window'; }
      else if (!streamMatches.length) {
        const candidates = eligible.filter(step => Number(step.time?.created) >= point && Number(step.time.created) <= callEnd + 1000);
        if (candidates.length === 1) { target = candidates[0]; association = 'trace-window-partial'; }
      }
    }
  if (!target) continue;
    target.routedModels ||= [];
    for (const name of call.actualModels || []) if (!target.routedModels.includes(name)) target.routedModels.push(name);
    target.modelCalls ||= [];
    if (!target.modelCalls.some(existing => existing.modelCallId === call.modelCallId)) target.modelCalls.push({
      modelCallId: call.modelCallId,
      startedAt: Number(call.startedAt) || null,
      finishedAt: Number(call.finishedAt) || null,
      status: call.status || null,
      actualModels: [...(call.actualModels || [])],
      association,
      evidenceSource: call.evidenceSource || null,
    });
    if (association !== "message-id") target.routedModelAssociation = association;
  }
  for (const assistant of assistants) assistant.modelCalls.sort((a, b) => Number(a.startedAt || 0) - Number(b.startedAt || 0));
  return timeline;
}

function requestStartForAssociation(step) {
  return step.time?.created;
}

function mergeRoutedModelCalls(sessionId, calls) {
  routedModelStore = routedModelStore && typeof routedModelStore === "object" ? routedModelStore : {};
  routedModelStore.format = "mobilework-routed-models/v2";
  routedModelStore.sessions = routedModelStore.sessions && typeof routedModelStore.sessions === "object" ? routedModelStore.sessions : {};
  const current = Array.isArray(routedModelStore.sessions[sessionId]?.calls) ? routedModelStore.sessions[sessionId].calls : [];
  const byId = new Map(current.map(call => [call.modelCallId, call]));
  let changed = false;
  for (const call of calls) {
    const actualModels = [...new Set((call.actualModels || []).filter(name => typeof name === "string" && name.trim()).map(name => name.trim().slice(0, 200)))];
    const startedAt = Number(call.startedAt) || null, finishedAt = Number(call.finishedAt) || null;
    if (typeof call.messageId !== "string" || (!actualModels.length && !startedAt && !finishedAt)) continue;
    const existing = byId.get(call.modelCallId);
    if (existing) {
      const mergedModels = [...new Set([...(existing.actualModels || []), ...actualModels])];
      if (mergedModels.join("\u0000") !== (existing.actualModels || []).join("\u0000") || (!existing.startedAt && startedAt) || (!existing.finishedAt && finishedAt) || existing.status !== call.status || (!existing.evidenceSource && call.evidenceSource)) {
        existing.actualModels = mergedModels;
        existing.startedAt = startedAt || existing.startedAt || null;
        existing.finishedAt = finishedAt || existing.finishedAt || null;
        existing.status = call.status || existing.status || null;
        existing.evidenceSource = call.evidenceSource || existing.evidenceSource || null;
        changed = true;
      }
      continue;
    }
    const item = { modelCallId: String(call.modelCallId), messageId: call.messageId, startedAt, finishedAt, status: call.status || null, actualModels, evidenceSource: call.evidenceSource || (call.requestModelRef || call.effectiveModelRef ? "plugin-reporting/model-raw" : null) };
    current.push(item);
    byId.set(item.modelCallId, item);
    changed = true;
  }
  if (changed) routedModelStore.sessions[sessionId] = { updatedAt: Date.now(), calls: current.sort((a, b) => Number(a.startedAt || 0) - Number(b.startedAt || 0)) };
  return changed;
}

async function saveRoutedModelStore() {
  routedModelWrite = routedModelWrite.then(async () => {
    await mkdir(path.dirname(routedModelPath), { recursive: true });
    const temporary = `${routedModelPath}.tmp`;
    await writeFile(temporary, JSON.stringify(routedModelStore), "utf8");
    await rename(temporary, routedModelPath);
  }).catch(() => {});
  return routedModelWrite;
}

function attachRetryEvidence(timeline, evidence, calls) {
  const assistants = timeline.filter(item => item.type === "assistant");
  const byId = new Map(assistants.map(item => [item.id, item]));
  for (const item of evidence.all || []) {
    const call = calls.find(candidate => candidate.modelCallId === item.modelCallId);
    let target = item.assistantMessageId ? byId.get(item.assistantMessageId) : call ? byId.get(call.messageId) : null;
    const associationPoint = Number(call?.startedAt ?? item.timestamp);
    if (!target && Number.isFinite(associationPoint)) {
      const candidates = assistants.map(step => {
        const start = Number(step.time?.created), end = Number(step.time?.completed) || start;
        const point = associationPoint;
        const distance = point < start ? start - point : point > end ? point - end : 0;
        return { step, distance };
      }).filter(candidate => Number.isFinite(candidate.distance) && candidate.distance <= 120000).sort((a, b) => a.distance - b.distance);
      if (candidates.length && (!candidates[1] || candidates[1].distance - candidates[0].distance >= 5000)) target = candidates[0].step;
    }
    if (target) {
      item.assistantMessageId = target.id;
      if (!call) item.stepAssociation = "time-within-session";
      target.retryEvidence ||= [];
      if (!target.retryEvidence.some(existing => existing.timestamp === item.timestamp && existing.operation === item.operation && existing.attempt === item.attempt)) target.retryEvidence.push(item);
    }
  }
  return timeline;
}

async function nativeRetryEvidenceForSession(sessionId, read = query) {
  const rows = await read(`select
    json_extract(data,'$.timestamp') as timestamp,
    json_extract(data,'$.attempt') as attempt,
    json_extract(data,'$.error.message') as errorMessage,
    json_extract(data,'$.error.isRetryable') as isRetryable,
    json_extract(data,'$.assistantMessageID') as assistantMessageId
    from event
    where type='session.next.retried.1' and json_extract(data,'$.sessionID')=${sqlString(sessionId)}
    order by json_extract(data,'$.timestamp'),rowid`);
  const all = rows.map(row => ({
    timestamp: Number(row.timestamp),
    operation: "session.next",
    attempt: Number(row.attempt),
    maxAttempts: null,
    confidence: "native-session-event",
    assistantMessageId: row.assistantMessageId || null,
    isRetryable: Boolean(row.isRetryable),
    errorMessage: typeof row.errorMessage === "string" ? row.errorMessage : null,
  })).filter(item => Number.isFinite(item.timestamp) && Number.isFinite(item.attempt));
  return { byMessage: new Map(), all };
}

function parseCpuLine(line) {
  const values = line.trim().split(/\s+/).slice(1).map(Number);
  const idle = values[3] + values[4];
  return { total: values.reduce((sum, value) => sum + (Number.isFinite(value) ? value : 0), 0), idle };
}

async function readCpuSample() {
  const proc = await readFile('/proc/stat', 'utf8');
  const line = proc.split('\n').find(value => value.startsWith('cpu '));
  return line ? parseCpuLine(line) : null;
}

function parseProcessStat(value) {
  const end = value.lastIndexOf(')');
  if (end < 0) return null;
  const fields = value.slice(end + 2).trim().split(/\s+/);
  const pid = Number(value.slice(0, value.indexOf(' ')));
  const name = value.slice(value.indexOf(' ') + 2, end);
  const ppid = Number(fields[1]);
  const ticks = Number(fields[11]) + Number(fields[12]);
  const startTicks = Number(fields[19]);
  return Number.isInteger(pid) && Number.isInteger(ppid) ? { pid, name, ppid, ticks, startTicks } : null;
}

async function processSnapshot() {
  let entries;
  try { entries = (await readdir('/proc')).filter(name => /^\d+$/.test(name)); } catch { return []; }
  const processes = await Promise.all(entries.map(async pid => {
    try {
      const [stat, status] = await Promise.all([readFile(`/proc/${pid}/stat`, 'utf8'), readFile(`/proc/${pid}/status`, 'utf8')]);
      const result = parseProcessStat(stat);
      if (!result) return null;
      const rss = Number(status.match(/^VmRSS:\s+(\d+)/m)?.[1] || 0) * 1024;
      return { ...result, rss };
    } catch { return null; }
  }));
  return processes.filter(Boolean);
}

async function readNetworkSample() {
  const text = await readFile('/proc/net/dev', 'utf8');
  const interfaces = {};
  for (const line of text.split('\n').slice(2)) {
    const match = line.match(/^\s*([^:]+):\s*(.*)$/);
    if (!match) continue;
    const fields = match[2].trim().split(/\s+/).map(Number);
    if (fields.length < 16) continue;
    interfaces[match[1].trim()] = { rxBytes: fields[0], rxPackets: fields[1], rxErrors: fields[2], txBytes: fields[8], txPackets: fields[9], txErrors: fields[10] };
  }
  return interfaces;
}

function deltaRate(current, previous, elapsedMs) {
  return previous == null || !Number.isFinite(elapsedMs) || elapsedMs <= 0 ? 0 : Math.max(0, current - previous) * 1000 / elapsedMs;
}

export async function collectResources(now = Date.now()) {
  const [cpu, network, processes] = await Promise.all([readCpuSample(), readNetworkSample(), processSnapshot()]);
  const cpuElapsed = previousCpuSample ? now - previousCpuSample.at : 0;
  const cpuUsage = cpu && previousCpuSample && cpu.total >= previousCpuSample.value.total
    ? Math.max(0, Math.min(100, (1 - (cpu.idle - previousCpuSample.value.idle) / (cpu.total - previousCpuSample.value.total)) * 100)) : null;
  previousCpuSample = cpu ? { at: now, value: cpu } : previousCpuSample;
  const interfaces = Object.fromEntries(Object.entries(network).filter(([name]) => name !== 'lo').map(([name, value]) => {
    const previous = previousNetworkSample?.interfaces?.[name];
    return [name, { ...value, rxBytesPerSecond: deltaRate(value.rxBytes, previous?.rxBytes, previousNetworkSample ? now - previousNetworkSample.at : 0), txBytesPerSecond: deltaRate(value.txBytes, previous?.txBytes, previousNetworkSample ? now - previousNetworkSample.at : 0) }];
  }));
  const totals = Object.values(interfaces).reduce((sum, value) => ({ rxBytes: sum.rxBytes + value.rxBytes, txBytes: sum.txBytes + value.txBytes, rxPackets: sum.rxPackets + value.rxPackets, txPackets: sum.txPackets + value.txPackets, rxErrors: sum.rxErrors + value.rxErrors, txErrors: sum.txErrors + value.txErrors, rxBytesPerSecond: sum.rxBytesPerSecond + value.rxBytesPerSecond, txBytesPerSecond: sum.txBytesPerSecond + value.txBytesPerSecond }), { rxBytes: 0, txBytes: 0, rxPackets: 0, txPackets: 0, rxErrors: 0, txErrors: 0, rxBytesPerSecond: 0, txBytesPerSecond: 0 });
  previousNetworkSample = { at: now, interfaces: network };
  const mobileNames = new Set(processes.filter(process => process.name === 'mobilework').map(process => process.pid));
  const mobileRoots = processes.filter(process => process.name === 'mobilework' && !mobileNames.has(process.ppid)).map(process => process.pid);
  const mobilePids = new Set(mobileRoots);
  let added = true;
  while (added) { added = false; for (const process of processes) if (!mobilePids.has(process.pid) && mobilePids.has(process.ppid)) { mobilePids.add(process.pid); added = true; } }
  const mobileProcesses = processes.filter(process => mobilePids.has(process.pid));
  const previousProcesses = collectResources.previousProcesses || new Map();
  const processElapsed = collectResources.previousProcessAt ? now - collectResources.previousProcessAt : 0;
  const previousMobileTicks = [...previousProcesses.entries()].filter(([pid]) => mobilePids.has(pid)).reduce((sum, [pid, item]) => sum + (mobileProcesses.find(process => process.pid === pid)?.ticks - item.ticks || 0), 0);
  collectResources.previousProcesses = new Map(processes.map(process => [process.pid, process]));
  collectResources.previousProcessAt = now;
  const bootAt = now - os.uptime() * 1000;
  return { ok: true, capturedAt: now, host: { cpu: { cores: os.cpus().length, usagePercent: cpuUsage, loadAverage: os.loadavg().slice(0, 3) }, memory: { totalBytes: os.totalmem(), freeBytes: os.freemem(), usedBytes: Math.max(0, os.totalmem() - os.freemem()), usagePercent: (1 - os.freemem() / os.totalmem()) * 100 }, network: { interfaces, totals } }, mobilework: { processCount: mobileProcesses.length, rootProcessCount: mobileRoots.length, cpuPercent: processElapsed > 0 ? Math.max(0, previousMobileTicks * 1000 / processElapsed) : null, memoryBytes: mobileProcesses.reduce((sum, process) => sum + process.rss, 0), uptimeSeconds: mobileProcesses.length ? Math.max(...mobileProcesses.map(process => Math.max(0, Math.floor((now - (bootAt + process.startTicks / 100 * 1000)) / 1000)))) : 0 } };
}

function capabilitySnapshot(sessionId) {
  const observed = snapshotStore[sessionId] || null;
  const native = nativeSnapshotStore[sessionId] || null;
  if (!observed && !native) return null;
  return {
    ...(observed || {}),
    ...(native || {}),
    skills: observed?.skills || [],
    mcp: native?.mcp || observed?.mcp || [],
    toolIds: native?.toolIds || observed?.toolIds || [],
  };
}

function normalizeAssistant(row) {
  const data = redact(safeJson(row.data, {}));
  const tokens = data.tokens || {};
  const content = Array.isArray(data.content) ? data.content : [];
  const tools = content.filter((part) => part?.type === "tool").map((tool) => ({
    id: tool.id || null,
    callID: tool.callID || tool.state?.callID || tool.state?.metadata?.callID || tool.id || null,
    childSessionId: tool.state?.metadata?.sessionId || null,
    name: tool.name || "unknown",
    status: tool.state?.status || tool.provider?.executed === false ? (tool.state?.status || "not-executed") : "unknown",
    input: tool.state?.input ?? null,
    output: tool.state?.output ?? tool.state?.result ?? null,
    error: tool.state?.error ?? null,
    provider: redact(tool.provider || null),
    time: tool.time || null,
  }));
  return {
    id: row.id,
    seq: row.seq,
    type: "assistant",
    time: data.time || { created: row.time_created, completed: row.time_updated },
    agent: data.agent || null,
    model: data.model || null,
    finish: data.finish || null,
    error: data.error || null,
    tokens: {
      input: tokens.input ?? 0,
      output: tokens.output ?? 0,
      reasoning: tokens.reasoning ?? 0,
      cacheRead: tokens.cache?.read ?? 0,
      cacheWrite: tokens.cache?.write ?? 0,
    },
    text: content.filter((part) => part?.type === "text").map((part) => part.text || "").join(""),
    thinking: content.filter((part) => part?.type === "reasoning").map((part) => part.text || part.reasoning || "").join(""),
    tools,
    waitTiming: timingForMessage(row.id, data.time?.completed),
  };
}

function normalizeMessage(row) {
  if (row.type === "assistant") return normalizeAssistant(row);
  const data = redact(safeJson(row.data, {}));
  return {
    id: row.id,
    seq: row.seq,
    type: row.type,
    time: data.time || { created: row.time_created, completed: row.time_updated },
    text: data.text || data.summary || "",
    agent: data.agent || null,
    model: data.model || null,
    error: data.error || null,
    raw: data,
  };
}

async function sessions() {
  const rows = await query(`select id,parent_id,title,agent,model,directory,time_created,time_updated,time_archived,tokens_input,tokens_output,tokens_reasoning,tokens_cache_read,tokens_cache_write,(select title from session parent where parent.id=session.parent_id) as parent_title,(select count(*) from session child where child.parent_id=session.id) as child_count from session order by time_updated desc limit 500`);
  await captureNewSessions(rows);
  return rows.map((row) => ({
    id: row.id,
    parentId: row.parent_id || null,
    parentTitle: row.parent_title || null,
    childCount: Number(row.child_count) || 0,
    title: row.title,
    agent: row.agent || null,
    directory: row.directory,
    createdAt: row.time_created,
    updatedAt: row.time_updated,
    archived: row.time_archived != null,
    model: modelLabel(row.model),
    timingRevision: `${timingRevisions.get(row.id) || 0}:${routedModelStore.sessions?.[row.id]?.updatedAt || 0}`,
    tokens: { input: row.tokens_input, output: row.tokens_output, reasoning: row.tokens_reasoning, cacheRead: row.tokens_cache_read, cacheWrite: row.tokens_cache_write },
  }));
}

export async function trace(sessionId, read = query, traversal = { seen: new Set(), depth: 0 }) {
  if (!/^ses_[A-Za-z0-9]+$/.test(sessionId)) throw new Error("Invalid session ID");
  if (!traversal.read) {
    let activeReads = 0;
    const waiting = [];
    traversal.read = async sql => {
      if (activeReads >= 8) await new Promise(resolve => waiting.push(resolve));
      activeReads += 1;
      try {
        return await read(sql);
      } finally {
        activeReads -= 1;
        waiting.shift()?.();
      }
    };
  }
  const boundedRead = traversal.read;
  const [session] = await boundedRead(`select id,parent_id,title,agent,model,directory,time_created,time_updated,tokens_input,tokens_output,tokens_reasoning,tokens_cache_read,tokens_cache_write from session where id=${sqlString(sessionId)}`);
  if (!session) return null;
  await captureSnapshot(session, { allowBackfill: true });
  if (!traversal.claimed) traversal.seen.add(sessionId);
  const rows = await boundedRead(`select id,type,seq,time_created,time_updated,data from session_message where session_id=${sqlString(sessionId)} order by seq`);
  const routedModels = await routedModelsForSession(sessionId);
  const retryEvidence = await nativeRetryEvidenceForSession(sessionId, boundedRead);
  const normalizedTimeline = rows.map(normalizeMessage).map(item => item.type === "assistant" ? { ...item, retryEvidence: [] } : item);
  const live = mergeLiveSteps(normalizedTimeline, timingStore.steps, liveMessages, sessionId);
  const timeline = attachRetryEvidence(attachRoutedModels(normalizedTimeline, routedModels), retryEvidence, routedModels.calls);
  const assistants = timeline.filter((item) => item.type === "assistant");
  const toolCalls = assistants.flatMap((item) => item.tools || []);
  // Read only identity metadata, including historical parts whose latest state lost it.
  const evidence = await boundedRead(`select json_extract(data,'$.callID') as callID,json_extract(data,'$.state.metadata.sessionId') as childSessionId from part where session_id=${sqlString(sessionId)} and json_extract(data,'$.tool')='task'
    union all select json_extract(data,'$.part.callID'),json_extract(data,'$.part.state.metadata.sessionId') from event where json_extract(data,'$.sessionID')=${sqlString(sessionId)} and json_extract(data,'$.part.tool')='task'
    union all select json_extract(data,'$.callID'),json_extract(data,'$.result.value.metadata.sessionId') from event where type='session.next.tool.success.1' and json_extract(data,'$.sessionID')=${sqlString(sessionId)}`);
  const children = await boundedRead(`select id from session where parent_id=${sqlString(sessionId)} order by time_created,id`);
  const associations = resolveTaskLinks(toolCalls, evidence);
  const ids = new Set([...children.map(c => c.id), ...associations.filter(a => a.status === 'explicit').map(a => a.childSessionId)]);
  const childTraces = [], warnings = [...live.warnings];
  const childIds = [...ids];
  const childResults = new Array(childIds.length);
  const pending = [];
  for (const [index, id] of childIds.entries()) {
    let reason;
    if (!/^ses_[A-Za-z0-9]+$/.test(id)) reason = 'invalid-session-id';
    else if (traversal.seen.has(id)) reason = 'cycle-or-duplicate';
    else if (traversal.depth >= 16 || traversal.seen.size >= 200) reason = 'traversal-limit';
    else {
      traversal.seen.add(id);
      pending.push({ id, index });
    }
    if (reason) childResults[index] = { warning: { sessionId: id, reason } };
  }
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(8, pending.length) }, async () => {
    while (next < pending.length) {
      const { id, index } = pending[next++];
      const child = await trace(id, read, { seen: traversal.seen, depth: traversal.depth + 1, claimed: true, read: boundedRead });
      childResults[index] = child ? { child } : { warning: { sessionId: id, reason: 'missing-session' } };
    }
  }));
  for (const result of childResults) {
    if (result.child) childTraces.push(result.child);
    if (result.warning) warnings.push(result.warning);
  }
  for (const association of associations) {
    if (association.status === 'explicit') association.status = childTraces.some(c => c.session.id === association.childSessionId) ? 'resolved' : warnings.find(w => w.sessionId === association.childSessionId)?.reason || 'unresolved';
  }
  return {
    session: { ...session, model: modelLabel(session.model), routedModels: routedModels.all, retryEvidence: retryEvidence.all, tokens: { input: session.tokens_input, output: session.tokens_output, reasoning: session.tokens_reasoning, cacheRead: session.tokens_cache_read, cacheWrite: session.tokens_cache_write }, capabilitySnapshot: capabilitySnapshot(session.id), timingRevision: `${timingRevisions.get(session.id) || 0}:${routedModelStore.sessions?.[session.id]?.updatedAt || 0}` },
    summary: {
      steps: assistants.length,
      toolCalls: toolCalls.length,
      toolFailures: toolCalls.filter((tool) => tool.status === "error" || tool.error).length,
      modelFailures: assistants.filter((item) => item.error || item.finish === "error").length,
      incomplete: assistants.filter((item) => !item.error && !item.finish).length,
    },
    timeline,
    childTraces,
    associations,
    warnings,
  };
}

export function resolveTaskLinks(tools, evidence) {
  return tools.filter(t => t.name === 'task').map(tool => {
    const callID = tool.callID || tool.id;
    const candidates = new Set([tool.childSessionId, ...evidence.filter(e => callID && e.callID === callID).map(e => e.childSessionId)].filter(Boolean));
    const childSessionId = candidates.size === 1 ? [...candidates][0] : null;
    return { callID, childSessionId, status: candidates.size > 1 ? 'conflicting-metadata' : childSessionId ? 'explicit' : 'missing-metadata' };
  });
}

function flattenTraceContexts(root) {
  const contexts = [];
  const visit = item => {
    if (!item?.session?.id || contexts.some(existing => existing.session.id === item.session.id)) return;
    contexts.push(item);
    for (const child of item.childTraces || []) visit(child);
  };
  visit(root);
  return contexts;
}

function exportedToolKind(tool, snapshot) {
  if (tool?.name === "skill") return { type: "skill", source: "part.state.input.name" };
  const provider = tool?.provider || {};
  if (provider.type === "mcp" || provider.source === "mcp" || provider.mcp === true || provider.serverID || provider.serverId) {
    return { type: "mcp", source: "part.provider" };
  }
  const names = (snapshot?.mcp || []).flatMap(item => [item.id, item.name]).filter(Boolean);
  if (names.some(name => tool?.name === name || tool?.name?.startsWith(`${name}_`) || tool?.name?.startsWith(`${name}__`) || tool?.name?.startsWith(`${name}:`))) {
    return { type: "mcp", source: "capability-snapshot-prefix" };
  }
  return { type: "tool", source: "part.tool" };
}

function exportedSkillName(tool) {
  if (tool?.name !== "skill") return null;
  const input = typeof tool.input === "string" ? safeJson(tool.input, null) : tool.input;
  return typeof input?.name === "string" && input.name.trim() ? input.name.trim() : null;
}

function nativeEvidence(context) {
  const snapshot = context.session.capabilitySnapshot || null;
  const assistants = (context.timeline || []).filter(item => item.type === "assistant");
  const modelCalls = assistants.flatMap(step => (step.modelCalls || []).map(call => ({
    messageId: step.id,
    requestedModel: step.requestedModel || null,
    ...call,
  })));
  const streamTimings = assistants.filter(step => step.waitTiming).map(step => ({
    messageId: step.id,
    requestedModel: step.requestedModel || null,
    ...step.waitTiming,
  }));
  const capabilityCalls = assistants.flatMap(step => (step.tools || []).map(tool => {
    const kind = exportedToolKind(tool, snapshot);
    const skillName = exportedSkillName(tool);
    return {
      messageId: step.id,
      callId: tool.callID || tool.id || null,
      type: kind.type,
      name: skillName ? `skill -> ${skillName}` : tool.name,
      rawToolName: tool.name,
      status: tool.status || null,
      time: tool.time || null,
      provider: tool.provider || null,
      evidenceSource: kind.source,
    };
  }));
  const compactions = (context.timeline || []).filter(item => item.type === "compaction").map(item => ({
    id: item.id,
    seq: item.seq,
    time: item.time || null,
    source: "session_message.type=compaction",
  }));
  const retryEvidence = context.session.retryEvidence || [];
  return {
    sessionId: context.session.id,
    capabilitySnapshot: snapshot,
    routedModels: context.session.routedModels || [],
    modelCalls,
    streamTimings,
    retryEvidence,
    compactions,
    capabilityCalls,
    taskAssociations: context.associations || [],
    warnings: context.warnings || [],
    completeness: {
      capabilitySnapshot: snapshot ? (snapshot.captureMode || "captured") : "not-captured",
      modelCalls: modelCalls.length ? "captured" : "not-captured",
      streamTimings: streamTimings.length ? "captured" : "not-captured",
      retryEvents: "native-event-table-exported",
      compactions: "native-session-message-exported",
      childTraversal: (context.warnings || []).length ? "partial" : "complete",
    },
  };
}

async function nativeTables(sessionId) {
  const [session] = await query(`select * from session where id=${sqlString(sessionId)}`);
  if (!session) return null;
  const [messages, parts, events] = await Promise.all([
    query(`select * from session_message where session_id=${sqlString(sessionId)} order by seq`),
    query(`select * from part where session_id=${sqlString(sessionId)} order by time_created, id`),
    query(`select * from event where aggregate_id=${sqlString(sessionId)} order by seq`),
  ]);
  const redactDataRows = rows => rows.map(row => {
    if (typeof row.data !== "string") return row;
    const parsed = safeJson(row.data, null);
    return parsed && typeof parsed === "object" ? { ...row, data: JSON.stringify(redact(parsed)) } : row;
  });
  return { session, session_message: redactDataRows(messages), part: redactDataRows(parts), event: redactDataRows(events) };
}

export async function nativeSession(sessionId) {
  if (!/^ses_[A-Za-z0-9]+$/.test(sessionId)) throw new Error("Invalid session ID");
  const enriched = await trace(sessionId);
  if (!enriched) return null;
  const contexts = flattenTraceContexts(enriched);
  const rawSessions = [];
  for (const context of contexts) {
    const tables = await nativeTables(context.session.id);
    if (tables) rawSessions.push({ sessionId: context.session.id, parentId: tables.session.parent_id || null, tables });
  }
  const root = rawSessions.find(item => item.sessionId === sessionId);
  const evidence = contexts.map(nativeEvidence);
  const warnings = contexts.flatMap(context => context.warnings || []);
  return redact({
    format: "mobilework-native-session-export/v2",
    exportedAt: new Date().toISOString(),
    redaction: "Sensitive credential fields and recognizable bearer/API-key values are redacted by the local trace API.",
    scope: {
      rootSessionId: sessionId,
      sessionIds: rawSessions.map(item => item.sessionId),
      includesLinkedDescendants: true,
      traversalStatus: warnings.length ? "partial" : "complete",
    },
    sources: [
      { id: "opencode-sqlite", content: ["session", "session_message", "part", "event"], raw: true },
      { id: "mobilework-capability-snapshot", content: ["tools", "skills", "mcp"], raw: false },
      { id: "plugin-reporting-model-summary", content: ["request-boundaries", "routed-models", "status"], rawBodiesIncluded: false },
      { id: "runtime-stream-timing", content: ["first-chunk", "stream-chunks", "retry-attempts"], rawBodiesIncluded: false },
    ],
    compatibility: "tables contains the root session in the v1 shape; relatedSessions contains linked descendants.",
    tables: root?.tables || null,
    relatedSessions: rawSessions.filter(item => item.sessionId !== sessionId),
    evidence: { sessions: evidence },
    completeness: {
      status: warnings.length ? "partial" : "mixed",
      notes: [
        "SQLite rows and linked descendant rows are exported when present.",
        "Capability, model-call, stream-timing, retry and compaction evidence report not-captured instead of inventing historical values.",
        "Raw plugin-reporting request/response bodies and credentials are intentionally excluded.",
      ],
      warnings,
    },
  });
}

async function send(response, status, payload, request = null) {
  const body = JSON.stringify(payload);
  const headers = { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store", Vary: "Accept-Encoding" };
  if (body.length >= 65536 && /\bgzip\b/i.test(request?.headers?.['accept-encoding'] || '')) {
    const compressed = await gzipAsync(body);
    response.writeHead(status, { ...headers, "Content-Encoding": "gzip", "Content-Length": compressed.length });
    response.end(compressed);
    return;
  }
  response.writeHead(status, { ...headers, "Content-Length": Buffer.byteLength(body) });
  response.end(body);
}

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url || "/", `http://${host}:${port}`);
    if (request.method === 'GET' && url.pathname === '/api/live-events') {
      response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive' });
      response.write('data: {}\n\n');
      liveClients.add(response);
      const heartbeat = setInterval(() => response.write(': heartbeat\n\n'), 15000);
      response.on('close', () => { clearInterval(heartbeat); liveClients.delete(response); });
      return;
    }
    if (['/api/sessions', '/api/trace', '/api/capabilities'].includes(url.pathname)) await refreshNativeSnapshots();
    if (request.method === "GET" && url.pathname === "/") {
      response.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
      response.end(html);
      return;
    }
    if (request.method === "GET" && url.pathname === "/api/sessions") return send(response, 200, { ok: true, version: appVersion, sessions: await sessions(), snapshotRevision: nativeSnapshotRevision, refreshedAt: Date.now() });
    if (request.method === "GET" && url.pathname === "/api/resources") return process.platform === "linux" ? send(response, 200, await collectResources()) : send(response, 501, { ok: false, error: "此便携版暂不支持 Windows 资源监控" });
    if (request.method === "GET" && url.pathname === "/api/capabilities") {
      const sessionId = url.searchParams.get("sessionId") || "";
      const snapshot = capabilitySnapshot(sessionId);
      return send(response, 200, { ok: true, monitorStartedAt, snapshot, currentInventory: snapshot ? null : await capabilityInventory() });
    }
    if (request.method === "GET" && url.pathname === "/api/native-session") {
      const result = await nativeSession(url.searchParams.get("sessionId") || "");
      return result ? send(response, 200, { ok: true, ...result }, request) : send(response, 404, { ok: false, error: "Session not found" });
    }
    if (request.method === "GET" && url.pathname === "/api/trace") {
      const snapshotRevision = nativeSnapshotRevision;
      const result = await trace(url.searchParams.get("sessionId") || "");
      return result ? send(response, 200, { ok: true, ...result, snapshotRevision, timingRevision: result.session.timingRevision, refreshedAt: Date.now() }, request) : send(response, 404, { ok: false, error: "Session not found" });
    }
    send(response, 404, { ok: false, error: "Not found" });
  } catch (error) {
    send(response, 500, { ok: false, error: error instanceof Error ? error.message : String(error) });
  }
});

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await resolveDatabase();
  snapshotStore = await readJsonFile(snapshotPath, {});
  routedModelStore = await readJsonFile(routedModelPath, routedModelStore);
  if (!routedModelStore || typeof routedModelStore !== "object") routedModelStore = { format: "mobilework-routed-models/v2", sessions: {} };
  const routedModelFormat = routedModelStore.format;
  routedModelStore.format = "mobilework-routed-models/v2";
  if (!routedModelStore.sessions || typeof routedModelStore.sessions !== "object") routedModelStore.sessions = {};
  if (routedModelFormat !== routedModelStore.format) await saveRoutedModelStore();
  await refreshNativeSnapshots();
  html = await readFile(htmlPath, "utf8");
  if (process.platform !== "linux") html = html.replace('data-view="resources"', 'data-view="resources" hidden');
  server.listen(port, host, () => console.log(`MobileWork Trace Observatory: http://${host}:${port}/`));
  startTimingCollector().catch(() => {});
}
