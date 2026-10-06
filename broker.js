#!/usr/bin/env node
'use strict';

const net = require('net');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawn, execFileSync } = require('child_process');
const { brokerDir: BROKER_DIR, endpoint: ENDPOINT, userHome, defaultWorkspace, isWindows, brokerSecret: BROKER_SECRET, version } = require('./broker-runtime');

const ACTIVE = new Set(['queued', 'starting', 'running']);
const TERMINAL = new Set(['done', 'error', 'cancelled']);
const integer = (value, fallback, min = 1) => Number.isFinite(Number(value)) ? Math.max(min, Math.floor(Number(value))) : fallback;
const MAX_WORKERS = integer(process.env.AGY_MAX_WORKERS || 3, 3);
const IDLE_TTL = integer(process.env.AGY_WORKER_IDLE_TTL_MS || 600000, 600000);
const START_TIMEOUT = integer(process.env.AGY_START_TIMEOUT_MS || 60000, 60000);
const COMMAND_TIMEOUT = integer(process.env.AGY_COMMAND_TIMEOUT_MS || START_TIMEOUT, START_TIMEOUT, 100);
const WATCHDOG = integer(process.env.AGY_WATCHDOG_MS || 600000, 600000);
const STOP_GRACE = integer(process.env.AGY_WORKER_STOP_GRACE_MS || 5000, 5000, 100);
const STOP_FORCE_GRACE = integer(process.env.AGY_WORKER_FORCE_GRACE_MS || 2000, 2000, 100);
const COMMAND_HARD_GRACE = integer(process.env.AGY_COMMAND_HARD_GRACE_MS || 2000, 2000, 100);
const MAX_QUEUE_PER_SESSION = integer(process.env.AGY_MAX_QUEUE_PER_SESSION || 32, 32);
const MAX_QUEUE_GLOBAL = integer(process.env.AGY_MAX_QUEUE_GLOBAL || 128, 128);
const MAX_PROMPT_CHARS = integer(process.env.AGY_MAX_PROMPT_CHARS || 200000, 200000);
const MAX_OUTPUT_CHARS = integer(process.env.AGY_MAX_OUTPUT_CHARS || 5000000, 5000000);
const JOB_RETENTION_MS = integer(process.env.AGY_JOB_RETENTION_MS || 7 * 24 * 3600000, 7 * 24 * 3600000);
const RETENTION_SWEEP_INTERVAL = integer(process.env.AGY_RETENTION_SWEEP_MS || 5 * 60 * 1000, 5 * 60 * 1000, 1000);
const RETENTION_BATCH = integer(process.env.AGY_RETENTION_BATCH || 100, 100);
const MAX_LOADED_JOBS = integer(process.env.AGY_MAX_LOADED_JOBS || 5000, 5000);
const MAX_MEMORY_TERMINAL_JOBS = integer(process.env.AGY_MAX_MEMORY_TERMINAL_JOBS || MAX_LOADED_JOBS, MAX_LOADED_JOBS);
const MAX_COMMAND_OUTPUT_CHARS = integer(process.env.AGY_MAX_COMMAND_OUTPUT_CHARS || MAX_OUTPUT_CHARS, MAX_OUTPUT_CHARS);
const MAX_CONSECUTIVE_TURNS = integer(process.env.AGY_MAX_CONSECUTIVE_TURNS || 4, 4, 1);
const MODEL_TTL = integer(process.env.AGY_MODEL_TTL_MS || process.env.AGY_MODEL_CACHE_TTL_MS || process.env.MODEL_CACHE_TTL || 300000, 300000, 0);
const HEARTBEAT = Number(process.env.AGY_HEARTBEAT_MS) === 0 ? 60000 : integer(process.env.AGY_HEARTBEAT_MS || 15000, 15000);
const JOBS_DIR = path.join(BROKER_DIR, 'jobs');
const SESSIONS_FILE = path.join(BROKER_DIR, 'sessions.json');
const LOCK_FILE = path.join(BROKER_DIR, 'broker.lock');
const now = () => new Date().toISOString();
const id = (prefix) => prefix + '_' + crypto.randomBytes(12).toString('hex');
const hash = (value) => crypto.createHash('sha256').update(value).digest('hex');
const tail = (value) => String(value || '').slice(-12000);
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const authRequired = (message) => /(?:authentication|login|sign[ -]?in) required|invalid_grant|reauthentication|credentials? (?:missing|expired|invalid|required)|no credentials|not authenticated/i.test(String(message));
const DEFAULT_MAX_READ = 5 * 1024 * 1024;
const reservedDevice = /^(CON|PRN|AUX|NUL|CONIN\$|CONOUT\$|CLOCK\$|COM[1-9]|LPT[1-9]|COM[¹²³]|LPT[¹²³])$/i;
const JOB_FILE = /^job_[a-f0-9]{24}\.json$/;

function isReservedDevice(value) {
  const normalized = String(value).replace(/[ .]+$/g, '');
  return reservedDevice.test(normalized.split('.')[0]);
}

function assertSafePathComponents(value) {
  for (const component of String(value).split(/[\\/]+/).filter(Boolean)) {
    if (component === '.' || component === '..' || /^[A-Za-z]:$/.test(component)) continue;
    if (isWindows && component.includes(':')) throw new Error('Alternate data streams are not allowed');
    if (isReservedDevice(component)) throw new Error('Reserved device name');
  }
}

// agy has emitted response text under several names across CLI versions.
// Normalize only text-bearing fields so metadata objects never become
// "[object Object]" in a completed job.
function textValue(value) {
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) return value.map(textValue).filter(Boolean).join('');
  if (!value || typeof value !== 'object') return '';
  for (const key of ['text', 'text_delta', 'response', 'output', 'content', 'message']) {
    const text = textValue(value[key]);
    if (text) return text;
  }
  return '';
}

function resultText(result, streamed) {
  for (const key of ['response', 'output', 'text', 'content', 'message']) {
    const text = textValue(result[key]);
    if (text) return text;
  }
  return String(streamed || '');
}

function read(file) { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch (_) { return null; } }
function write(file, value) {
  const temp = file + '.' + process.pid + '.tmp';
  fs.writeFileSync(temp, JSON.stringify(value));
  try { fs.renameSync(temp, file); }
  catch (error) {
    // Some Windows filesystem providers reject rename even for sibling paths.
    // Keep other persistence failures visible; EXDEV requires a non-atomic copy.
    if (error.code !== 'EXDEV') { try { fs.unlinkSync(temp); } catch (_) {} throw error; }
    fs.copyFileSync(temp, file);
    fs.unlinkSync(temp);
  }
  try { fs.chmodSync(file, 0o600); } catch (_) {}
}
function duration(value) {
  const match = String(value).match(/^(\d+)(ms|s|m|h)?$/i);
  if (!match) throw new Error('Invalid print_timeout');
  const amount = Number(match[1]);
  return amount * ({ ms: 1, s: 1000, m: 60000, h: 3600000 }[(match[2] || 'ms').toLowerCase()]);
}
function childEnv() {
  const env = { ...process.env };
  const proxyEnvFile = path.join(userHome, '.codex', '.env');
  try {
    for (const line of fs.readFileSync(proxyEnvFile, 'utf8').split(/\r?\n/)) {
      const match = line.match(/^\s*(HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|NO_PROXY)\s*=\s*["']?([^"']*)["']?\s*$/i);
      if (match && !env[match[1].toUpperCase()] && !env[match[1].toLowerCase()]) env[match[1].toUpperCase()] = match[2];
    }
  } catch (_) {}
  for (const [lower, upper] of [['http_proxy', 'HTTP_PROXY'], ['https_proxy', 'HTTPS_PROXY'], ['all_proxy', 'ALL_PROXY'], ['no_proxy', 'NO_PROXY']]) {
    if (!env[upper] && env[lower]) env[upper] = env[lower];
    if (!env[lower] && env[upper]) env[lower] = env[upper];
  }
  if (!isWindows) return { ...env, HOME: env.HOME || userHome };
  const homeRoot = path.parse(userHome).root;
  return { ...env,
    USERPROFILE: userHome,
    HOME: userHome,
    HOMEDRIVE: process.env.HOMEDRIVE || homeRoot.replace(/\\$/, ''),
    HOMEPATH: process.env.HOMEPATH || userHome.slice(homeRoot.length - 1),
    APPDATA: process.env.APPDATA || path.join(userHome, 'AppData', 'Roaming'),
    LOCALAPPDATA: process.env.LOCALAPPDATA || path.join(userHome, 'AppData', 'Local'),
    AGY_CLI_NONINTERACTIVE_HEADLESS: process.env.AGY_CLI_NONINTERACTIVE_HEADLESS || 'true',
  };
}
function findAgy() {
  if (process.env.AGY_PATH) return process.env.AGY_PATH;
  try {
    if (isWindows) {
      const output = execFileSync('where.exe', ['agy'], { encoding: 'utf8', windowsHide: true, stdio: ['ignore', 'pipe', 'ignore'] });
      return output.trim().split(/\r?\n/)[0];
    }
    return execFileSync('which', ['agy'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim().split('\n')[0];
  } catch (_) {
    throw new Error('Antigravity CLI (agy) was not found in PATH. Install it or set AGY_PATH.');
  }
}
function quoteCmdArg(value) {
  const text = String(value);
  const escaped = text.replace(/[\^&|<>%]/g, (character) => '^' + character).replace(/"/g, '""');
  return /[\s"&|<>^()%]/.test(text) ? '"' + escaped + '"' : escaped;
}
function launcher(cli, args) {
  if (isWindows && /\.(cmd|bat)$/i.test(cli)) {
    const command = [cli, ...args].map(quoteCmdArg).join(' ');
    // `call` makes cmd.exe parse a quoted batch path reliably, including paths
    // containing spaces, without enabling the global shell mode.
    return { file: process.env.ComSpec || 'cmd.exe', args: ['/d', '/s', '/c', 'call ' + command] };
  }
  return { file: cli, args };
}
function terminate(proc, force = false) {
  if (!proc || !proc.pid || proc.exitCode !== null) return Promise.resolve();
  if (!isWindows) {
    try { process.kill(-proc.pid, force ? 'SIGKILL' : 'SIGTERM'); } catch (_) { try { proc.kill(force ? 'SIGKILL' : 'SIGTERM'); } catch (__) {} }
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    let settled = false;
    const finish = () => { if (settled) return; settled = true; clearTimeout(timer); resolve(); };
    const timer = setTimeout(() => { try { proc.kill(force ? 'SIGKILL' : 'SIGTERM'); } catch (_) {} finish(); }, force ? 2000 : 1000);
    let killer;
    try {
      const args = ['/PID', String(proc.pid), '/T'];
      if (force) args.push('/F');
      killer = spawn('taskkill.exe', args, { windowsHide: true, stdio: 'ignore' });
      killer.once('close', finish); killer.once('error', finish);
    } catch (_) { try { proc.kill(force ? 'SIGKILL' : 'SIGTERM'); } catch (__) {} finish(); }
  });
}
function waitForExit(proc, timeout) {
  if (!proc || proc.exitCode !== null) return Promise.resolve();
  return new Promise((resolve) => {
    let settled = false;
    const finish = () => { if (settled) return; settled = true; clearTimeout(timer); resolve(); };
    const timer = setTimeout(finish, timeout);
    proc.once('close', finish); proc.once('error', finish);
  });
}

function canonical(value, base = process.cwd()) {
  if (typeof value !== 'string' || !value.trim()) throw new Error('Path required');
  assertSafePathComponents(value);
  const resolved = path.isAbsolute(value) ? value : path.resolve(base, value);
  const parts = []; let current = path.resolve(resolved);
  while (!fs.existsSync(current)) { const parent = path.dirname(current); if (parent === current) throw new Error('Path has no existing ancestor'); parts.unshift(path.basename(current)); current = parent; }
  return path.join(fs.realpathSync.native(current), ...parts);
}
function assertNoSymlinkComponents(value, base = process.cwd()) {
  const resolved = path.isAbsolute(value) ? path.resolve(value) : path.resolve(base, value);
  const root = path.parse(resolved).root;
  let current = root;
  for (const component of resolved.slice(root.length).split(/[\\/]+/).filter(Boolean)) {
    current = path.join(current, component);
    try { if (fs.lstatSync(current).isSymbolicLink()) throw new Error('Symbolic-link paths are not allowed'); }
    catch (error) { if (error.code !== 'ENOENT') throw error; break; }
  }
}
function within(root, target) {
  const relative = path.relative(root, target);
  return relative === '' || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative));
}

const DEFAULT_CONFIG = { model: process.env.AGY_MODEL || null, autoApprove: process.env.AGY_AUTO_APPROVE !== 'false', sandbox: process.env.AGY_SANDBOX === 'true', printTimeout: process.env.AGY_PRINT_TIMEOUT || '15m', agent: null, project: null, mode: null, addDirs: [], cwd: defaultWorkspace };
const DEFAULT_ALLOWED_ROOTS = [...new Set([userHome, process.cwd()])].filter((value) => { try { return fs.existsSync(value); } catch (_) { return false; } });
const CONFIG_FIELDS = { model: 'model', auto_approve: 'autoApprove', sandbox: 'sandbox', print_timeout: 'printTimeout', agent: 'agent', project: 'project', mode: 'mode', add_dirs: 'addDirs' };
function argsFor(config, conversation, fresh) {
  const args = ['--input-format', 'stream-json', '--output-format', 'stream-json'];
  // Plan-mode workers are non-interactive. Without this flag agy waits for a
  // permission review that the stream-json protocol cannot provide, then
  // reports SUCCESS with an empty response after denying the tool call.
  if (config.autoApprove) args.push('--dangerously-skip-permissions'); if (config.sandbox) args.push('--sandbox'); if (fresh) args.push('--new-project');
  for (const [key, flag] of [['model', '--model'], ['agent', '--agent'], ['project', '--project'], ['mode', '--mode'], ['printTimeout', '--print-timeout']]) if (config[key]) args.push(flag, config[key]);
  for (const directory of config.addDirs) args.push('--add-dir', directory); if (conversation) args.push('--conversation', conversation); return args;
}

class AuthGate {
  constructor() { this.state = 'UNKNOWN'; this.error = null; this.serial = Promise.resolve(); }
  run(action) {
    const task = this.serial.then(async () => {
      if (this.state === 'AUTH_REQUIRED') throw new Error(this.error || 'Authentication required; sign in interactively then call antigravity_auth_retry');
      this.state = 'CHECKING';
      try { const result = await action(); this.state = 'READY'; this.error = null; return result; }
      catch (error) { this.state = authRequired(error.message) ? 'AUTH_REQUIRED' : 'UNKNOWN'; this.error = error.message; throw error; }
    });
    this.serial = task.catch(() => {}); return task;
  }
  reset() { return this.serial.then(() => { this.state = 'UNKNOWN'; this.error = null; }); }
}

class Worker {
  constructor(broker, session, job) {
    this.broker = broker; this.sessionId = session.sessionId; this.generation = job.generation; this.config = job.options; this.workerId = id('worker');
    this.current = null; this.initialized = false; this.stopping = false; this.closed = false; this.buffer = ''; this.stderr = ''; this.lastUsed = Date.now(); this.readySettled = false;
    this.ready = new Promise((resolve, reject) => { this.resolveReady = resolve; this.rejectReady = reject; }); this.ready.catch(() => {}); this.exit = new Promise((resolve) => { this.resolveExit = resolve; });
  }
  settleReady(error) { if (this.readySettled) return; this.readySettled = true; clearTimeout(this.startTimer); if (error) this.rejectReady(error); else this.resolveReady(); }
  start(job) {
    const b = this.broker; const args = [...b.cliPrefix, ...argsFor(this.config, job.resumeConversation, job.fresh)]; b.assertConfig(this.config);
    const command = launcher(b.cli, args);
    this.proc = spawn(command.file, command.args, { cwd: this.config.cwd, env: childEnv(), stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true, windowsVerbatimArguments: isWindows && /\.(cmd|bat)$/i.test(b.cli), detached: !isWindows });
    job.workerPid = this.proc.pid; job.workerId = this.workerId; b.saveJob(job); this.proc.stdout.setEncoding('utf8'); this.proc.stderr.setEncoding('utf8');
    this.proc.stdout.on('data', (chunk) => this.parse(chunk)); this.proc.stderr.on('data', (chunk) => { this.stderr = tail(this.stderr + chunk); }); this.proc.stdin.on('error', (error) => this.abort(error)); this.proc.on('error', (error) => this.abort(error)); this.proc.on('close', (code) => this.onClose(code));
    this.startTimer = setTimeout(() => this.abort(new Error('Worker initialization timed out')), START_TIMEOUT); this.send(job, false); return this.ready;
  }
  send(job, warm = true) {
    this.current = job; this.lastUsed = Date.now(); this.stderr = ''; job.rawOutput = ''; job.rawChunks = []; job.rawOutputLength = 0; job.eventTypes = []; job.lastEvent = null; job.stderr = ''; if (warm) this.broker.transition(job, 'running'); const timeout = duration(job.options.printTimeout); const limit = timeout > 0 ? timeout : WATCHDOG; this.turnTimer = setTimeout(() => this.abort(new Error('Worker turn timed out')), limit);
    try { this.proc.stdin.write(JSON.stringify({ event: 'user', message: { content: job.promptText } }) + '\n'); } catch (error) { this.abort(error); }
  }
  parse(chunk) {
    this.buffer += chunk; if (this.buffer.length > 8 * 1024 * 1024) return this.abort(new Error('Oversized stream-json event')); let index;
    while ((index = this.buffer.indexOf('\n')) >= 0) { const line = this.buffer.slice(0, index).trim(); this.buffer = this.buffer.slice(index + 1); if (!line) continue; try { this.event(JSON.parse(line)); } catch (error) { return this.abort(error); } }
  }
  event(event) {
    if (this.closed || this.stopping) return; const b = this.broker; const kind = event.event || event.type || 'unknown'; const jobForDiagnostics = this.current; if (jobForDiagnostics) { jobForDiagnostics.eventTypes = [...new Set([...(jobForDiagnostics.eventTypes || []), kind])].slice(-32); jobForDiagnostics.lastEvent = { kind, keys: Object.keys(event).slice(0, 24) }; } const conversation = event.conversation_id || event.init?.conversation_id || event.step_update?.conversation_id || event.result?.conversation_id; if (conversation) b.bind(this, conversation);
    if (kind === 'init') { this.initialized = true; this.settleReady(); if (this.current?.status === 'starting') b.transition(this.current, 'running'); }
    const job = this.current; if (!job || TERMINAL.has(job.status)) return;
    if (kind === 'error') {
      const err = event.error?.message || event.error || event.message || 'agy reported an error event';
      return this.abort(new Error(err));
    }
    if (kind === 'step_update' && event.step_update?.step_type === 'agent_response') {
      const delta = textValue(event.step_update);
      if (delta) {
        const remaining = Math.max(0, MAX_OUTPUT_CHARS - (job.rawOutputLength || 0));
        if (remaining > 0) { const piece = delta.slice(0, remaining); job.rawChunks.push(piece); job.rawOutputLength += piece.length; job.partial = tail((job.partial || '') + piece); }
        if (delta.length > remaining) job.outputTruncated = true;
      }
    }
    if (kind !== 'result') return; const result = event.result || event; if (result.status !== 'SUCCESS') return this.abort(new Error(result.error || 'agy status ' + result.status)); if (!this.initialized) return this.abort(new Error('Result arrived before init'));
    this.current = null; clearTimeout(this.turnTimer); this.lastUsed = Date.now(); job.output = resultText(result, (job.rawChunks || []).join('')); if (job.output.length > MAX_OUTPUT_CHARS) { job.output = job.output.slice(0, MAX_OUTPUT_CHARS); job.outputTruncated = true; } job.partial = tail(job.output); job.rawOutput = ''; job.rawChunks = []; job.rawOutputLength = 0; job.usage = result.usage; job.bytes = Buffer.byteLength(job.output); if (result.conversation_id) job.conversationId = result.conversation_id;
    if (!job.output.trim()) {
      const denied = Array.isArray(result.denied_actions) && result.denied_actions.length ? ' Denied actions: ' + result.denied_actions.map((action) => action.display_name || action.action || String(action)).join(', ') + '.' : '';
      job.stderr = this.stderr;
      const eventHint = (job.eventTypes || []).join(', ') || 'none';
      const detail = result.error || this.stderr || `Antigravity returned SUCCESS without textual output. Events: ${eventHint}. Last event: ${JSON.stringify(job.lastEvent || null)}`;
      job.error = detail + denied;
      b.transition(job, 'error');
      b.kick();
      return;
    }
    if (job.extract === 'last_code_block') job.extracted = [...job.output.matchAll(/```[^\n]*\n([\s\S]*?)```/g)].at(-1)?.[1] ?? null;
    if (job.writeToFile) { try { b.writeFile(b.sessions.get(job.sessionId), job.writeToFile, job.output, true, job.options); job.writtenTo = job.writeToFile; } catch (error) { job.writeError = error.message; } }
    b.transition(job, 'done'); b.kick();
  }
  abort(error) {
    if (this.closed || this.stopping) return; this.settleReady(error); if (!this.initialized && authRequired(error.message)) { this.broker.auth.state = 'AUTH_REQUIRED'; this.broker.auth.error = error.message; }
    if (this.current && ACTIVE.has(this.current.status)) this.broker.finishError(this.current, error.message); this.current = null; this.stop().catch(() => {});
  }
  async stop() { if (this.stopping) return this.exit; this.stopping = true; clearTimeout(this.startTimer); clearTimeout(this.turnTimer); this.settleReady(new Error('Worker stopped')); if (this.proc) { await terminate(this.proc); await waitForExit(this.proc, STOP_GRACE); if (this.proc.exitCode === null) { await terminate(this.proc, true); await waitForExit(this.proc, STOP_FORCE_GRACE); if (this.proc.exitCode === null) this.onClose(-1); } } else this.onClose(0); return this.exit; }
  onClose(code) {
    if (this.closed) return; this.closed = true; clearTimeout(this.startTimer); clearTimeout(this.turnTimer); const message = this.stderr || 'agy worker exited with code ' + code; this.settleReady(new Error(message));
    if (!this.initialized && authRequired(message)) { this.broker.auth.state = 'AUTH_REQUIRED'; this.broker.auth.error = message; } if (this.current && ACTIVE.has(this.current.status)) this.broker.finishError(this.current, message); this.current = null; this.broker.workers.delete(this.workerId);
    const session = this.broker.sessions.get(this.sessionId); if (session?.workerId === this.workerId) session.workerId = null; this.resolveExit(); this.broker.kick();
  }
}

class Broker {
  constructor(options = {}) {
    fs.mkdirSync(BROKER_DIR, { recursive: true, mode: 0o700 }); fs.mkdirSync(JOBS_DIR, { recursive: true, mode: 0o700 }); try { fs.chmodSync(BROKER_DIR, 0o700); fs.chmodSync(JOBS_DIR, 0o700); } catch (_) {}
    this.cli = options.cli || findAgy(); this.cliPrefix = options.cliPrefix || []; this.sessions = new Map(); this.conversationOwners = new Map(); this.conversationReservations = new Map(); this.jobs = new Map(); this.workers = new Map(); this.clients = new Set(); this.auth = new AuthGate(); this.catalogs = new Map(); this.catalogPending = new Map(); this.jobWaiters = new Map(); this.scheduling = false; this.again = false; this.closing = false; this.sweepRunning = false; this.lastMaintenanceError = null;
    const configuredRoots = options.allowedRoots || JSON.parse(process.env.AGY_ALLOWED_ROOTS || JSON.stringify(DEFAULT_ALLOWED_ROOTS)); if (!Array.isArray(configuredRoots) || !configuredRoots.length) throw new Error('AGY_ALLOWED_ROOTS must be a nonempty JSON array'); this.allowedRoots = configuredRoots.map(canonical); this.load();
  }
  load() {
    const persisted = read(SESSIONS_FILE) || {};
    try { if (fs.existsSync(SESSIONS_FILE)) fs.chmodSync(SESSIONS_FILE, 0o600); } catch (_) {}
    for (const [conversation, sessionId] of Object.entries(persisted.conversationOwners || {})) this.conversationOwners.set(conversation, sessionId);
    for (const saved of persisted.sessions || []) { if (!saved.ownerHash) continue; this.sessions.set(saved.sessionId, { ...saved, queue: [], workerId: null }); if (saved.conversationId) this.conversationOwners.set(saved.conversationId, saved.sessionId); }
    let files = [];
    try { files = fs.readdirSync(JOBS_DIR).filter((name) => JOB_FILE.test(name)); } catch (error) { this.recordMaintenanceError(error); }
    files.sort((a, b) => { try { return fs.statSync(path.join(JOBS_DIR, b)).mtimeMs - fs.statSync(path.join(JOBS_DIR, a)).mtimeMs; } catch (_) { return 0; } });
    const markRestarted = (job, file) => {
      if (!job || !ACTIVE.has(job.status)) return;
      job.status = 'error'; job.error = 'Broker restarted with unfinished work; retry explicitly'; job.finishedAt = now(); job.durationMs = Date.parse(job.finishedAt) - Date.parse(job.createdAt); job.history = Array.isArray(job.history) ? job.history : []; job.history.push({ status: 'error', at: job.finishedAt });
      try { this.saveJob(job); } catch (error) { this.recordMaintenanceError(error); }
    };
    for (const file of files.slice(0, MAX_LOADED_JOBS)) { try { fs.chmodSync(path.join(JOBS_DIR, file), 0o600); } catch (_) {} const job = read(path.join(JOBS_DIR, file)); if (!job?.jobId) continue; markRestarted(job, file); this.jobs.set(job.jobId, job); }
    for (const file of files.slice(MAX_LOADED_JOBS)) { try { fs.chmodSync(path.join(JOBS_DIR, file), 0o600); } catch (_) {} markRestarted(read(path.join(JOBS_DIR, file)), file); }
  }
  persist() { write(SESSIONS_FILE, { version: 3, sessions: [...this.sessions.values()].map(({ queue, workerId, ...session }) => session), conversationOwners: Object.fromEntries(this.conversationOwners) }); }
  saveJob(job) { const persisted = { ...job }; delete persisted.promptText; delete persisted.rawOutput; delete persisted.rawChunks; write(path.join(JOBS_DIR, job.jobId + '.json'), persisted); }
  recordMaintenanceError(error) { this.lastMaintenanceError = { message: String(error?.message || error), at: now() }; }
  async maintenanceSweep(options = {}) {
    const runRetention = options.retention !== false;
    try {
      for (const worker of this.workers.values()) {
        if (!worker.current && worker.initialized && !worker.stopping && Date.now() - worker.lastUsed >= IDLE_TTL) {
          try { await worker.stop(); } catch (error) { this.recordMaintenanceError(error); }
        }
      }
      for (const job of this.jobs.values()) {
        if (!ACTIVE.has(job.status)) continue;
        job.lastHeartbeatAt = now();
        try { this.saveJob(job); } catch (error) { this.recordMaintenanceError(error); }
      }
      if (runRetention) {
        const cutoff = Date.now() - JOB_RETENTION_MS;
        const expired = new Set();
        for (const [jobId, job] of this.jobs) if (TERMINAL.has(job.status) && job.finishedAt && Date.parse(job.finishedAt) < cutoff) expired.add(jobId);
        let files = [];
        try { files = fs.readdirSync(JOBS_DIR).filter((name) => JOB_FILE.test(name)); } catch (error) { this.recordMaintenanceError(error); }
        for (const file of files) {
          const jobId = file.slice(0, -5);
          const job = this.jobs.get(jobId) || read(path.join(JOBS_DIR, file));
          if (job?.jobId === jobId && TERMINAL.has(job.status) && job.finishedAt && Date.parse(job.finishedAt) < cutoff) expired.add(jobId);
        }
        for (const jobId of [...expired].slice(0, RETENTION_BATCH)) {
          try { fs.unlinkSync(path.join(JOBS_DIR, jobId + '.json')); this.jobs.delete(jobId); } catch (error) {
            if (error.code === 'ENOENT') this.jobs.delete(jobId);
            else this.recordMaintenanceError(error);
          }
        }
      }
      try { this.kick(); } catch (error) { this.recordMaintenanceError(error); }
    } catch (error) {
      this.recordMaintenanceError(error);
    }
  }
  transition(job, state) {
    const legal = { queued: ['starting', 'error', 'cancelled'], starting: ['running', 'error', 'cancelled'], running: ['done', 'error', 'cancelled'] }; if (!legal[job.status]?.includes(state)) throw new Error('Invalid job transition ' + job.status + ' -> ' + state);
    job.status = state; job.history.push({ status: state, at: now() }); job.lastHeartbeatAt = now(); if (state === 'starting') job.startingAt = now(); if (state === 'running') job.startedAt = now();
    if (TERMINAL.has(state)) {
      this.releaseReservation(job);
      job.finishedAt = now();
      job.durationMs = Date.parse(job.finishedAt) - Date.parse(job.createdAt);
      const waiters = this.jobWaiters?.get(job.jobId);
      if (waiters) {
        for (const notify of waiters) try { notify(job); } catch (_) {}
        this.jobWaiters.delete(job.jobId);
      }
    }
    this.saveJob(job);
    if (TERMINAL.has(state)) this.pruneTerminalJobs();
  }
  pruneTerminalJobs() {
    const terminal = [...this.jobs.values()].filter((job) => TERMINAL.has(job.status));
    if (terminal.length <= MAX_MEMORY_TERMINAL_JOBS) return;
    terminal.sort((a, b) => String(a.finishedAt || a.createdAt).localeCompare(String(b.finishedAt || b.createdAt)));
    for (const job of terminal.slice(0, terminal.length - MAX_MEMORY_TERMINAL_JOBS)) this.jobs.delete(job.jobId);
  }
  finishError(job, message) { if (ACTIVE.has(job.status)) { job.error = message; this.releaseReservation(job); this.transition(job, 'error'); } }
  session(params) {
    const key = params.session_id; if (typeof key !== 'string' || !key.trim() || key.length > 256) throw new Error('session_id required'); if (typeof params.session_token !== 'string' || params.session_token.length < 16) throw new Error('session_token must contain at least 16 characters');
    const ownerHash = hash(params.session_token); let session = this.sessions.get(key); if (session && session.ownerHash !== ownerHash) throw new Error('Session ownership denied');
    if (!session) { const cwd = canonical(params.workspace || DEFAULT_CONFIG.cwd); this.operatorPath(cwd); session = { sessionId: key, ownerHash, createdAt: now(), updatedAt: now(), generation: 0, activeGeneration: 0, conversationId: null, turnsSinceYield: 0, config: { ...DEFAULT_CONFIG, addDirs: [], cwd }, activeConfig: null, queue: [], workerId: null }; this.sessions.set(key, session); this.persist(); }
    if (!Number.isFinite(session.turnsSinceYield)) session.turnsSinceYield = 0;
    if (params.workspace !== undefined && canonical(params.workspace) !== canonical(session.config.cwd)) throw new Error('Session workspace is immutable; use a new session_id');
    this.assertConfig(session.config);
    return session;
  }
  operatorPath(value, base = process.cwd()) { assertNoSymlinkComponents(value, base); const target = canonical(value, base); if (!this.allowedRoots.some((root) => within(root, target))) throw new Error('Path outside AGY_ALLOWED_ROOTS'); return target; }
  assertConfig(config) { this.operatorPath(config.cwd); for (const directory of config.addDirs) this.operatorPath(directory); }
  filePath(session, value, config = session.config) { const target = this.operatorPath(value, config.cwd); const roots = [config.cwd, ...config.addDirs].map(canonical); if (!roots.some((root) => within(root, target))) throw new Error('Path outside session workspace'); return target; }
  addDirectory(session, value) {
    const directory = this.operatorPath(value, session.config.cwd);
    if (!fs.statSync(directory).isDirectory()) throw new Error('Additional path must be a directory');
    if (canonical(session.config.cwd) === directory) return { ok: true, directory, added: false, directories: [...session.config.addDirs] };
    const added = !session.config.addDirs.includes(directory);
    if (!added) return { ok: true, directory, added: false, directories: [...session.config.addDirs] };
    session.config.addDirs = [...session.config.addDirs, directory].sort();
    session.updatedAt = now();
    this.assertConfig(session.config);
    this.persist();
    return { ok: true, directory, added, directories: [...session.config.addDirs] };
  }
  removeDirectory(session, value) {
    const directory = this.operatorPath(value, session.config.cwd);
    const before = session.config.addDirs.length;
    session.config.addDirs = session.config.addDirs.filter((candidate) => candidate !== directory);
    if (session.config.addDirs.length === before) return { ok: true, directory, removed: false, directories: [...session.config.addDirs] };
    session.updatedAt = now();
    this.persist();
    return { ok: true, directory, removed: true, directories: [...session.config.addDirs] };
  }
  directories(session) { return { workspace: session.config.cwd, addDirs: [...session.config.addDirs] }; }
  config(session, params) {
    const config = { ...session.config };
    for (const [input, field] of Object.entries(CONFIG_FIELDS)) {
      if (input === 'add_dirs') continue;
      if (params[input] !== undefined) config[field] = params[input];
    }
    if (params.add_dirs !== undefined && !Array.isArray(params.add_dirs)) throw new Error('add_dirs must be an array');
    const requestedDirs = params.add_dirs === undefined ? [] : params.add_dirs;
    config.addDirs = [...new Set([...session.config.addDirs, ...requestedDirs].map((value) => this.operatorPath(value, config.cwd)))].sort();
    duration(config.printTimeout);
    this.assertConfig(config);
    if (config.mode && !['plan', 'accept-edits'].includes(config.mode)) throw new Error('Invalid mode');
    if (config.mode === 'plan' && config.autoApprove === false) throw new Error('plan mode requires auto_approve=true');
    return config;
  }
  enqueue(session, params, kind) {
    if (typeof params.prompt !== 'string' || !params.prompt.trim()) throw new Error('prompt required'); if (params.prompt.length > MAX_PROMPT_CHARS) throw new Error('prompt exceeds maximum size'); if (params.new_project && params.conversation_id) throw new Error('new_project and conversation_id are mutually exclusive'); if (kind === 'continue' && !params.conversation_id && !session.conversationId && !session.queue.length && !session.workerId) throw new Error('No conversation bound to this session');
    if (params.workspace !== undefined && canonical(params.workspace) !== canonical(session.config.cwd)) throw new Error('Session workspace is immutable; use a new session_id');
    const requestId = typeof params._request_id === 'string' && params._request_id.length <= 128 ? params._request_id : null;
    if (requestId) {
      const existing = [...this.jobs.values()].find((candidate) => candidate.sessionId === session.sessionId && candidate.requestId === requestId && candidate.kind === kind);
      if (existing) return { jobId: existing.jobId, sessionId: existing.sessionId, status: existing.status, deduplicated: true };
      const diskFiles = (() => { try { return fs.readdirSync(JOBS_DIR).filter((name) => JOB_FILE.test(name)); } catch (_) { return []; } })();
      for (const file of diskFiles) { const candidate = read(path.join(JOBS_DIR, file)); if (candidate?.sessionId === session.sessionId && candidate.requestId === requestId && candidate.kind === kind) return { jobId: candidate.jobId, sessionId: candidate.sessionId, status: candidate.status, deduplicated: true }; }
    }
    const queuedGlobal = [...this.jobs.values()].filter((job) => job.status === 'queued').length; if (session.queue.length >= MAX_QUEUE_PER_SESSION) throw new Error('Session queue limit exceeded'); if (queuedGlobal >= MAX_QUEUE_GLOBAL) throw new Error('Global queue limit exceeded');
    const config = this.config(session, params); if (params.write_to_file) this.filePath(session, params.write_to_file, config); const explicit = params.conversation_id || null;
    if (explicit) {
      const knownOwner = this.conversationOwners.get(explicit); if (knownOwner && knownOwner !== session.sessionId) throw new Error('Conversation belongs to another session'); const reservation = this.conversationReservations.get(explicit); if (reservation && reservation.sessionId !== session.sessionId) throw new Error('Conversation belongs to another session'); for (const other of this.sessions.values()) if (other.sessionId !== session.sessionId && other.conversationId === explicit) throw new Error('Conversation belongs to another session');
      for (const otherJob of this.jobs.values()) if (otherJob.sessionId !== session.sessionId && ACTIVE.has(otherJob.status) && otherJob.explicitConversation === explicit) throw new Error('Conversation belongs to another session');
    }
    const switchConversation = !!params.new_project || (!!explicit && explicit !== session.conversationId); const generation = session.generation + (switchConversation ? 1 : 0); const job = { jobId: id('job'), sessionId: session.sessionId, status: 'queued', history: [{ status: 'queued', at: now() }], createdAt: now(), kind, requestId, prompt: params.prompt.slice(0, 300), promptText: params.prompt, options: config, generation, fresh: !!params.new_project, explicitConversation: explicit, extract: params.extract || null, writeToFile: params.write_to_file || null, partial: '', lastHeartbeatAt: now() };
    this.jobs.set(job.jobId, job); this.saveJob(job); session.generation = generation; session.config = config; session.lastJobId = job.jobId; session.updatedAt = now(); session.queue.push(job.jobId); if (explicit && !this.conversationReservations.has(explicit)) this.conversationReservations.set(explicit, { sessionId: session.sessionId, jobId: job.jobId }); this.persist(); this.kick(); return { jobId: job.jobId, sessionId: session.sessionId, status: job.status };
  }
  releaseReservation(job) { if (!job?.explicitConversation) return; const reservation = this.conversationReservations.get(job.explicitConversation); if (reservation?.jobId === job.jobId) this.conversationReservations.delete(job.explicitConversation); }
  bind(worker, conversation) { const session = this.sessions.get(worker.sessionId); if (!worker.current || session?.workerId !== worker.workerId || session.activeGeneration !== worker.generation) return; const owner = this.conversationOwners.get(conversation); if (owner && owner !== session.sessionId) return worker.abort(new Error('Conversation belongs to another session')); this.conversationOwners.set(conversation, session.sessionId); const reservation = this.conversationReservations.get(conversation); if (reservation?.jobId === worker.current.jobId) this.conversationReservations.delete(conversation); session.conversationId = conversation; session.updatedAt = now(); worker.current.conversationId = conversation; this.persist(); }
  isRunnableSession(session) {
    const job = this.jobs.get(session.queue[0]);
    if (!job || job.status !== 'queued' || this.auth.state === 'AUTH_REQUIRED') return false;
    const worker = this.workers.get(session.workerId);
    if (worker?.current || worker?.stopping) return false;
    if (worker && (worker.generation !== job.generation || JSON.stringify(worker.config) !== JSON.stringify(job.options))) return true;
    if (worker) return true;
    if (this.workers.size < MAX_WORKERS) return true;
    return [...this.workers.values()].some((candidate) => !candidate.current && candidate.initialized && !candidate.stopping);
  }
  kick() {
    if (this.closing) return; this.again = true; if (this.scheduling) return; this.scheduling = true; setImmediate(() => this.schedule().catch((error) => this.recordMaintenanceError(error)).finally(() => { this.scheduling = false; if (this.again && !this.closing) this.kick(); }));
  }
  async schedule() {
    do {
      this.again = false; const candidates = [...this.sessions.values()].filter((session) => { const job = this.jobs.get(session.queue[0]); return !!job && job.status === 'queued'; }).sort((a, b) => { const aj = this.jobs.get(a.queue[0]); const bj = this.jobs.get(b.queue[0]); const age = aj.createdAt.localeCompare(bj.createdAt); if (age) return age; const aw = this.workers.get(a.workerId); const bw = this.workers.get(b.workerId); const aWarm = aw && !aw.current && !aw.stopping ? 1 : 0; const bWarm = bw && !bw.current && !bw.stopping ? 1 : 0; return bWarm - aWarm; });
      for (const session of candidates) {
        let job = null;
        try {
          while (session.queue.length && this.jobs.get(session.queue[0])?.status !== 'queued') session.queue.shift(); job = this.jobs.get(session.queue[0]); if (!job) continue; if (this.auth.state === 'AUTH_REQUIRED') { session.queue.shift(); this.finishError(job, this.auth.error || 'Authentication required'); continue; }
          const otherRunnable = candidates.some((candidate) => candidate !== session && this.isRunnableSession(candidate)); if (otherRunnable && session.turnsSinceYield >= MAX_CONSECUTIVE_TURNS) { session.turnsSinceYield = 0; continue; }
          let worker = this.workers.get(session.workerId); if (worker?.current || worker?.stopping) continue; if (worker && (worker.generation !== job.generation || JSON.stringify(worker.config) !== JSON.stringify(job.options))) { await worker.stop(); worker = null; }
          if (!worker && this.workers.size >= MAX_WORKERS) { const victim = [...this.workers.values()].filter((candidate) => !candidate.current && candidate.initialized && !candidate.stopping).sort((a, b) => a.lastUsed - b.lastUsed)[0]; if (!victim) continue; await victim.stop(); }
          const previousSession = { activeGeneration: session.activeGeneration, conversationId: session.conversationId, activeConfig: session.activeConfig, updatedAt: session.updatedAt };
          session.queue.shift(); this.transition(job, 'starting'); if (job.generation !== session.activeGeneration) { session.activeGeneration = job.generation; session.conversationId = job.explicitConversation; } job.resumeConversation = session.conversationId; session.activeConfig = job.options; session.updatedAt = now(); this.persist();
          if (worker) { worker.send(job); session.turnsSinceYield += 1; continue; }
          try { await this.auth.run(async () => { if (this.closing || job.status !== 'starting') return; worker = new Worker(this, session, job); session.workerId = worker.workerId; this.workers.set(worker.workerId, worker); try { await worker.start(job); } catch (error) { await worker.stop(); throw error; } }); session.turnsSinceYield += 1; } catch (error) { Object.assign(session, previousSession); session.workerId = null; try { this.persist(); } catch (persistError) { this.recordMaintenanceError(persistError); } this.finishError(job, error.message); }
        } catch (error) {
          this.recordMaintenanceError(error);
          if (job && ACTIVE.has(job.status)) { job.error = 'Scheduler error: ' + error.message; try { this.finishError(job, job.error); } catch (persistError) { this.recordMaintenanceError(persistError); } }
        }
      }
    } while (this.again && !this.closing);
  }
  async catalog(kind) {
    const cached = this.catalogs.get(kind); if (this.auth.state === 'READY' && cached && Date.now() - cached.at < MODEL_TTL) return { ...cached.value, cached: true, cacheAgeMs: Date.now() - cached.at }; if (this.catalogPending.has(kind)) return this.catalogPending.get(kind);
    const task = this.auth.run(async () => { const output = await this.command([kind], { completeOnOutput: true }); const entries = [...new Set(output.split(/\r?\n/).map((line) => line.trim().replace(/^[-*•]\s*/, '')).filter((line) => line && !/:$/.test(line)))]; const value = kind === 'models' ? { output, models: entries } : kind === 'agents' ? { output, agents: entries } : { output }; this.catalogs.set(kind, { at: Date.now(), value }); return { ...value, cached: false, cacheAgeMs: 0 }; }); this.catalogPending.set(kind, task); try { return await task; } finally { this.catalogPending.delete(kind); }
  }
  command(args, options = {}) {
    return new Promise((resolve, reject) => {
      const command = launcher(this.cli, [...this.cliPrefix, ...args]);
      const proc = spawn(command.file, command.args, { stdio: ['ignore', 'pipe', 'pipe'], env: childEnv(), windowsHide: true, windowsVerbatimArguments: isWindows && /\.(cmd|bat)$/i.test(this.cli), detached: !isWindows });
      this.commandProc = proc;
      let output = ''; let errorOutput = ''; let forced = null; let settled = false; let hardTimer; let outputIdleTimer;
      let timer;
      const settle = (callback, value) => { if (settled) return; settled = true; clearTimeout(timer); clearTimeout(hardTimer); clearTimeout(outputIdleTimer); if (this.commandProc === proc) this.commandProc = null; callback(value); };
      const commandTimeout = Number.isFinite(Number(options.timeoutMs)) ? Math.max(100, Number(options.timeoutMs)) : COMMAND_TIMEOUT;
      const settleAfterOutput = () => {
        if (!options.completeOnOutput || settled || !output.trim()) return;
        clearTimeout(outputIdleTimer);
        outputIdleTimer = setTimeout(() => {
          if (settled || !output.trim()) return;
          settle(resolve, output.trim());
          terminate(proc).then(() => waitForExit(proc, STOP_GRACE)).then(async () => {
            if (proc.exitCode === null) {
              await terminate(proc, true).catch(() => {});
              await waitForExit(proc, STOP_FORCE_GRACE);
            }
          }).catch(() => {});
        }, Math.min(350, Math.max(25, commandTimeout - 25)));
      };
      const timeout = () => {
        forced = new Error('CLI command timed out');
        terminate(proc).catch(() => {});
        hardTimer = setTimeout(() => { if (!settled) { terminate(proc, true).catch(() => {}); settle(reject, forced); } }, COMMAND_HARD_GRACE);
      };
      timer = setTimeout(timeout, commandTimeout);
      proc.stdout.setEncoding('utf8'); proc.stderr.setEncoding('utf8'); proc.stdout.on('data', (chunk) => { const text = String(chunk); if (output.length + text.length > MAX_COMMAND_OUTPUT_CHARS) { output += text.slice(0, MAX_COMMAND_OUTPUT_CHARS - output.length); if (!forced) timeout(); } else output += text; settleAfterOutput(); }); proc.stderr.on('data', (chunk) => { errorOutput = tail(errorOutput + chunk); }); proc.on('error', (error) => settle(reject, error)); proc.on('close', (code) => { const outputCompleted = options.completeOnOutput && output.trim(); if (forced || (code !== 0 && !outputCompleted) || (authRequired(errorOutput + output) && !outputCompleted)) settle(reject, forced || new Error(errorOutput || output || 'CLI exited ' + code)); else settle(resolve, output.trim()); });
    });
  }
  ownedJob(session, jobId) { const job = this.jobs.get(jobId); return job?.sessionId === session.sessionId ? job : null; }
  diskJob(session, jobId) {
    if (!/^job_[a-f0-9]{24}$/.test(String(jobId))) return null;
    const job = read(path.join(JOBS_DIR, String(jobId) + '.json'));
    return job?.jobId === jobId && job.sessionId === session.sessionId ? job : null;
  }
  async result(session, params) {
    const wait = Math.min(300000, Math.max(0, Number(params.wait_ms) || 0));
    let job = this.ownedJob(session, params.jobId) || this.diskJob(session, params.jobId);
    if (!job) return { status: 'not_found', jobId: params.jobId };
    if (!ACTIVE.has(job.status) || wait === 0) return job;
    await new Promise((resolve) => {
      let timer;
      const onDone = (updatedJob) => {
        clearTimeout(timer);
        resolve(updatedJob);
      };
      timer = setTimeout(() => {
        const waiters = this.jobWaiters?.get(params.jobId);
        if (waiters) {
          waiters.delete(onDone);
          if (!waiters.size) this.jobWaiters.delete(params.jobId);
        }
        resolve();
      }, wait);
      if (!this.jobWaiters.has(params.jobId)) this.jobWaiters.set(params.jobId, new Set());
      this.jobWaiters.get(params.jobId).add(onDone);
    });
    job = this.ownedJob(session, params.jobId) || this.diskJob(session, params.jobId);
    if (!job) return { status: 'not_found', jobId: params.jobId };
    if (ACTIVE.has(job.status)) return { jobId: job.jobId, sessionId: job.sessionId, status: job.status, chars: (job.partial || '').length, tail: (job.partial || '').slice(-200), createdAt: job.createdAt, startedAt: job.startedAt };
    return job;
  }
  async cancel(session, jobId) { const job = this.ownedJob(session, jobId); if (!job) return { status: 'not_found', jobId }; if (!ACTIVE.has(job.status)) return { status: 'not_running', jobId }; this.transition(job, 'cancelled'); session.queue = session.queue.filter((key) => key !== jobId); const worker = this.workers.get(session.workerId); if (worker?.current === job) { worker.current = null; await worker.stop(); } this.kick(); return { status: 'cancelled', jobId }; }
  writeFile(session, value, content, overwrite, config = session.config) { let target = this.filePath(session, value, config); fs.mkdirSync(path.dirname(target), { recursive: true }); target = this.filePath(session, target, config); if (fs.existsSync(target) && fs.statSync(target).nlink > 1) throw new Error('Hard-linked write denied'); fs.writeFileSync(target, String(content ?? ''), { encoding: 'utf8', flag: overwrite === false ? 'wx' : 'w' }); return { ok: true, wrote: target, bytes: Buffer.byteLength(String(content ?? '')) }; }
  tree(session, base, spec) { const operations = []; const walk = (directory, entries) => { if (!entries || typeof entries !== 'object' || Array.isArray(entries)) throw new Error('Invalid tree spec'); for (const [name, value] of Object.entries(entries)) { if (!name || name === '.' || name === '..' || /[\\/:]/.test(name) || isReservedDevice(name)) throw new Error('Invalid tree entry'); const target = this.filePath(session, path.join(directory, name)); if (value === null) operations.push([target, null]); else if (typeof value === 'string') operations.push([target, value]); else if (value && typeof value === 'object') { operations.push([target, null]); walk(target, value); } else throw new Error('Invalid tree value'); } }; base = this.filePath(session, base); walk(base, spec); fs.mkdirSync(base, { recursive: true }); for (const [target, value] of operations) { if (value === null) fs.mkdirSync(this.filePath(session, target), { recursive: true }); else this.writeFile(session, target, value, true); } return { ok: true, created: operations.map(([target]) => target) }; }
  async dispatch(method, params) {
    const session = this.session(params);
    switch (method) {
      case 'use': case 'continue': return this.enqueue(session, params, method);
      case 'result': return this.result(session, params);
      case 'cancel': return this.cancel(session, params.jobId);
      case 'jobs': return [...this.jobs.values()].filter((job) => job.sessionId === session.sessionId).sort((a, b) => b.createdAt.localeCompare(a.createdAt)).map(({ promptText, rawOutput, options, ...job }) => job);
      case 'sessions': return [{ sessionId: session.sessionId, conversationId: session.conversationId, generation: session.generation, activeGeneration: session.activeGeneration, queuedJobs: session.queue.length, workerId: session.workerId, config: session.config }];
      case 'add_directory': return this.addDirectory(session, params.path);
      case 'remove_directory': return this.removeDirectory(session, params.path);
      case 'list_directories': return this.directories(session);
      case 'models': case 'agents': return this.catalog(method);
      case 'auth_retry': await this.auth.reset(); this.catalogs.clear(); this.kick(); return { state: this.auth.state };
      case 'health': return { ok: true, version, broker: { pid: process.pid, endpoint: ENDPOINT, maxWorkers: MAX_WORKERS, activeWorkers: this.workers.size, auth: { state: this.auth.state, error: this.auth.error }, idleTtlMs: IDLE_TTL, maintenanceError: this.lastMaintenanceError }, jobs: Object.fromEntries([...ACTIVE, ...TERMINAL].map((state) => [state, [...this.jobs.values()].filter((job) => job.sessionId === session.sessionId && job.status === state).length])), modelsCached: this.catalogs.get('models')?.value.models.length || 0 };
      case 'cleanup': {
        const cutoff = Date.now() - Math.max(0, Number(params.older_than_hours ?? 168)) * 3600000;
        const explicit = Boolean(params.jobId);
        const candidateMap = new Map([...this.jobs.values()]
          .filter((job) => job.sessionId === session.sessionId && TERMINAL.has(job.status) && (explicit || Date.parse(job.finishedAt) <= cutoff))
          .map((job) => [job.jobId, job]));
        let files = [];
        try { files = fs.readdirSync(JOBS_DIR).filter((name) => JOB_FILE.test(name)); } catch (error) { this.recordMaintenanceError(error); }
        for (const file of files) {
          const diskJob = read(path.join(JOBS_DIR, file));
          if (diskJob?.sessionId === session.sessionId && TERMINAL.has(diskJob.status) && (explicit || Date.parse(diskJob.finishedAt) <= cutoff)) candidateMap.set(diskJob.jobId, diskJob);
        }
        const candidates = [...candidateMap.values()];
        if (params.all) {
          let count = 0;
          for (const cand of candidates) {
            const diskPath = path.join(JOBS_DIR, cand.jobId + '.json');
            try { fs.unlinkSync(diskPath); } catch (_) {}
            this.jobs.delete(cand.jobId);
            count += 1;
          }
          return { ok: true, deleted: count, candidates: candidates.map((j) => j.jobId) };
        }
        if (!params.jobId) return { ok: true, candidates: candidates.map((job) => job.jobId), deleted: 0, hint: 'Specify one jobId to delete one terminal record, or all: true to delete candidates' };
        if (!/^job_[a-f0-9]{24}$/.test(String(params.jobId))) throw new Error('Invalid jobId');
        let job = candidates.find((candidate) => candidate.jobId === params.jobId);
        if (!job) {
          const diskPath = path.join(JOBS_DIR, params.jobId + '.json');
          const diskJob = read(diskPath);
          if (diskJob?.jobId === params.jobId && diskJob.sessionId === session.sessionId && TERMINAL.has(diskJob.status)) job = diskJob;
        }
        if (!job) throw new Error('Job not eligible for cleanup');
        const diskPath = path.join(JOBS_DIR, job.jobId + '.json');
        try { fs.unlinkSync(diskPath); } catch (error) { if (error.code !== 'ENOENT') throw error; }
        this.jobs.delete(job.jobId);
        return { ok: true, deleted: 1, jobId: job.jobId };
      }
      case 'create_folder': { const target = this.filePath(session, params.path); fs.mkdirSync(target, { recursive: true }); this.filePath(session, target); return { ok: true, created: target }; }
      case 'create_file': return this.writeFile(session, params.path, params.content, params.overwrite);
      case 'create_tree': return this.tree(session, params.base_path, params.spec);
      case 'list_dir': return { items: fs.readdirSync(this.filePath(session, params.path), { withFileTypes: true }).map((entry) => (entry.isDirectory() ? '[DIR] ' : '[FILE] ') + entry.name) };
      case 'read_file': { const target = this.filePath(session, params.path); const stat = fs.statSync(target); if (!stat.isFile()) throw new Error('Not a regular file'); const requested = params.max_bytes == null ? DEFAULT_MAX_READ : Number(params.max_bytes); if (!Number.isFinite(requested) || requested < 0) throw new Error('max_bytes must be a nonnegative number'); const limit = Math.min(stat.size, Math.floor(requested)); const buf = Buffer.alloc(limit); const fd = fs.openSync(target, 'r'); let bytesRead = 0; try { bytesRead = limit ? fs.readSync(fd, buf, 0, limit, 0) : 0; } finally { fs.closeSync(fd); } return { ok: true, path: target, bytes: stat.size, truncated: stat.size > bytesRead, content: buf.subarray(0, bytesRead).toString('utf8') }; }
      default: throw new Error('Unknown broker method: ' + method);
    }
  }
  async start() {
    this.server = net.createServer((socket) => { socket.setEncoding('utf8'); this.clients.add(socket); let buffer = ''; let owner = null; socket.on('error', () => {}); socket.on('close', () => this.clients.delete(socket)); socket.on('data', (chunk) => { buffer += chunk; if (buffer.length > 8 * 1024 * 1024) { socket.destroy(); return; } let index; while ((index = buffer.indexOf('\n')) >= 0) { const line = buffer.slice(0, index).trim(); buffer = buffer.slice(index + 1); if (!line) continue; let request; try { request = JSON.parse(line); } catch (_) { socket.destroy(); return; } const respond = (ok, result, error) => { if (!socket.destroyed) socket.write(JSON.stringify({ id: request.id, ok, result, error }) + '\n'); }; Promise.resolve().then(() => { if (request.params?._broker_token !== BROKER_SECRET) throw new Error('Broker authentication failed'); const current = this.session(request.params || {}); if (owner && owner !== current.ownerHash) throw new Error('Connection owner cannot change'); owner = current.ownerHash; return this.dispatch(request.method, request.params); }).then((result) => respond(true, result), (error) => respond(false, undefined, error.message)); } }); });
    await new Promise((resolve, reject) => { this.server.once('error', reject); this.server.listen(ENDPOINT, resolve); });
    const runSweep = (retention) => { if (this.sweepRunning) return; this.sweepRunning = true; Promise.resolve(this.maintenanceSweep({ retention })).catch((error) => this.recordMaintenanceError(error)).finally(() => { this.sweepRunning = false; }); };
    this.sweep = setInterval(() => runSweep(false), Math.min(HEARTBEAT, Math.max(10, Math.floor(IDLE_TTL / 2))));
    this.retentionSweep = setInterval(() => runSweep(true), RETENTION_SWEEP_INTERVAL);
    this.sweep.unref(); this.retentionSweep.unref(); return this;
  }
  async shutdown() { this.closing = true; clearInterval(this.sweep); clearInterval(this.retentionSweep); for (const job of this.jobs.values()) if (ACTIVE.has(job.status)) this.finishError(job, 'Broker shutdown'); await terminate(this.commandProc, true); await Promise.all([...this.workers.values()].map((worker) => worker.stop())); for (const socket of this.clients) socket.destroy(); if (this.server) { await new Promise((resolve) => this.server.close(resolve)); if (!isWindows) { try { fs.unlinkSync(ENDPOINT); } catch (_) {} } } this.persist(); }
}

function alive(pid) { try { process.kill(pid, 0); return true; } catch (error) { return error.code === 'EPERM'; } }
async function runBroker() {
  fs.mkdirSync(BROKER_DIR, { recursive: true });
  const checkPipe = () => new Promise((resolve) => {
    const socket = net.createConnection(ENDPOINT);
    const finish = (active) => { try { socket.destroy(); } catch (_) {} resolve(active); };
    socket.once('connect', () => finish(true));
    socket.once('error', () => finish(false));
  });
  if (await checkPipe()) return;
  let lock;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      lock = fs.openSync(LOCK_FILE, 'wx');
      fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, startedAt: Date.now() }));
      try { fs.fsyncSync(lock); } catch (_) {}
      break;
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      const old = read(LOCK_FILE);
      let lockMtime = 0;
      try { lockMtime = fs.statSync(LOCK_FILE).mtimeMs; } catch (_) {}
      const isRecent = (Date.now() - lockMtime) < 3000;
      if (old?.pid && alive(old.pid)) {
        await delay(500);
        if (await checkPipe()) return;
        continue;
      }
      if (isRecent) { await delay(500); continue; }
      try { fs.unlinkSync(LOCK_FILE); } catch (unlinkError) { if (unlinkError.code !== 'ENOENT') throw unlinkError; }
    }
  }
  if (!lock) return;
  if (!isWindows) { try { fs.unlinkSync(ENDPOINT); } catch (error) { if (error.code !== 'ENOENT') throw error; } }
  fs.closeSync(lock);
  const release = () => { try { if (read(LOCK_FILE)?.pid === process.pid) fs.unlinkSync(LOCK_FILE); } catch (_) {} };
  process.on('exit', release);
  if (!isWindows && fs.existsSync(ENDPOINT)) fs.unlinkSync(ENDPOINT);
  const broker = new Broker();
  await broker.start();
  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    await broker.shutdown();
    release();
    process.exit(0);
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}
if (require.main === module) runBroker().catch((error) => { console.error(error); process.exitCode = 1; });
module.exports = { Broker, AuthGate, ENDPOINT, BROKER_DIR, runBroker };
