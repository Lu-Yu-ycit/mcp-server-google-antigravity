const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), 'agy-broker-test-')));
process.env.AGY_BROKER_DIR = path.join(root, 'broker');
process.env.AGY_MAX_WORKERS = '3';
process.env.AGY_WORKER_IDLE_TTL_MS = '100';
process.env.AGY_WORKER_STOP_GRACE_MS = '100';
process.env.AGY_WORKER_FORCE_GRACE_MS = '100';
process.env.AGY_COMMAND_TIMEOUT_MS = '100';
process.env.AGY_COMMAND_HARD_GRACE_MS = '100';
process.env.AGY_RETENTION_BATCH = '2';
process.env.AGY_MAX_OUTPUT_CHARS = '100';
const { Broker, AuthGate, ENDPOINT } = require('../broker');

const fakeCli = path.join(__dirname, '..', 'fixtures', 'fake-agy.js');
const tokenA = 'session-token-a-123456789';
const tokenB = 'session-token-b-123456789';
const workspace = root;

function params(session_id, session_token, extra = {}) {
  return { session_id, session_token, workspace, ...extra };
}

async function waitFor(broker, session_id, session_token, jobId, requestWorkspace = workspace) {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const result = await broker.dispatch('result', params(session_id, session_token, { workspace: requestWorkspace, jobId, wait_ms: 50 }));
    if (result.status === 'done' || result.status === 'error' || result.status === 'cancelled' || result.status === 'not_found') return result;
  }
  throw new Error('job did not reach a terminal state: ' + jobId);
}

test('job lifecycle, ownership, persistence, catalog cache, and worker eviction', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    const first = await broker.dispatch('use', params('session-a', tokenA, { prompt: 'hello' }));
    assert.equal(first.status, 'queued');
    const completed = await waitFor(broker, 'session-a', tokenA, first.jobId);
    assert.equal(completed.status, 'done');
    assert.deepEqual(completed.history.map((entry) => entry.status), ['queued', 'starting', 'running', 'done']);

    for (const [prompt, expected] of [['output-field', 'fake:output-field'], ['content-field', 'fake:content-field'], ['step-field', 'fake:step-field']]) {
      const alternate = await broker.dispatch('use', params('session-a', tokenA, { prompt }));
      const alternateResult = await waitFor(broker, 'session-a', tokenA, alternate.jobId);
      assert.equal(alternateResult.status, 'done');
      assert.equal(alternateResult.output, expected);
      assert.equal(alternateResult.bytes, Buffer.byteLength(expected));
    }

    const empty = await broker.dispatch('use', params('session-a', tokenA, { prompt: 'empty-result' }));
    const emptyResult = await waitFor(broker, 'session-a', tokenA, empty.jobId);
    assert.equal(emptyResult.status, 'error');
    assert.match(emptyResult.error, /Denied actions: RunCommand/);

    const denied = await broker.dispatch('result', params('session-z', tokenB, { jobId: first.jobId }));
    assert.equal(denied.status, 'not_found');

    const continued = await broker.dispatch('continue', params('session-a', tokenA, { prompt: 'follow-up' }));
    const continuedResult = await waitFor(broker, 'session-a', tokenA, continued.jobId);
    assert.equal(continuedResult.status, 'done');
    assert.equal(continuedResult.conversationId, completed.conversationId);

    const catalog1 = await broker.dispatch('models', params('session-a', tokenA));
    const catalog2 = await broker.dispatch('models', params('session-a', tokenA));
    assert.equal(catalog1.cached, false);
    assert.equal(catalog2.cached, true);

    const reserved = await broker.dispatch('use', params('session-race-a', 'session-race-a-token-123456', { prompt: 'slow reservation', conversation_id: 'shared-conversation' }));
    assert.equal(broker.conversationOwners.has('shared-conversation'), false);
    assert.equal(broker.conversationReservations.get('shared-conversation').jobId, reserved.jobId);
    await assert.rejects(() => broker.dispatch('use', params('session-race-b', 'session-race-b-token-123456', { prompt: 'should be denied', conversation_id: 'shared-conversation' })), /belongs to another session/);
    await broker.dispatch('cancel', params('session-race-a', 'session-race-a-token-123456', { jobId: reserved.jobId }));
    assert.equal(broker.conversationReservations.has('shared-conversation'), false);

    const streamed = await broker.dispatch('use', params('stream-session', tokenB, { prompt: 'stream-large' }));
    const streamedResult = await waitFor(broker, 'stream-session', tokenB, streamed.jobId);
    assert.equal(streamedResult.status, 'done');
    assert.equal(streamedResult.outputTruncated, true);

    const jobs = await Promise.all(['b', 'c', 'd', 'e'].map((name) => broker.dispatch('use', params('session-' + name, tokenA + name, { prompt: 'slow-' + name }))));
    assert.ok(jobs.some((job) => job.status === 'queued'));
    for (const job of jobs) {
      const result = await waitFor(broker, job.sessionId, tokenA + job.sessionId.slice(-1), job.jobId);
      assert.equal(result.status, 'done');
    }

    // Windows taskkill can delay the close event beyond the idle TTL.
    const evictionDeadline = Date.now() + 5000;
    while (broker.workers.size && Date.now() < evictionDeadline) await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(broker.workers.size, 0);

    const persisted = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
    const restored = persisted.sessions.get('session-a');
    assert.equal(restored.config.cwd, workspace);
    assert.equal(restored.conversationId, continuedResult.conversationId);
    const cleanupJob = await broker.dispatch('use', params('cleanup-session', tokenB, { prompt: 'cleanup-me' }));
    const cleanupResult = await waitFor(broker, 'cleanup-session', tokenB, cleanupJob.jobId);
    assert.equal(cleanupResult.status, 'done');
    broker.jobs.delete(cleanupJob.jobId);
    const cleaned = await broker.dispatch('cleanup', params('cleanup-session', tokenB, { jobId: cleanupJob.jobId }));
    assert.equal(cleaned.deleted, 1);
    assert.equal(fs.existsSync(path.join(process.env.AGY_BROKER_DIR, 'jobs', cleanupJob.jobId + '.json')), false);
    broker.jobs.delete(continued.jobId);
    const diskResult = await broker.dispatch('result', params('session-a', tokenA, { jobId: continued.jobId }));
    assert.equal(diskResult.status, 'done');
    await persisted.shutdown();
  } finally {
    await broker.shutdown();
  }
});

test('command timeout is bounded and Windows launcher remains explicit', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  try {
    const started = Date.now();
    await assert.rejects(() => broker.command(['models', '--hang']), /CLI command timed out/);
    assert.ok(Date.now() - started < 2000);
    if (process.platform === 'win32') {
      const spaced = fs.mkdtempSync(path.join(root, 'space dir-'));
      fs.copyFileSync(path.join(__dirname, '..', 'fixtures', 'fake-agy.js'), path.join(spaced, 'fake agy.js'));
      fs.writeFileSync(path.join(spaced, 'fake agy.cmd'), '@echo off\r\nnode "%~dp0fake agy.js" %*\r\n');
      const wrapped = new Broker({ cli: path.join(spaced, 'fake agy.cmd'), allowedRoots: [root] });
      assert.deepEqual(await wrapped.dispatch('models', params('cmd-wrapper', tokenA)), { output: 'Gemini Fake Flash\nGemini Fake Pro', models: ['Gemini Fake Flash', 'Gemini Fake Pro'], cached: false, cacheAgeMs: 0 });
    }
  } finally {
  }
});

test('catalog command may complete after output when CLI keeps a background process alive', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  const started = Date.now();
  const output = await broker.command(['models', '--linger'], { completeOnOutput: true, timeoutMs: 1000 });
  assert.equal(output, 'Gemini Fake Flash\nGemini Fake Pro');
  assert.ok(Date.now() - started < 1500);
});

test('catalog output wins over transient auth warnings emitted on stderr', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  const output = await broker.command(['models', '--warning'], { completeOnOutput: true, timeoutMs: 1000 });
  assert.equal(output, 'Gemini Fake Flash\nGemini Fake Pro');
});

test('restart marks disk active jobs and scheduler isolates one session failure', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  const staleId = 'job_' + 'c'.repeat(24);
  const stalePath = path.join(process.env.AGY_BROKER_DIR, 'jobs', staleId + '.json');
  fs.writeFileSync(stalePath, JSON.stringify({ jobId: staleId, sessionId: 'stale-session', status: 'running', createdAt: new Date().toISOString(), history: [] }));
  const restored = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  assert.equal(restored.jobs.get(staleId).status, 'error');
  await broker.start();
  try {
    const failing = await broker.dispatch('use', params('scheduler-failure', tokenA, { prompt: 'slow-failure' }));
    const healthy = await broker.dispatch('use', params('scheduler-healthy', tokenB, { prompt: 'hello' }));
    const originalPersist = broker.persist.bind(broker);
    broker.persist = () => { if (broker.jobs.get(failing.jobId)?.status === 'starting') throw new Error('session persistence failed'); return originalPersist(); };
    const failedResult = await waitFor(broker, 'scheduler-failure', tokenA, failing.jobId);
    const healthyResult = await waitFor(broker, 'scheduler-healthy', tokenB, healthy.jobId);
    assert.equal(failedResult.status, 'error');
    assert.equal(healthyResult.status, 'done');
  } finally {
    await broker.shutdown();
  }
});

test('filesystem roots reject paths outside the session workspace', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    await assert.rejects(() => broker.dispatch('read_file', params('session-fs', tokenB, { path: path.dirname(root) })), /outside/);
  } finally {
    await broker.shutdown();
  }
});

test('filesystem helpers reject symlink path components', async () => {
  const link = path.join(root, 'workspace-link');
  try { fs.symlinkSync(root, link, 'junction'); } catch (_) { return; }
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    await assert.rejects(() => broker.dispatch('read_file', params('symlink-session', tokenA, { path: path.join('workspace-link', 'large.txt') })), /Symbolic-link paths/);
  } finally {
    await broker.shutdown();
  }
});

test('workspace is immutable and prompt/queue bounds are enforced', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    await broker.dispatch('health', params('immutable-session', tokenA));
    await assert.rejects(() => broker.dispatch('use', params('immutable-session', tokenA, { workspace: path.join(root, 'other'), _workspace_explicit: true, prompt: 'change workspace' })), /workspace is immutable/);
    await assert.rejects(() => broker.dispatch('use', params('bounded-session', tokenB, { prompt: 'x'.repeat(200001) })), /prompt exceeds maximum size/);
  } finally {
    await broker.shutdown();
  }
});

test('filesystem helpers support workspace-relative reads and bounded memory reads', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    fs.writeFileSync(path.join(root, 'large.txt'), '0123456789'.repeat(1000));
    const read = await broker.dispatch('read_file', params('relative-fs', tokenA, { path: 'large.txt', max_bytes: 7 }));
    assert.equal(read.content, '0123456');
    assert.equal(read.bytes, 10000);
    assert.equal(read.truncated, true);
    if (process.platform === 'win32') await assert.rejects(() => broker.dispatch('read_file', params('relative-fs', tokenA, { path: 'large.txt:secret', max_bytes: 7 })), /Alternate data streams/);
    await assert.rejects(() => broker.dispatch('create_tree', params('relative-fs', tokenA, { base_path: '.', spec: { CON: 'blocked' } })), /Invalid tree entry/);
  } finally {
    await broker.shutdown();
  }
});

test('request retries are idempotent for queued jobs', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    const request = params('idempotent-session', tokenA, { prompt: 'slow-idempotent', _request_id: 'request-idempotent-1' });
    const first = await broker.dispatch('use', request);
    const duplicate = await broker.dispatch('use', request);
    assert.equal(duplicate.jobId, first.jobId);
    assert.equal(duplicate.deduplicated, true);
    const result = await waitFor(broker, 'idempotent-session', tokenA, first.jobId);
    assert.equal(result.status, 'done');
  } finally {
    await broker.shutdown();
  }
});

test('broker socket requires the per-user master token', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    const response = await new Promise((resolve, reject) => {
      const socket = require('net').createConnection(ENDPOINT);
      let buffer = '';
      socket.setEncoding('utf8');
      socket.on('data', (chunk) => { buffer += chunk; const index = buffer.indexOf('\n'); if (index >= 0) { resolve(JSON.parse(buffer.slice(0, index))); socket.destroy(); } });
      socket.on('error', reject);
      socket.on('connect', () => socket.write(JSON.stringify({ id: 1, method: 'health', params: {} }) + '\n'));
    });
    assert.equal(response.ok, false);
    assert.match(response.error, /Broker authentication failed/);
  } finally {
    await broker.shutdown();
  }
});

test('AuthGate serializes cold starts and exposes retryable auth state', async () => {
  const gate = new AuthGate();
  let active = 0;
  let peak = 0;
  const coldStart = () => gate.run(async () => {
    active += 1;
    peak = Math.max(peak, active);
    await new Promise((resolve) => setTimeout(resolve, 15));
    active -= 1;
    return 'ready';
  });
  assert.deepEqual(await Promise.all([coldStart(), coldStart()]), ['ready', 'ready']);
  assert.equal(peak, 1);
  await assert.rejects(() => gate.run(async () => { throw new Error('authentication required'); }), /authentication required/);
  assert.equal(gate.state, 'AUTH_REQUIRED');
  await gate.reset();
  assert.equal(gate.state, 'UNKNOWN');
});

test('OAuth failure gates other projects and an explicit retry restores isolated workers', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  const projectA = path.join(root, 'project-a');
  const projectB = path.join(root, 'project-b');
  fs.mkdirSync(projectA, { recursive: true });
  fs.mkdirSync(projectB, { recursive: true });
  const a = params('auth-project-a', tokenA, { workspace: projectA });
  const b = params('auth-project-b', tokenB, { workspace: projectB });
  await broker.start();
  try {
    const failed = await broker.dispatch('use', { ...a, prompt: 'auth-failure' });
    assert.match((await waitFor(broker, a.session_id, tokenA, failed.jobId, projectA)).error, /authentication failed or timed out/);
    assert.equal((await broker.dispatch('health', b)).broker.auth.state, 'AUTH_REQUIRED');
    const blocked = await broker.dispatch('use', { ...b, prompt: 'hello' });
    const blockedResult = await waitFor(broker, b.session_id, tokenB, blocked.jobId, projectB);
    assert.equal(blockedResult.status, 'error');
    assert.equal(blockedResult.workerId, undefined);
    assert.deepEqual(await broker.dispatch('auth_retry', b), { state: 'UNKNOWN' });
    const jobA = await broker.dispatch('use', { ...a, prompt: 'hello-a' });
    const jobB = await broker.dispatch('use', { ...b, prompt: 'hello-b' });
    const resultA = await waitFor(broker, a.session_id, tokenA, jobA.jobId, projectA);
    const resultB = await waitFor(broker, b.session_id, tokenB, jobB.jobId, projectB);
    assert.equal(resultA.status, 'done');
    assert.equal(resultB.status, 'done');
    assert.notEqual(resultA.conversationId, resultB.conversationId);
    assert.equal(resultA.options.cwd, projectA);
    assert.equal(resultB.options.cwd, projectB);
  } finally {
    await broker.shutdown();
  }
});

test('session persistence handles EXDEV without hiding other rename failures', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  const rename = fs.renameSync;
  try {
    fs.renameSync = () => { throw Object.assign(new Error('cross-device rename'), { code: 'EXDEV' }); };
    await broker.dispatch('health', params('exdev-session', tokenA));
    const sessionsFile = path.join(process.env.AGY_BROKER_DIR, 'sessions.json');
    assert.ok(JSON.parse(fs.readFileSync(sessionsFile, 'utf8')).sessions.some((s) => s.sessionId === 'exdev-session'));
    assert.equal(fs.existsSync(sessionsFile + '.' + process.pid + '.tmp'), false);
    fs.renameSync = () => { throw Object.assign(new Error('permission denied'), { code: 'EACCES' }); };
    assert.throws(() => broker.persist(), /permission denied/);
  } finally {
    fs.renameSync = rename;
    await broker.shutdown();
  }
});

test('failure paths reject unsafe config and preserve broker maintenance', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    await assert.rejects(() => broker.dispatch('use', params('plan-invalid', tokenA, { prompt: 'plan', mode: 'plan', auto_approve: false })), /plan mode requires/);
    await broker.dispatch('health', params('workspace-invariant', tokenA));
    await assert.rejects(() => broker.dispatch('health', params('workspace-invariant', tokenA, { workspace: path.join(root, 'other') })), /workspace is immutable/);
    await assert.rejects(() => broker.dispatch('create_file', params('reserved-components', tokenA, { path: path.join('nested', 'CON.txt'), content: 'blocked' })), /Reserved device name/);

    const oldJobId = 'job_' + 'a'.repeat(24);
    const oldJobPath = path.join(process.env.AGY_BROKER_DIR, 'jobs', oldJobId + '.json');
    fs.writeFileSync(oldJobPath, JSON.stringify({ jobId: oldJobId, sessionId: 'maintenance-session', status: 'done', finishedAt: new Date(0).toISOString() }));
    await broker.dispatch('health', params('maintenance-session', tokenB));
    broker.jobs.delete(oldJobId);
    await broker.maintenanceSweep();
    assert.equal(fs.existsSync(oldJobPath), false);

    const originalSaveJob = broker.saveJob;
    broker.jobs.set('job_' + 'b'.repeat(24), { jobId: 'job_' + 'b'.repeat(24), sessionId: 'maintenance-session', status: 'running', createdAt: new Date().toISOString(), history: [] });
    broker.saveJob = () => { throw new Error('simulated maintenance write failure'); };
    await assert.doesNotReject(() => broker.maintenanceSweep());
    assert.match(broker.lastMaintenanceError.message, /simulated maintenance write failure/);
    broker.saveJob = originalSaveJob;
  } finally {
    await broker.shutdown();
  }
});

test('error event triggers immediate job error instead of hanging', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    const job = await broker.dispatch('use', params('error-session', tokenA, { prompt: 'trigger-error-event' }));
    const result = await waitFor(broker, 'error-session', tokenA, job.jobId);
    assert.equal(result.status, 'error');
    assert.match(result.error, /Quota exceeded from fake agy/);
  } finally {
    await broker.shutdown();
  }
});

test('cleanup batch with all: true cleans all terminal records', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    const job1 = await broker.dispatch('use', params('batch-cleanup-session', tokenA, { prompt: 'hello-1' }));
    const job2 = await broker.dispatch('use', params('batch-cleanup-session', tokenA, { prompt: 'hello-2' }));
    await waitFor(broker, 'batch-cleanup-session', tokenA, job1.jobId);
    await waitFor(broker, 'batch-cleanup-session', tokenA, job2.jobId);
    const cleaned = await broker.dispatch('cleanup', params('batch-cleanup-session', tokenA, { all: true, older_than_hours: 0 }));
    assert.ok(cleaned.ok);
    assert.ok(cleaned.deleted >= 2);
  } finally {
    await broker.shutdown();
  }
});

test('cleanup batch scans terminal records that were evicted from memory', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  await broker.start();
  try {
    const job = await broker.dispatch('use', params('disk-cleanup-session', tokenA, { prompt: 'disk-only-cleanup' }));
    assert.equal((await waitFor(broker, 'disk-cleanup-session', tokenA, job.jobId)).status, 'done');
    broker.jobs.delete(job.jobId);
    const cleaned = await broker.dispatch('cleanup', params('disk-cleanup-session', tokenA, { all: true, older_than_hours: 0 }));
    assert.equal(cleaned.deleted, 1);
    assert.equal(fs.existsSync(path.join(process.env.AGY_BROKER_DIR, 'jobs', job.jobId + '.json')), false);
  } finally {
    await broker.shutdown();
  }
});

test('workspace inheritance permits subsequent file operations without repeating workspace', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  const subWorkspace = path.join(root, 'sub-project');
  fs.mkdirSync(subWorkspace, { recursive: true });
  fs.writeFileSync(path.join(subWorkspace, 'hello.txt'), 'sub project content');
  await broker.start();
  try {
    await broker.dispatch('use', params('sub-session', tokenA, { workspace: subWorkspace, prompt: 'hello' }));
    const read = await broker.dispatch('read_file', { session_id: 'sub-session', session_token: tokenA, path: 'hello.txt' });
    assert.equal(read.content, 'sub project content');
  } finally {
    await broker.shutdown();
  }
});

test('session directories can be added, listed, persisted, and removed', async () => {
  const broker = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
  const extra = path.join(root, 'extra-repo');
  const workspaceDir = path.join(root, 'primary-repo');
  fs.mkdirSync(workspaceDir, { recursive: true });
  fs.mkdirSync(extra, { recursive: true });
  fs.writeFileSync(path.join(extra, 'hello.txt'), 'extra content');
  await broker.start();
  try {
    const session = 'directory-session';
    const added = await broker.dispatch('add_directory', params(session, tokenA, { workspace: workspaceDir, path: extra }));
    assert.equal(added.added, true);
    assert.deepEqual(added.directories, [extra]);
    assert.deepEqual(await broker.dispatch('list_directories', params(session, tokenA, { workspace: workspaceDir })), { workspace: workspaceDir, addDirs: [extra] });
    const read = await broker.dispatch('read_file', { session_id: session, session_token: tokenA, path: path.join(extra, 'hello.txt') });
    assert.equal(read.content, 'extra content');
    const taskExtra = path.join(root, 'task-extra');
    fs.mkdirSync(taskExtra, { recursive: true });
    const job = await broker.dispatch('use', params(session, tokenA, { workspace: workspaceDir, add_dirs: [taskExtra], prompt: 'hello' }));
    assert.equal((await waitFor(broker, session, tokenA, job.jobId, workspaceDir)).status, 'done');
    assert.deepEqual((await broker.dispatch('list_directories', params(session, tokenA, { workspace: workspaceDir }))).addDirs, [extra, taskExtra]);
    const restored = new Broker({ cli: process.execPath, cliPrefix: [fakeCli], allowedRoots: [root] });
    assert.deepEqual(restored.sessions.get(session).config.addDirs, [extra, taskExtra]);
    await restored.shutdown();
    const removed = await broker.dispatch('remove_directory', params(session, tokenA, { workspace: workspaceDir, path: extra }));
    assert.equal(removed.removed, true);
    assert.deepEqual(removed.directories, [taskExtra]);
    await assert.rejects(() => broker.dispatch('read_file', { session_id: session, session_token: tokenA, path: path.join(extra, 'hello.txt') }), /outside session workspace/);
  } finally {
    await broker.shutdown();
  }
});
