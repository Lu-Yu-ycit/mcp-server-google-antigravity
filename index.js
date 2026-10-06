#!/usr/bin/env node
/*
 * MCP facade.  The facade is intentionally stateless: it connects to one
 * per-user broker and forwards requests with the facade's process-stable
 * session id. Each MCP facade owns one broker connection/session, so a single
 * Named Pipe socket can never switch owners between requests.
 */
const { McpServer } = require("@modelcontextprotocol/sdk/server/mcp.js");
const { StdioServerTransport } = require("@modelcontextprotocol/sdk/server/stdio.js");
const { z } = require("zod");
const net = require("net");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const runtime = require("./broker-runtime");

const { brokerDir, endpoint, brokerSecret, version } = runtime;
const brokerScript = path.join(__dirname, "broker.js");
const sessionId = String(process.env.AGY_SESSION_ID || process.env.CODEX_SESSION_ID || "codex-" + crypto.randomBytes(10).toString("hex"));
function deriveSessionToken(targetSessionId) {
  if (process.env.AGY_SESSION_TOKEN) return String(process.env.AGY_SESSION_TOKEN);
  return crypto.createHmac("sha256", brokerSecret).update(String(targetSessionId)).digest("hex");
}
const sessionToken = deriveSessionToken(sessionId);
function ensureBrokerDir() { fs.mkdirSync(brokerDir, { recursive: true, mode: 0o700 }); try { fs.chmodSync(brokerDir, 0o700); } catch (_) {} }

function startBrokerDetached() {
  ensureBrokerDir();
  const logFile = path.join(brokerDir, "broker.log");
  let outFd;
  try { outFd = fs.openSync(logFile, "a"); } catch (_) { outFd = "ignore"; }
  const child = spawn(process.execPath, [brokerScript], {
    detached: true,
    stdio: ["ignore", outFd, outFd],
    windowsHide: true,
    env: Object.assign({}, process.env, { AGY_BROKER_DIR: brokerDir }),
  });
  child.on("error", () => {});
  child.unref();
  if (typeof outFd === "number") {
    try { fs.closeSync(outFd); } catch (_) {}
  }
}

class BrokerClient {
  constructor() {
    this.socket = null;
    this.buffer = "";
    this.nextId = 1;
    this.pending = new Map();
    this.connectPromise = null;
    this.lastBrokerStartAt = 0;
  }

  async connect() {
    if (this.socket && !this.socket.destroyed) return this.socket;
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = new Promise((resolve, reject) => {
      const socket = net.createConnection(endpoint);
      let settled = false;
      const fail = (error) => {
        if (settled) return;
        settled = true;
        try { socket.destroy(); } catch (_) {}
        reject(error);
      };
      socket.setEncoding("utf8");
      socket.setNoDelay(true);
      socket.once("connect", () => {
        if (settled) return;
        settled = true;
        this.socket = socket;
        this.attach(socket);
        resolve(socket);
      });
      socket.once("error", fail);
      socket.once("close", () => {
        if (this.socket === socket) this.socket = null;
        this.buffer = "";
        const error = Object.assign(new Error("Global MCP broker disconnected"), { code: "BROKER_DISCONNECTED" });
        for (const pending of this.pending.values()) pending.reject(error);
        this.pending.clear();
      });
    }).finally(() => { this.connectPromise = null; });
    return this.connectPromise;
  }

  attach(socket) {
    socket.on("error", (error) => {
      if (this.socket !== socket) return;
      const wrapped = Object.assign(new Error("Global MCP broker socket error: " + (error.message || error)), { code: error.code || "BROKER_SOCKET_ERROR" });
      for (const pending of this.pending.values()) pending.reject(wrapped);
      this.pending.clear();
      try { socket.destroy(); } catch (_) {}
    });
    socket.on("data", (chunk) => {
      this.buffer += chunk;
      if (this.buffer.length > 8 * 1024 * 1024) {
        this.buffer = "";
        try { socket.destroy(); } catch (_) {}
        return;
      }
      let index;
      while ((index = this.buffer.indexOf("\n")) >= 0) {
        const line = this.buffer.slice(0, index).trim();
        this.buffer = this.buffer.slice(index + 1);
        if (!line) continue;
        let response;
        try { response = JSON.parse(line); } catch (_) { continue; }
        const pending = this.pending.get(response.id);
        if (!pending) continue;
        this.pending.delete(response.id);
        if (response.ok) pending.resolve(response.result);
        else pending.reject(new Error(response.error || "Broker request failed"));
      }
    });
  }

  async call(method, params) {
    let lastError = null;
    const requestId = (method === "use" || method === "continue") ? ((params && params._request_id) || crypto.randomUUID()) : null;
    for (let attempt = 0; attempt < 8; attempt += 1) {
      try {
        await this.connect();
      } catch (error) {
        lastError = error;
        if (Date.now() - this.lastBrokerStartAt >= 1000) {
          this.lastBrokerStartAt = Date.now();
          startBrokerDetached();
        }
        await new Promise((resolve) => setTimeout(resolve, Math.min(500, 100 + attempt * 75)));
        continue;
      }
      const id = this.nextId++;
      const request = Object.assign({}, params || {}, {
        session_id: sessionId,
        session_token: sessionToken,
        _broker_token: brokerSecret,
      });
      if (params && params.workspace) {
        request.workspace = params.workspace;
      }
      if (method === "use" || method === "continue") {
        request._request_id = requestId;
      }
      try {
        const result = await new Promise((resolve, reject) => {
          const requestedWait = Math.max(0, Number(params?.wait_ms) || 0);
          const timer = setTimeout(() => { this.pending.delete(id); reject(new Error("Broker request timed out")); }, Math.max(310000, requestedWait + 10000));
          this.pending.set(id, { resolve: (value) => { clearTimeout(timer); resolve(value); }, reject: (error) => { clearTimeout(timer); reject(error); } });
          try { this.socket.write(JSON.stringify({ id, method, params: request }) + "\n"); }
          catch (error) { this.pending.delete(id); clearTimeout(timer); reject(error); }
        });
        return result;
      } catch (error) {
        lastError = error;
        const retryable = error?.code === "BROKER_DISCONNECTED" || error?.code === "BROKER_SOCKET_ERROR" || error?.code === "EPIPE" || error?.code === "ECONNRESET";
        if (retryable && attempt < 7) { await new Promise((resolve) => setTimeout(resolve, Math.min(500, 100 + attempt * 75))); continue; }
        throw error;
      }
    }
    let brokerLogTail = "";
    try {
      const logContent = fs.readFileSync(path.join(brokerDir, "broker.log"), "utf8");
      brokerLogTail = logContent.slice(-1000).trim();
    } catch (_) {}
    const errorMsg = "Unable to connect to global MCP broker" + (brokerLogTail ? ": " + brokerLogTail : "");
    throw lastError ? Object.assign(new Error(errorMsg), { cause: lastError }) : new Error(errorMsg);
  }
}

const broker = new BrokerClient();
const server = new McpServer({ name: "antigravity", version }, { capabilities: { logging: {} } });

function textResult(value) {
  return { content: [{ type: "text", text: typeof value === "string" ? value : JSON.stringify(value) }] };
}
function errorResult(error, method) {
  return { isError: true, content: [{ type: "text", text: JSON.stringify({ ok: false, error: String(error.message || error), method }) }] };
}
function withSession(params) {
  const result = Object.assign({}, params || {}, {
    session_id: sessionId,
    session_token: sessionToken,
    _workspace_explicit: Boolean(params && params.workspace),
  });
  if (params && params.workspace) {
    result.workspace = params.workspace;
  }
  return result;
}
function define(name, description, schema, method, transform) {
  server.tool(name, description, schema, async (params) => {
    try { return textResult(await broker.call(method, transform ? transform(params) : withSession(params))); }
    catch (error) { return errorResult(error, method); }
  });
}

define("use_antigravity", "Delegate a task to the global Antigravity broker. Jobs are queued centrally and bound to this isolated session.", {
  prompt: z.string().describe("The question or task to send to Antigravity"),
  thinking_depth: z.enum(["low", "high"]).optional(),
  add_dirs: z.array(z.string()).optional(),
  auto_approve: z.boolean().optional(),
  new_project: z.boolean().optional(),
  model: z.string().optional(),
  mode: z.enum(["plan", "accept-edits"]).optional(),
  agent: z.string().optional(),
  project: z.string().optional(),
  sandbox: z.boolean().optional(),
  print_timeout: z.string().optional(),
  write_to_file: z.string().optional(),
  extract: z.enum(["last_code_block"]).optional(),
  workspace: z.string().optional().describe("Session workspace; must be inside AGY_ALLOWED_ROOTS"),
}, "use", (params) => {
  const prefix = params.thinking_depth === "low" ? "Answer briefly and directly.\n\n" : params.thinking_depth === "high" ? "Think step by step very carefully before answering.\n\n" : "";
  return withSession(Object.assign({}, params, { prompt: prefix + params.prompt }));
});

define("antigravity_continue", "Continue the conversation bound to this isolated session. Global fallback to another session is disabled.", {
  prompt: z.string(),
  conversation_id: z.string().optional(),
  thinking_depth: z.enum(["low", "high"]).optional(),
  add_dirs: z.array(z.string()).optional(),
  auto_approve: z.boolean().optional(),
  model: z.string().optional(),
  agent: z.string().optional(),
  sandbox: z.boolean().optional(),
  mode: z.enum(["plan", "accept-edits"]).optional(),
  print_timeout: z.string().optional(),
  write_to_file: z.string().optional(),
  extract: z.enum(["last_code_block"]).optional(),
  workspace: z.string().optional(),
}, "continue", (params) => {
  const prefix = params.thinking_depth === "low" ? "Answer briefly and directly.\n\n" : params.thinking_depth === "high" ? "Think step by step very carefully before answering.\n\n" : "";
  return withSession(Object.assign({}, params, { prompt: prefix + params.prompt }));
});

define("antigravity_add_directory", "Approve an additional directory for this session. The path must be inside AGY_ALLOWED_ROOTS.", {
  path: z.string().describe("Directory to add to the session"),
}, "add_directory", (params) => withSession(params));

define("antigravity_remove_directory", "Remove a previously approved additional directory from this session.", {
  path: z.string().describe("Directory to remove from the session"),
}, "remove_directory", (params) => withSession(params));

define("antigravity_list_directories", "List the fixed session workspace and approved additional directories.", {
}, "list_directories", (params) => withSession(params));

define("antigravity_result", "Get the result of a centrally managed Antigravity job.", {
  jobId: z.string(),
  wait_ms: z.number().optional(),
  poll_interval_ms: z.number().optional(),
}, "result");

define("antigravity_jobs", "List centrally managed Antigravity jobs owned by this session.", {}, "jobs");
define("antigravity_sessions", "Show this session's bound conversation, configuration, and worker.", {}, "sessions");
define("antigravity_auth_retry", "Reset the broker authentication gate after interactive agy sign-in.", {}, "auth_retry");
define("antigravity_cancel", "Cancel a centrally managed Antigravity job.", { jobId: z.string() }, "cancel");
define("antigravity_cleanup", "List old job records or delete one explicitly selected terminal record.", { older_than_hours: z.number().optional(), jobId: z.string().optional(), all: z.boolean().optional() }, "cleanup");
define("antigravity_models", "List available Antigravity/Gemini models through the broker.", {}, "models");
define("antigravity_agents", "List available agy agent profiles through the broker.", {}, "agents");
define("antigravity_health", "Check the global broker, worker pool, agy CLI, and persisted job state.", {}, "health");

define("antigravity_create_folder", "Create a folder through the broker filesystem service.", { path: z.string() }, "create_folder");
define("antigravity_create_file", "Create/write a text file through the broker filesystem service.", { path: z.string(), content: z.string().optional(), overwrite: z.boolean().optional() }, "create_file");
define("antigravity_create_tree", "Create a folder/file tree through the broker filesystem service.", { base_path: z.string(), spec: z.record(z.any()) }, "create_tree");
define("antigravity_list_dir", "List a directory through the broker filesystem service.", { path: z.string() }, "list_dir");
define("antigravity_read_file", "Read a UTF-8 file through the broker filesystem service.", { path: z.string(), max_bytes: z.number().optional() }, "read_file");

const transport = new StdioServerTransport();
server.connect(transport);
