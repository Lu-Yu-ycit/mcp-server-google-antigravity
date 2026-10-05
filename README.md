# MCP Server Google Antigravity

[![CI](https://github.com/TurkerYakup/mcp-server-google-antigravity/actions/workflows/ci.yml/badge.svg)](https://github.com/TurkerYakup/mcp-server-google-antigravity/actions/workflows/ci.yml)
[![npm version](https://badge.fury.io/js/mcp-server-google-antigravity.svg)](https://www.npmjs.com/package/mcp-server-google-antigravity)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Model Context Protocol (MCP) server providing an MCP facade and per-user global Broker for Google Antigravity (`agy` CLI / Gemini). It provides session isolation, bounded persistent worker pools, centrally managed jobs, codebase analysis, and safe workspace-scoped filesystem tools.

---

## Architecture Overview

```
                      +-----------------------------+
                      |   MCP Clients (IDE / CLI)   |
                      +--------------+--------------+
                                     | (Stdio JSON-RPC)
                      +--------------v--------------+
                      |      index.js (Facade)      |
                      +--------------+--------------+
                                     | (Named Pipe / Unix Domain Socket)
                      +--------------v--------------+
                      |       Global Broker         |
                      |         (broker.js)         |
                      +-------+--------------+------+
                              |              |
                      +-------v------+ +-----v------+
                      | Sessions Map | | Job Queue  |
                      +-------+------+ +-----+------+
                              |              |
                      +-------v--------------v------+
                      |   Worker Pool (1..N agy)   |
                      +-----------------------------+
```

1. **MCP Facade (`index.js`)**: Connects to the per-user background broker. Forwards requests with an explicit session id, allowing multiple projects/IDE instances to share a single background broker and OAuth state without sharing conversations.
2. **Global Broker (`broker.js`)**: Central background daemon managing job scheduling, worker pooling (stream-json), session lifecycle, file sandboxing, and job persistence.
3. **Workers**: Persistent child processes running `agy` in `stream-json` input/output format, warm-reused across consecutive turns to minimize startup latency.

---

## Installation

Ensure you have [Google Antigravity CLI (`agy`)](https://github.com/google/antigravity) installed and in your `PATH`, then install via npm:

```bash
npm install -g mcp-server-google-antigravity
```

Or run directly using `npx`:

```bash
npx mcp-server-google-antigravity
```

---

## Configuration

Add the server to your MCP client configuration (e.g., `claude_desktop_config.json`, Cursor, or Codex):

```json
{
  "mcpServers": {
    "antigravity": {
      "command": "npx",
      "args": ["-y", "mcp-server-google-antigravity"],
      "env": {
        "AGY_WORKSPACE": "/path/to/your/project"
      }
    }
  }
}
```

### Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `AGY_PATH` | (auto-discovered) | Path to the `agy` executable (or `.cmd` on Windows). |
| `AGY_BROKER_DIR` | `%LOCALAPPDATA%\mcp-server-google-antigravity` (Win) or `~/.local/state/mcp-server-google-antigravity` (POSIX) | State and jobs directory. |
| `AGY_ALLOWED_ROOTS` | `[process.cwd(), userHome]` | JSON array of allowed directory paths for workspace operations. |
| `AGY_MAX_WORKERS` | `3` | Maximum concurrent persistent CLI workers. |
| `AGY_WORKER_IDLE_TTL_MS` | `600000` (10m) | Idle duration before an unused worker is evicted. |
| `AGY_AUTO_APPROVE` | `true` | Automatically approve non-interactive tool operations via `--dangerously-skip-permissions`. |
| `AGY_SESSION_ID` | (auto-generated) | Logical session identifier. |
| `AGY_SESSION_TOKEN` | (HMAC-derived) | Session ownership token for isolation. |
| `AGY_WORKSPACE` | `process.cwd()` | Initial fixed workspace for newly created sessions. Existing sessions keep their workspace. |

---

## Available MCP Tools

### Task & Job Management
- **`use_antigravity`**: Delegate a prompt or task to the global broker. Queues a job bound to the current session and returns `{ jobId, status }`.
- **`antigravity_continue`**: Continue the conversation bound to this session with a follow-up prompt.
- **`antigravity_result`**: Fetch results of a job (supports long-polling via `wait_ms`).
- **`antigravity_cancel`**: Cancel an active or queued job.
- **`antigravity_jobs`**: List jobs belonging to the current session.
- **`antigravity_cleanup`**: Clean up finished jobs (`{ jobId }` or `{ all: true }`).

### Environment & Diagnostics
- **`antigravity_health`**: Inspect broker daemon status, active workers, and queue metrics.
- **`antigravity_models`**: Query available Gemini/Antigravity models.
- **`antigravity_agents`**: Query available agent profiles.
- **`antigravity_auth_retry`**: Reset authentication gate after logging into `agy`.

### Sandboxed Filesystem Tools
- **`antigravity_read_file`**: Read files bounded by workspace roots and byte limits.
- **`antigravity_create_file`**: Write/create files within workspace bounds.
- **`antigravity_create_folder`**: Create folders within workspace bounds.
- **`antigravity_create_tree`**: Create file and folder trees.
- **`antigravity_list_dir`**: List directory contents.
- **`antigravity_add_directory`**: Persistently approve an additional directory for the current session. The directory must be inside `AGY_ALLOWED_ROOTS`.
- **`antigravity_remove_directory`**: Remove a previously approved additional directory.
- **`antigravity_list_directories`**: Show the fixed workspace and the session's approved additional directories.

Sessions keep their primary workspace immutable. Use `antigravity_add_directory` to attach other repositories or datasets without creating a new session; the approved directories are persisted in `sessions.json` and are inherited by subsequent jobs and filesystem operations.

---

## Development & Testing

```bash
# Run syntax and lint checks
npm run check

# Run test suite
npm test
```

---

## License

[MIT](LICENSE)
