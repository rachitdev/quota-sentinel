<div align="center">

# 🛡️ Quota Sentinel `v0.1.0` (`qs` · `quota-sentinel-dsh-plugin`)

### **Zero-Daemon Multi-Subscription Quota Guardian, Predictive OmniRoute Model Router, stdio MCP Server & Native DSH Host Plugin for DeepSeek Harness (DSH), Claude Code, OpenAI Codex, Grok, Google Antigravity & OpenCode Go**

*Continuously monitors 5-hour, weekly, and monthly quota exhaustion windows across paid AI subscriptions, enforces predictive model rerouting via OmniRoute, prevents hard rate limits with bounded overshoot, and delivers automated daily self-telemetry reports via Resend.*

[![Release: v0.1.0](https://img.shields.io/badge/Release-v0.1.0-eb6c36?style=for-the-badge)](https://github.com/rachitdev/quota-sentinel)
[![Author: Rachit Dev](https://img.shields.io/badge/Author-Rachit%20Dev-eb6c36?style=for-the-badge&logo=github)](https://github.com/rachitdev)
[![LinkedIn: rachit-dev](https://img.shields.io/badge/LinkedIn-Connect%20with%20Rachit%20Dev-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/rachit-dev)
[![DeepSeek Harness: Host Plugin](https://img.shields.io/badge/DeepSeek%20Harness-Host%20Plugin-0f766e?style=for-the-badge)](./plugins/dsh-quota-sentinel)
[![OmniRoute: Gateway Compatible](https://img.shields.io/badge/OmniRoute-Gateway%20Compatible-3b82f6?style=for-the-badge)](https://localhost:20128)
[![MCP: stdio JSON--RPC](https://img.shields.io/badge/MCP-stdio%20Server-7c3aed?style=for-the-badge)](./src/mcp.rs)
[![Built with Rust](https://img.shields.io/badge/Core-Rust%202024-1c2434?style=for-the-badge&logo=rust)](./src)
[![Tests: 92 Passed](https://img.shields.io/badge/Tests-92%20Passed-10b981?style=for-the-badge)](./tests)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](./LICENSE)

[**Key Capabilities**](#-key-capabilities) · [**System Architecture**](#-system-architecture) · [**Live Deployment Status**](#-live-deployment-status) · [**CLI & MCP Reference**](#-cli--mcp-reference) · [**DSH Plugin Integration**](#-deepseek-harness-dsh-host-plugin) · [**Safety Contract & Adversarial Verification**](#-safety-contract--adversarial-verification) · [**About the Author**](#-about-the-author--rachit-dev)

</div>

---

## 🚀 Overview: What is Quota Sentinel?

Power users and AI coding engineers frequently pay for multiple premium AI subscriptions simultaneously:
- **Claude Pro/Max** (Claude Code / Claude Desktop)
- **ChatGPT Plus/Pro** (OpenAI Codex CLI)
- **SuperGrok** (Grok Build CLI / xAI)
- **Google Gemini** (Google Antigravity CLI / IDE)
- **OpenCode Go** (OpenCode CLI)

Each vendor enforces disjoint, opaque rolling windows: **5-hour session blocks**, **rolling 7-day weekly caps**, or **monthly spend limits**. When a window fills mid-task:
1. **The Hard Wall:** Coding agents crash or hang mid-turn with `429 Too Many Requests` or quota exhaustion errors.
2. **Sibling Confusion:** Multi-account providers (e.g. `grok-cli` vs `xai-oauth`) or multi-window models get partially stranded while other credentials remain idle.
3. **Wasted Headroom:** Unused subscription quota silently expires without being tracked, while other providers take unnecessary load.

**Quota Sentinel** (`qs`) solves this with an auditable, zero-daemon Rust architecture:
- **Per-Window Quota Extraction:** Normalizes raw telemetry across all vendor formats (`quotas`, `quotaGroups`, `speed`, and dollar caps) into unified 5-hour, weekly, and monthly data points.
- **Predictive & Proactive Routing:** Monitors token burn rate; applies early routing exclusions when consumption is projected to cross `90%` before the next evaluation tick.
- **Atomic Connection Holds:** Coordinates with the local [OmniRoute](https://localhost:20128) gateway by applying single-PATCH `isActive: false` modifications alongside persistent `providerSpecificData.qsHold` ownership metadata.
- **Self-Healing & Auto-Restoration:** Automatically re-enables held subscriptions only when fresh authoritative telemetry confirms a natural window reset or usage drops below `<70%`.
- **Honest Daily Attribution:** Sends automated daily digest emails via [Resend](https://resend.com) reporting exact window peaks, open holds, manual overrides, and verifiable rerouting self-telemetry.

---

## 🌟 Key Capabilities

| Capability | Technical Mechanism | Benefit |
|---|---|---|
| **Zero-Daemon Rust Core** | Fast, single-shot binary (`qs tick`) executed via internal host timer, CLI, or MCP. | **0 MB** persistent background daemon RAM; crash-resilient. |
| **Transactional SQLite Ledger** | Append-only WAL database (`~/.dsh/state/quota-sentinel/qs.db`). | Full auditability of every observation, tick, decision, and hold. |
| **Subscription-Level Sibling Coordination** | Merges multi-connection providers (`grok-cli` + `xai-oauth`) into unified worst-case decisions. | Prevents partial holds where one account is blocked while a sibling gets hammered. |
| **Crash-Safe Ownership Protocol** | Stamped `qsHold` marker (`holdId`, `window`, `resetAt`, `priorActive`) written *with* connection state. | Protects user intent; foreign or manual disables are never auto-restored. |
| **Emergency Kill Switch (`PAUSE`)** | Instant dynamic file check (`~/.dsh/state/quota-sentinel/PAUSE`). | Freezes all gateway mutations in under `1ms` without bulk re-enabling. |
| **Manual Override Immunity** | Senses when an operator manually toggles an active connection. | Releases hold cleanly and grants a **24-hour suppression window** without fighting the user. |
| **In-Flight Stream Protection** | Verified admission-level control. | Active streaming responses finish normally; only *new* requests are redirected to fallback rungs. |
| **Daily Resend Reporting** | High-DPI HTML + plaintext email reports dispatched at 08:00 local time. | Clear visibility into subscription ROI, peak burn, and self-telemetry. |

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           QUOTA SENTINEL PIPELINE                           │
└─────────────────────────────────────────────────────────────────────────────┘

  Vendor Telemetry                 Reconciler & Policy             OmniRoute Gateway
 ┌─────────────────┐             ┌─────────────────────┐         ┌───────────────────┐
 │ Claude (5h, 7d) │             │  Freshness Check    │         │ auto-general-     │
 │ Codex (5h, 7d)  │──[Normalize]│  Sibling Merge      │──[Hold]─│ ladder            │
 │ Grok (Weekly)   │             │  Predictive Burn    │         │ (Reroutes traffic │
 │ Gemini (Groups) │             │  Strictest Window   │         │  to next fallback)│
 │ OpenCode ($60)  │             └──────────┬──────────┘         └───────────────────┘
 └─────────────────┘                        │                              ▲
                                   [Ownership Stamping]                    │
                                            ▼                              │
                                 ┌─────────────────────┐                   │
                                 │    SQLite Ledger    │                   │
                                 │   (WAL / Verified)  │───────────────────┘
                                 └──────────┬──────────┘  Single-PATCH Atomic State
                                            │             `isActive: false` + `qsHold`
                                            ▼
                                 ┌─────────────────────┐
                                 │    Resend Email     │
                                 │   Daily Telemetry   │
                                 │ (08:00 Local Digest)│
                                 └─────────────────────┘
```

### Architectural Safeguards

1. **Strictest-Window Priority:** A hold is locked to the most severe window triggered (`Monthly > Weekly > 5-Hour`). A 5-hour session reset can **never** accidentally clear a weekly or monthly hold.
2. **Freshness Asymmetry (Fail-Closed):** High usage on stale telemetry still holds (protecting quota); but stale or missing evidence can **never** authorize a release.
3. **Two-Way Dwell Hysteresis:** Re-enabling requires usage to drop below `<70%` (or authoritative reset timestamp passage) plus a minimum **30-minute dwell** to prevent rapid flapping.
4. **Capacity Floor:** All subscriptions may be held if exhausted; the floor is reserved for free/local rungs (`openrouter-free`, `lmstudio`, etc.), which Sentinel never disables.

---

## 📊 Live Deployment Status

As of `v0.1.0`, Quota Sentinel operates with strict staged gating:

| Subscription | Connection ID | State | Evidence & Gating Results |
|---|---|---|---|
| **ChatGPT / Codex** | `411f6c47` | **LIVE** | **Gated & Passed (Astra Approved)**: Real in-flight stream completion verified, 401 direct rejection (`0.01s`), fallback to free rungs verified, gateway restart recovery tested, `qs release` verified. |
| **OpenCode Go** | `625bf7b6` | **LIVE** | **Gated & Passed (Astra Approved)**: Real trigger tested against active monthly cap (`$60/$60` = `100%`). Monthly hold active until reset on Nov 16 at 08:39 UTC. |
| **Gemini (Antigravity)** | `099b2bcf` | **LIVE** | **Gated & Passed (Astra Approved)**: Verified against real `agy` connection with lowered threshold. Grouped quota parsing, fallback, and zero unexpected mutations confirmed. |
| **Grok** | `6d285c12` & `175330f6` | **SHADOW** | **Gated**: Sibling coordination verified across `grok-cli` and `xai-oauth`. 16-point test suite passed. Held in shadow pending natural reset observation. |
| **Claude** | `db842aae` | **SHADOW** | **Gated**: Functional tests passed. Held in shadow per Astra's safety recommendation regarding 30-hour OAuth token survival. |

---

## 💻 CLI & MCP Reference

### Command-Line Interface (`qs`)

The single-binary executable is installed to `~/.dsh/bin/qs`:

```bash
# Execute a single evaluation tick (idempotent; file-lock protected)
qs tick

# Emit structured JSON summary of tick results
qs tick --json

# View live quota percentages across all managed connections
qs status

# Inspect currently active holds, reset timestamps, and maximum durations
qs holds

# Manually release a specific hold by ID with verified ownership checks
qs release <hold-id>

# Temporarily suppress holds on a connection for N hours (e.g. after manual override)
qs override <connection-id> --hours 24

# Instant Emergency Kill Switch: freeze all gateway writes immediately
qs pause

# Resume automated enforcement
qs resume

# Generate and send daily telemetry email immediately (or preview with --dry)
qs report
qs report --dry
```

### Model Context Protocol (MCP) Server

Quota Sentinel exposes a native `stdio` JSON-RPC 2.0 MCP server:

```bash
qs mcp
```

#### Available MCP Tools

- `qs_status`: Returns current mode (`live`, `shadow`, `paused`), active subscription counts, and last tick heartbeat.
- `qs_holds`: Returns array of active holds with expiration boundaries and rationale.
- `qs_stats`: Returns 24-hour peak utilization, action history, and self-telemetry.
- `qs_decisions`: Queries the historical decision log with optional limits.
- `qs_pause`: Sets the emergency `PAUSE` file.
- `qs_resume`: Removes the `PAUSE` file.
- `qs_override`: Applies an operator override suppression to a connection.

---

## 🔌 DeepSeek Harness (DSH) Host Plugin

Quota Sentinel includes a first-class DSH Host bundle (`quota-sentinel-dsh-plugin`) located at `plugins/dsh-quota-sentinel`:

### Features
- **In-Process Background Maintenance:** Runs automatic 15-minute evaluation ticks inside the DSH Host process without consuming chat agent turns or LLM tokens.
- **Daily 08:00 Local Delivery:** Background timer dispatches the daily Resend telemetry digest automatically.
- **Visual Integration:** High-DPI custom SVG icon and manifest displayed directly in DSH's **Plugins Manager**.
- **Native Context API:** Exposes `ctx.quotaSentinel.runTick()` and `ctx.quotaSentinel.runReport()` to other Cordis services.
- **Agent Tool:** Mounts `quota_sentinel_tick` onto `ctx.tools`.

### Installation in DSH Profile
In `~/.dsh/profiles/web/package.json`:
```json
{
  "dependencies": {
    "quota-sentinel-dsh-plugin": "file:/Users/rachit/code/quota-sentinel/plugins/dsh-quota-sentinel"
  },
  "dsh": {
    "profile": {
      "bundles": [
        "quota-sentinel-dsh-plugin"
      ]
    }
  }
}
```

In `~/.dsh/profiles/web/cordis.patch.yml`:
```yaml
- id: quota-sentinel
  config:
    binaryPath: !!js "process.env['HOME'] + '/.dsh/bin/qs'"
    tickIntervalMinutes: 15
    autoTick: true

- insert:
    - id: mcp-quota-sentinel
      name: '@deepseek-ai/dsh-mcp-client'
      config:
        serverName: quota_sentinel
        transport: stdio
        command: !!js "process.env['HOME'] + '/.dsh/bin/qs'"
        args: [ 'mcp' ]
        failOnStartupError: false
        toolCallTimeoutMs: 60000
```

---

## 🔒 Safety Contract & Adversarial Verification

Quota Sentinel was architected and verified through an adversarial design review loop with **GPT-6 Astra** (`cx/gpt-6-astra-high`):

```
v0.3 Review ──► REJECT (15m polling gap, unproven capacity floor)
     │
v0.4 Review ──► REJECT (Fail-open request shim cannot serve as hard boundary)
     │
v0.5 Review ──► APPROVE-WITH-CHANGES (Bounded Overshoot contract defined)
     │
Code Review ──► REJECT (Ambiguity in pending holds, reset jitter, pre-write race)
     │
Final Gates ──► ALL PASS (Deployed verification suite across live connections)
```

### The Bounded Overshoot Contract
No client-side or gateway poller can guarantee a mathematical `90.000%` hard stop: external traffic occurs outside the gateway, and upstream vendor telemetry lags. Quota Sentinel defines an **explicit Bounded Overshoot contract**:
1. **Durable Fail-Closed Hold (L1):** `isActive: false` written to the gateway DB. Persists across full process and host reboots.
2. **Predictive Guard Band (L3):** Automatically adjusts trigger threshold using historical burn rate + latency window (`tick_minutes + latency_minutes`).
3. **Honest Attribution:** Daily telemetry explicitly reports observed worst-case overshoot and labels unmeasured reroutes as `unavailable` rather than fabricating metrics.

---

## 👨‍💻 About the Author — Rachit Dev

**Rachit Dev**  
*Chief AI Transformation Officer at EZ Sol Precision*  
*Specialist in Autonomous Agent Infrastructure, Cognitive Architecture & Distributed LLM Systems*

- **GitHub:** [@rachitdev](https://github.com/rachitdev)
- **LinkedIn:** [linkedin.com/in/rachit-dev](https://www.linkedin.com/in/rachit-dev)
- **Portfolio & Systems:** Creator of [Skillforge](https://github.com/rachitdev/skillforge), Decision Forge, Mac Mini Fan Driver, and Quota Sentinel.

---

## 📄 License

This project is licensed under the [MIT License](./LICENSE).
