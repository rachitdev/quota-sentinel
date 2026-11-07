# Quota Sentinel (`qs`)

Rust one-shot CLI + stdio MCP server that keeps paid LLM subscriptions below their quota limits by holding
(`isActive=false`) the matching OmniRoute connection, and emails a daily report. No daemon. Spec:
`~/code/dsh ExBuilds/docs/quota-sentinel/SPEC.md` (v0.6, reviewed by GPT Astra 6).

## State (2026-11-07)
Default is SHADOW (observe + log, never write). A connection is written only if `enabled = true` AND its subscription is in
`live_subscriptions` AND its connection id is in `live_connections` (`~/.dsh/etc/quota-sentinel.toml`).
| Subscription | State | Evidence |
|---|---|---|
| chatgpt (codex connection) | **LIVE** | full gate on real connection + gateway-restart-while-held |
| opencode-go (main) | **LIVE** | full gate on real 100% monthly trigger |
| claude | SHADOW | Astra REJECT: needs a ~30h hold to prove the OAuth credential survives + alias/route check |
| grok, gemini | SHADOW | gates not run |
Not tested anywhere: a natural window reset (release was exercised with a simulated reset).

## Commands
| | |
|---|---|
| `qs tick [--json]` | one run (flock-protected) |
| `qs status` / `qs holds` | read-only views |
| `qs report [--dry]` | daily report; without `--dry` sends email |
| `qs pause` / `qs resume` | PAUSE file: freezes every gateway write, never bulk re-enables |
| `qs override <connection> --hours N` | no new holds on a connection (use after re-enabling by hand) |
| `qs mcp` | stdio MCP: qs_status, qs_holds, qs_stats, qs_decisions, qs_pause, qs_resume, qs_override |

## One-time setup (needs the user)
1. **Email:** create `~/.dsh/secrets/quota-sentinel-smtp.env` (chmod 600):
   `SMTP_USER=rachit.dev@gmail.com`, `SMTP_PASS=<Gmail app password>`, optional `MAIL_TO`, `MAIL_FROM`.
2. **DSH MCP entry** (profile `cordis.patch.yml`): `serverName: quota_sentinel`, `transport: stdio`,
   `command: ~/.dsh/bin/qs`, `args: [mcp]`.
3. **DSH Automation tasks:** `*/15 * * * *` -> run `~/.dsh/bin/qs tick --json`; `0 8 * * *` -> run `~/.dsh/bin/qs report`.

## Enabling enforcement (per provider, one at a time)
Set `enabled = true` and add the subscription to `live_subscriptions`. Order: `chatgpt`, `grok`, `opencode-go`, `claude`, `gemini`.
Each needs these Astra gates PASSED on that provider first:
- quota readable while the connection is inactive; real reset recovery observed;
- direct + combo routing honour the hold; new vs in-flight streaming behaviour;
- marker null / manual-override behaviour on that provider; zero gateway writes in shadow across fault paths;
- documented limits: no gateway CAS (a user write between the final read and PATCH can be overwritten and may be
  undetectable); a user disabling an already-held connection is undetectable; usage outside the gateway is not controlled.
