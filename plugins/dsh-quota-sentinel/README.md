# Quota Sentinel DSH Plugin

DSH Host plugin for Quota Sentinel:
- Continuous background maintenance and quota enforcement in OmniRoute every 15 minutes.
- Evaluates per-window quota usage (5-hour, weekly, monthly) across all active paid subscriptions.
- Automatic routing exclusions and connection holds at >=90% quota.
- Daily usage and self-telemetry email reports sent via Resend API.
- Native DSH tool: `quota_sentinel_tick`.
- Exposes MCP server `quota_sentinel` (`qs_status`, `qs_holds`, `qs_stats`, `qs_decisions`, `qs_pause`, `qs_resume`, `qs_override`).

Author: Rachit Dev <rachit.dev@gmail.com>
License: MIT
