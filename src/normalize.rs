//! Turn the gateway's per-connection usage payloads into [`WindowObs`] readings.
//!
//! Payloads differ per vendor (verified 2026-11-07): `quotas{name:{used,total,remainingPercentage,resetAt}}`
//! for Claude / Codex / OpenCode Go / Grok, and `quotaGroups[].windows{session,weekly}` for Antigravity.
use crate::model::{WindowKind, WindowObs};
use chrono::{DateTime, Utc};
use serde_json::Value;

#[derive(Debug, Default)]
pub struct Normalized {
    pub obs: Vec<WindowObs>,
    /// Window names present in the payload but not classified (never guessed).
    pub ignored: Vec<String>,
    /// Set when the payload carries no usable quota (error, message-only, no windows).
    pub unavailable: Option<String>,
}

fn parse_time(v: Option<&Value>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(v?.as_str()?).ok().map(|d| d.with_timezone(&Utc))
}

/// Percent used, 0..=100. Prefers `remainingPercentage` (carries fractions) over `used/total`.
fn used_pct(w: &Value) -> Option<f64> {
    if w.get("unlimited").and_then(|x| x.as_bool()).unwrap_or(false) {
        return None;
    }
    if let Some(rem) = w.get("remainingPercentage").and_then(|x| x.as_f64()) {
        return Some((100.0 - rem).clamp(0.0, 100.0));
    }
    let used = w.get("used").and_then(|x| x.as_f64())?;
    let total = w.get("total").and_then(|x| x.as_f64())?;
    if total > 0.0 {
        Some((used / total * 100.0).clamp(0.0, 100.0))
    } else {
        None
    }
}

pub fn normalize(
    provider: &str,
    conn_id: &str,
    subscription: &str,
    usage: &Value,
    fetched_at: DateTime<Utc>,
) -> Normalized {
    let mut out = Normalized::default();
    let top_fetched = parse_time(usage.get("fetchedAt")).unwrap_or(fetched_at);
    let push = |name: &str, w: &Value, group: Option<&str>, out: &mut Normalized| {
        let Some(kind) = WindowKind::classify(name) else {
            out.ignored.push(name.to_string());
            return;
        };
        let Some(used) = used_pct(w) else {
            out.ignored.push(format!("{name}(no-percent)"));
            return;
        };
        out.obs.push(WindowObs {
            provider: provider.to_string(),
            connection_id: conn_id.to_string(),
            subscription: subscription.to_string(),
            kind,
            group: group.map(str::to_string),
            used_pct: used,
            reset_at: parse_time(w.get("resetAt")),
            observed_at: parse_time(w.get("observedAt")).unwrap_or(top_fetched),
        });
    };

    // Antigravity carries both `quotaGroups[].windows` and a per-model `quotas` map keyed by model id that
    // repeats the same numbers. Groups are the source of truth; the per-model map is then ignored, not double counted.
    let has_groups = usage.get("quotaGroups").and_then(|x| x.as_array()).is_some_and(|g| !g.is_empty());
    if let Some(q) = usage.get("quotas").and_then(|x| x.as_object()) {
        for (name, w) in q {
            if has_groups {
                out.ignored.push(format!("{name}(per-model, covered by quotaGroups)"));
            } else {
                push(name, w, None, &mut out);
            }
        }
    }
    if let Some(groups) = usage.get("quotaGroups").and_then(|x| x.as_array()) {
        for g in groups {
            let gid = g.get("id").and_then(|x| x.as_str());
            if let Some(ws) = g.get("windows").and_then(|x| x.as_object()) {
                for (name, w) in ws {
                    push(name, w, gid, &mut out);
                }
            }
        }
    }

    // Grok reports the same weekly numbers twice ("weekly" and "product_grok_build"); the second is ignored above.
    if out.obs.is_empty() {
        out.unavailable = Some(
            usage
                .get("error")
                .and_then(|e| e.as_str())
                .or_else(|| usage.get("message").and_then(|e| e.as_str()))
                .unwrap_or("no recognised quota windows")
                .chars()
                .take(160)
                .collect(),
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn fx(name: &str) -> Value {
        let p = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
        serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
    }
    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 7, 14, 0, 0).unwrap()
    }
    fn kinds(n: &Normalized) -> Vec<(WindowKind, i64)> {
        let mut v: Vec<_> = n.obs.iter().map(|o| (o.kind, o.used_pct.round() as i64)).collect();
        v.sort();
        v
    }

    #[test]
    fn classify_names() {
        use WindowKind::*;
        assert_eq!(WindowKind::classify("session (5h)"), Some(FiveHour));
        assert_eq!(WindowKind::classify("session"), Some(FiveHour));
        assert_eq!(WindowKind::classify("weekly (7d)"), Some(Weekly));
        assert_eq!(WindowKind::classify("mcp_monthly"), Some(Monthly));
        assert_eq!(WindowKind::classify("product_grok_build"), None);
    }

    #[test]
    fn claude_has_5h_and_weekly() {
        let n = normalize("claude", "c1", "claude", &fx("usage_claude.json"), now());
        let k: Vec<_> = n.obs.iter().map(|o| o.kind).collect();
        assert!(k.contains(&WindowKind::FiveHour) && k.contains(&WindowKind::Weekly), "{k:?}");
        assert!(n.unavailable.is_none());
    }

    #[test]
    fn opencode_go_monthly_exhausted_while_5h_free() {
        let n = normalize("opencode-go", "o1", "opencode-go", &fx("usage_opencode-go.json"), now());
        let m = n.obs.iter().find(|o| o.kind == WindowKind::Monthly).expect("monthly");
        let s = n.obs.iter().find(|o| o.kind == WindowKind::FiveHour).expect("5h");
        assert_eq!(m.used_pct.round() as i64, 100);
        assert_eq!(s.used_pct.round() as i64, 0);
    }

    #[test]
    fn grok_weekly_only_and_duplicate_product_row_ignored() {
        let n = normalize("grok-cli", "g1", "grok", &fx("usage_grok-cli.json"), now());
        assert_eq!(n.obs.len(), 1, "{:?}", kinds(&n));
        assert_eq!(n.obs[0].kind, WindowKind::Weekly);
        assert!(n.ignored.iter().any(|s| s == "product_grok_build"));
    }

    #[test]
    fn antigravity_groups_carry_group_ids() {
        let n = normalize("agy", "a1", "gemini", &fx("usage_agy.json"), now());
        assert!(n.obs.len() >= 4, "{:?}", kinds(&n));
        assert!(n.obs.iter().all(|o| o.group.is_some()));
    }

    #[test]
    fn codex_session_and_weekly() {
        let n = normalize("codex", "x1", "chatgpt", &fx("usage_codex.json"), now());
        let k: Vec<_> = n.obs.iter().map(|o| o.kind).collect();
        assert!(k.contains(&WindowKind::FiveHour) && k.contains(&WindowKind::Weekly));
    }

    #[test]
    fn message_only_payload_is_unavailable_not_recovery() {
        let n = normalize("kimi-coding", "k1", "x", &fx("usage_antigravity.json"), now());
        // inactive antigravity fixture may or may not have windows; the invariant is: no obs => unavailable set
        if n.obs.is_empty() {
            assert!(n.unavailable.is_some());
        }
        let v = serde_json::json!({"error":"Usage not available for this connection"});
        let n = normalize("opencode-zen", "z", "x", &v, now());
        assert!(n.obs.is_empty() && n.unavailable.is_some());
    }

    #[test]
    fn unlimited_and_zero_total_are_skipped() {
        let v = serde_json::json!({"quotas":{"weekly":{"unlimited":true,"used":0,"total":0},"session":{"used":1,"total":0}}});
        let n = normalize("p", "c", "s", &v, now());
        assert!(n.obs.is_empty());
    }
}
