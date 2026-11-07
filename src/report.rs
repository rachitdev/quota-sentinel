//! Daily report: built purely from the ledger, so it is reproducible and testable. Every number is labelled
//! measured / estimate / unavailable; nothing is invented (spec section 8).
use crate::ledger::Ledger;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use std::fmt::Write;

#[derive(Debug, Default)]
pub struct Report {
    pub subject: String,
    pub text: String,
    pub html: String,
}

struct Row {
    sub: String,
    win: String,
    now_pct: f64,
    peak_pct: f64,
    reset: Option<String>,
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

pub fn build(l: &Ledger, now: DateTime<Utc>, mode: &str) -> Result<Report> {
    let since = (now - Duration::hours(24)).to_rfc3339();

    // Utilisation: latest and 24h peak per subscription/window (worst across connections).
    let mut st = l.db.prepare(
        "SELECT subscription, win, MAX(used_pct) FROM obs WHERE at>=? GROUP BY subscription, win ORDER BY subscription, win",
    )?;
    let peaks: Vec<(String, String, f64)> = st.query_map([&since], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<Result<_, _>>()?;
    let mut rows = Vec::new();
    for (sub, win, peak) in peaks {
        let latest: Option<(f64, Option<String>)> = l
            .db
            .query_row(
                "SELECT used_pct, reset_at FROM obs WHERE subscription=? AND win=? ORDER BY at DESC, used_pct DESC LIMIT 1",
                [&sub, &win],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
        let (now_pct, reset) = latest.unwrap_or((peak, None));
        rows.push(Row { sub, win, now_pct, peak_pct: peak, reset });
    }

    // Actions in the last 24h.
    let mut st = l.db.prepare("SELECT at, action, subscription, window, reason, applied FROM decisions WHERE at>=? AND action NOT LIKE 'alert:%' ORDER BY at")?;
    let acts: Vec<(String, String, Option<String>, Option<String>, String, i64)> =
        st.query_map([&since], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get::<_, Option<String>>(4)?.unwrap_or_default(), r.get(5)?)))?.collect::<Result<_, _>>()?;
    let applied = acts.iter().filter(|a| a.5 == 1).count();

    let mut st = l.db.prepare("SELECT action, COUNT(*) FROM decisions WHERE at>=? AND action LIKE 'alert:%' GROUP BY action ORDER BY 2 DESC")?;
    let alerts: Vec<(String, i64)> = st.query_map([&since], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?;

    let holds = l.open_holds()?;

    // Self-telemetry (measured).
    let (ticks, ok_ticks): (i64, i64) = l.db.query_row("SELECT COUNT(*), COALESCE(SUM(ok),0) FROM ticks WHERE started_at>=? AND finished_at IS NOT NULL", [&since], |r| Ok((r.get(0)?, r.get(1)?)))?;
    let expected = 24 * 4;
    let shadow: i64 = l.db.query_row("SELECT COUNT(*) FROM ticks WHERE started_at>=? AND mode!='live'", [&since], |r| r.get(0))?;
    let max_gap: Option<f64> = {
        let mut st = l.db.prepare("SELECT started_at FROM ticks WHERE started_at>=? ORDER BY started_at")?;
        let ts: Vec<DateTime<Utc>> = st.query_map([&since], |r| r.get::<_, String>(0))?.filter_map(|s| s.ok()).filter_map(|s| DateTime::parse_from_rfc3339(&s).ok()).map(|d| d.with_timezone(&Utc)).collect();
        ts.windows(2).map(|w| (w[1] - w[0]).num_minutes() as f64).fold(None, |m, v| Some(m.map_or(v, |x: f64| x.max(v))))
    };
    // Honest label: enforcement only happened for 'live' ticks.
    let saved = "unavailable — the gateway does not expose the baseline route a request would have taken, so \
                 'requests rerouted away from a provider at >=90%' cannot be measured yet. Measured instead: holds placed and released (above).";

    let subject = format!("Quota Sentinel {} — {} holds placed, {} open [{}]", now.format("%Y-%m-%d"), acts.iter().filter(|a| a.1.starts_with("hold") && a.5 == 1).count(), holds.len(), mode);

    let mut t = String::new();
    let _ = writeln!(t, "QUOTA SENTINEL — daily report {}  (mode now: {})\n", now.format("%Y-%m-%d %H:%M UTC"), mode);
    let _ = writeln!(t, "UTILISATION (latest / 24h peak, worst connection per subscription)");
    for r in &rows {
        let _ = writeln!(t, "  {:<12} {:<8} {:>5.1}%  peak {:>5.1}%  resets {}", r.sub, r.win, r.now_pct, r.peak_pct, r.reset.as_deref().unwrap_or("unknown"));
    }
    let _ = writeln!(t, "\nOPEN HOLDS ({})", holds.len());
    for h in &holds {
        let _ = writeln!(t, "  {} {} {} {} until reset {} (max {})", h.subscription, h.window.as_str(), if h.applied { "applied" } else { "PENDING" }, h.id, h.reset_at.map(|d| d.to_rfc3339()).unwrap_or("unknown".into()), h.max_until.to_rfc3339());
    }
    let _ = writeln!(t, "\nACTIONS LAST 24H ({} applied, {} shadow/aborted)", applied, acts.len() - applied);
    for a in acts.iter().take(40) {
        let _ = writeln!(t, "  {} {:<16} {:<11} {:<7} {}{}", &a.0[11..16], a.1, a.2.as_deref().unwrap_or("-"), a.3.as_deref().unwrap_or("-"), if a.5 == 1 { "" } else { "[not applied] " }, a.4);
    }
    let _ = writeln!(t, "\nALERTS: {}", if alerts.is_empty() { "none".into() } else { alerts.iter().map(|a| format!("{}×{}", a.0.trim_start_matches("alert:"), a.1)).collect::<Vec<_>>().join(", ") });
    let _ = writeln!(t, "\nSELF-TELEMETRY (measured)");
    let _ = writeln!(t, "  ticks run {ticks}/{expected} expected, ok {ok_ticks}, longest gap {} min, non-live ticks {shadow}", max_gap.map(|m| format!("{m:.0}")).unwrap_or("n/a".into()));
    let _ = writeln!(t, "  holds placed {}, released {}", acts.iter().filter(|a| a.1.starts_with("hold") && a.5 == 1).count(), acts.iter().filter(|a| a.1 == "release" && a.5 == 1).count());
    let _ = writeln!(t, "  saved-you (requests rerouted): {saved}");
    let _ = writeln!(t, "  enforcement class: bounded overshoot; Claude and Antigravity have no native gateway cutoff (spec v0.6). Usage outside the gateway is not controlled.");
    if ticks < expected / 2 {
        let _ = writeln!(t, "  WARNING: fewer than half of expected ticks ran (app closed or scheduler stopped); protection was reduced.");
    }

    let mut h = String::new();
    let _ = write!(h, "<html><body style=\"font-family:-apple-system,Segoe UI,sans-serif;max-width:760px;color:#2d3142\"><h2>Quota Sentinel — {}</h2><p>mode: <b>{}</b></p><h3>Utilisation</h3><table cellpadding=6 style=\"border-collapse:collapse\"><tr><th align=left>Subscription</th><th>Window</th><th>Now</th><th>24h peak</th><th align=left>Resets</th></tr>", now.format("%Y-%m-%d"), esc(mode));
    for r in &rows {
        let colour = if r.now_pct >= 90.0 { "#c0392b" } else if r.now_pct >= 70.0 { "#d68910" } else { "#2d3142" };
        let _ = write!(h, "<tr style=\"border-top:1px solid #ddd\"><td>{}</td><td>{}</td><td style=\"color:{colour}\"><b>{:.1}%</b></td><td>{:.1}%</td><td>{}</td></tr>", esc(&r.sub), esc(&r.win), r.now_pct, r.peak_pct, esc(r.reset.as_deref().unwrap_or("unknown")));
    }
    let _ = write!(h, "</table><h3>Open holds ({})</h3><ul>", holds.len());
    for x in &holds {
        let _ = write!(h, "<li>{} {} — {} — resets {}</li>", esc(&x.subscription), x.window.as_str(), if x.applied { "applied" } else { "PENDING" }, esc(&x.reset_at.map(|d| d.to_rfc3339()).unwrap_or("unknown".into())));
    }
    let _ = write!(h, "</ul><h3>Self-telemetry (measured)</h3><ul><li>ticks {ticks}/{expected}, ok {ok_ticks}, longest gap {} min</li><li>saved-you: {}</li><li>enforcement: bounded overshoot (no native cutoff for Claude/Antigravity)</li></ul><pre style=\"font-size:12px\">{}</pre></body></html>", max_gap.map(|m| format!("{m:.0}")).unwrap_or("n/a".into()), esc(saved), esc(&t));
    Ok(Report { subject, text: t, html: h })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{WindowKind, WindowObs};
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 7, 8, 0, 0).unwrap()
    }
    #[test]
    fn report_from_ledger_has_honest_saved_you_and_no_invented_numbers() {
        let l = Ledger::memory().unwrap();
        l.record_obs(now() - Duration::hours(1), &WindowObs { provider: "claude".into(), connection_id: "c".into(), subscription: "claude".into(), kind: WindowKind::Weekly, group: None, used_pct: 92.0, reset_at: Some(now() + Duration::days(1)), observed_at: now() - Duration::hours(1) }).unwrap();
        let t = l.start_tick(now() - Duration::hours(1), "shadow").unwrap();
        l.finish_tick(t, now(), true, "x").unwrap();
        l.record_decision(now() - Duration::hours(1), t, Some("c"), Some("claude"), "hold", Some(WindowKind::Weekly), "claude weekly 92%", false, "shadow").unwrap();
        let r = build(&l, now(), "shadow").unwrap();
        assert!(r.text.contains("92.0%"), "{}", r.text);
        assert!(r.text.contains("unavailable"), "saved-you must be labelled unavailable");
        assert!(r.text.contains("[not applied]"));
        assert!(r.text.contains("ticks run 1/96"));
        assert!(r.text.contains("WARNING: fewer than half"));
        assert!(r.html.contains("<table") && !r.html.contains("<script"));
    }
    #[test]
    fn empty_ledger_still_renders() {
        let r = build(&Ledger::memory().unwrap(), now(), "shadow").unwrap();
        assert!(r.text.contains("OPEN HOLDS (0)"));
    }
    #[test]
    fn html_escapes_untrusted_text() {
        let l = Ledger::memory().unwrap();
        let t = l.start_tick(now(), "shadow").unwrap();
        l.record_decision(now(), t, Some("c"), Some("<b>x</b>"), "hold", None, "<script>alert(1)</script>", false, "").unwrap();
        let r = build(&l, now(), "shadow").unwrap();
        assert!(!r.html.contains("<script>alert"));
    }
}
