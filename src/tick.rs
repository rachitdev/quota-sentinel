//! One `qs tick`: read -> normalise -> record -> decide -> act -> log. Idempotent; takes an OS lock.
use crate::actuator::{Actuator, Outcome};
use crate::config::Config;
use crate::gateway::Gateway;
use crate::ledger::Ledger;
use crate::model::{Conn, WindowKind};
use crate::normalize::normalize;
use crate::policy::{decide, Action, ConnInput, PolicyInput};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;
use std::fs::OpenOptions;

#[derive(Debug, Default)]
pub struct TickReport {
    pub tick_id: i64,
    pub mode: String,
    pub managed: usize,
    pub probe_errors: Vec<String>,
    pub results: Vec<(Action, Outcome)>,
}

pub fn pause_file(cfg: &Config) -> std::path::PathBuf {
    cfg.state_path().join("PAUSE")
}

/// Take the tick lock; `None` means another tick is running (this one exits quietly).
pub fn try_lock(cfg: &Config) -> Result<Option<std::fs::File>> {
    use fs2::FileExt;
    std::fs::create_dir_all(cfg.state_path())?;
    let f = OpenOptions::new().create(true).truncate(false).write(true).open(cfg.state_path().join("tick.lock"))?;
    Ok(if f.try_lock_exclusive().is_ok() { Some(f) } else { None })
}

/// Percentage points per minute from observations within the last hour; only rising segments count.
fn burn_rates(ledger: &Ledger, conn: &str, now: DateTime<Utc>) -> BTreeMap<WindowKind, f64> {
    let mut m = BTreeMap::new();
    for w in [WindowKind::FiveHour, WindowKind::Weekly, WindowKind::Monthly] {
        let s = ledger.recent_series(conn, w, now - Duration::minutes(60)).unwrap_or_default();
        let (Some(first), Some(last)) = (s.first(), s.last()) else { continue };
        let mins = (last.0 - first.0).num_seconds() as f64 / 60.0;
        let d = last.1 - first.1;
        if mins >= 5.0 && d > 0.0 {
            m.insert(w, d / mins);
        }
    }
    m
}

pub fn run_tick(cfg: &Config, gw: &dyn Gateway, ledger: &Ledger, now: DateTime<Utc>) -> Result<TickReport> {
    let paused = pause_file(cfg).exists();
    let mode = if paused { "paused" } else if cfg.enabled { "live" } else { "shadow" };
    let tick = ledger.start_tick(now, mode)?;
    let mut rep = TickReport { tick_id: tick, mode: mode.into(), ..Default::default() };

    let raw = gw.connections()?;
    let mut inputs: Vec<ConnInput> = Vec::new();
    let mut live: BTreeMap<String, (String, bool)> = BTreeMap::new();

    for v in &raw {
        let Some(c) = Conn::from_json(v) else { continue };
        let Some(sub) = cfg.subscription_of(&c.provider).map(str::to_string) else { continue };
        rep.managed += 1;
        live.insert(c.id.clone(), (sub.clone(), cfg.is_live(&sub, &c.id)));
        let (obs, unavailable) = match gw.usage(&c.id) {
            Ok(u) => {
                let n = normalize(&c.provider, &c.id, &sub, &u, now);
                (n.obs, n.unavailable)
            }
            Err(e) => {
                rep.probe_errors.push(format!("{}: {e:#}", c.provider));
                (vec![], Some(format!("probe error: {e:#}")))
            }
        };
        for o in &obs {
            ledger.record_obs(now, o)?;
        }
        inputs.push(ConnInput { burn: burn_rates(ledger, &c.id, now), conn: c, subscription: sub, obs, unavailable });
    }

    let holds = ledger.open_holds()?;
    let mut overrides = BTreeMap::new();
    for i in &inputs {
        if let Some(u) = ledger.override_until(&i.conn.id)? {
            overrides.insert(i.conn.id.clone(), u);
        }
    }
    let actions = decide(&PolicyInput { now, th: &cfg.thresholds, conns: &inputs, holds: &holds, overrides: &overrides });
    rep.results = Actuator { gw, ledger, now, live: &live, paused, pause_file: Some(pause_file(cfg)), tick }.apply_all(&actions);
    let failed = rep.results.iter().filter(|(_, o)| matches!(o, Outcome::Failed(_))).count();
    ledger.finish_tick(tick, Utc::now(), failed == 0 && rep.probe_errors.is_empty(), &format!("managed={} actions={} failed={failed}", rep.managed, rep.results.len()))?;
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::fake::FakeGateway;
    use chrono::TimeZone;
    use serde_json::json;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 7, 14, 0, 0).unwrap()
    }
    fn cfg(live: bool) -> Config {
        let mut c = Config::default();
        c.enabled = live;
        if live {
            c.live_subscriptions = vec!["claude".into()];
            c.live_connections = vec!["c1".into()];
        }
        c.state_dir = tempfile::tempdir().unwrap().keep().to_string_lossy().into();
        c
    }
    fn usage(weekly_used: f64, weekly_reset_min: i64) -> serde_json::Value {
        json!({"quotas": {
            "session (5h)": {"used": 10, "total": 100, "remainingPercentage": 90, "resetAt": (now()+Duration::hours(3)).to_rfc3339(), "observedAt": now().to_rfc3339()},
            "weekly (7d)": {"used": weekly_used, "total": 100, "remainingPercentage": 100.0-weekly_used, "resetAt": (now()+Duration::minutes(weekly_reset_min)).to_rfc3339(), "observedAt": now().to_rfc3339()}
        }})
    }
    fn rig() -> (FakeGateway, Ledger) {
        let g = FakeGateway::default();
        g.add("c1", "claude", true);
        g.add("free", "openrouter", true);
        (g, Ledger::memory().unwrap())
    }

    #[test]
    fn shadow_tick_decides_but_never_writes() {
        let (g, l) = rig();
        g.usage.borrow_mut().insert("c1".into(), usage(95.0, 3000));
        let r = run_tick(&cfg(false), &g, &l, now()).unwrap();
        assert_eq!(r.mode, "shadow");
        assert!(r.results.iter().any(|(a, o)| matches!(a, Action::Hold { .. }) && *o == Outcome::Shadow));
        assert!(g.patches.borrow().is_empty());
        assert_eq!(g.get("c1")["isActive"], json!(true));
    }
    #[test]
    fn live_tick_holds_then_next_tick_is_idempotent() {
        let (g, l) = rig();
        g.usage.borrow_mut().insert("c1".into(), usage(95.0, 3000));
        let c = cfg(true);
        run_tick(&c, &g, &l, now()).unwrap();
        assert_eq!(g.get("c1")["isActive"], json!(false));
        let n = g.patches.borrow().len();
        run_tick(&c, &g, &l, now() + Duration::minutes(15)).unwrap();
        assert_eq!(g.patches.borrow().len(), n, "second tick must not write again");
        assert_eq!(l.open_holds().unwrap().len(), 1);
    }
    #[test]
    fn non_allowlisted_sibling_is_never_written_but_its_reading_protects_the_allowlisted_one() {
        let (g, l) = rig();
        g.add("c2", "claude", true); // same subscription, NOT allowlisted
        g.usage.borrow_mut().insert("c1".into(), usage(10.0, 3000));
        g.usage.borrow_mut().insert("c2".into(), usage(99.0, 3000));
        let r = run_tick(&cfg(true), &g, &l, now()).unwrap();
        // c2 (not allowlisted) is only ever shadowed
        assert!(r.results.iter().any(|(a, o)| matches!(a, Action::Hold { conn_id, .. } if conn_id == "c2") && *o == Outcome::Shadow));
        assert_eq!(g.get("c2")["isActive"], json!(true));
        assert!(g.patches.borrow().iter().all(|(id, _)| id == "c1"), "only the allowlisted connection is ever written");
        // c1 is held because the SUBSCRIPTION is exhausted according to its sibling
        assert_eq!(g.get("c1")["isActive"], json!(false));
    }
    #[test]
    fn restart_while_held_recovers_from_ledger_and_gateway_state_alone() {
        let (g, l) = rig();
        let c = cfg(true);
        g.usage.borrow_mut().insert("c1".into(), usage(95.0, 3000));
        run_tick(&c, &g, &l, now()).unwrap();
        // simulate a new process: nothing but the gateway state and the ledger file survive
        let n = g.patches.borrow().len();
        let r = run_tick(&c, &g, &l, now() + Duration::minutes(15)).unwrap();
        assert_eq!(g.patches.borrow().len(), n);
        assert_eq!(g.get("c1")["isActive"], json!(false));
        assert_eq!(l.open_holds().unwrap().len(), 1);
        assert!(r.results.iter().all(|(a, _)| !matches!(a, Action::Release { .. } | Action::ManualOverride { .. })));
    }
    #[test]
    fn unmanaged_providers_are_never_touched() {
        let (g, l) = rig();
        g.usage.borrow_mut().insert("c1".into(), usage(10.0, 3000));
        let r = run_tick(&cfg(true), &g, &l, now()).unwrap();
        assert_eq!(r.managed, 1);
        assert!(g.patches.borrow().is_empty());
    }
    #[test]
    fn probe_error_never_releases_an_existing_hold() {
        let (g, l) = rig();
        g.usage.borrow_mut().insert("c1".into(), usage(95.0, -1));
        let c = cfg(true);
        // hold placed with a future reset first
        g.usage.borrow_mut().insert("c1".into(), usage(95.0, 3000));
        run_tick(&c, &g, &l, now()).unwrap();
        g.usage.borrow_mut().remove("c1"); // probe now errors
        let r = run_tick(&c, &g, &l, now() + Duration::days(8)).unwrap();
        assert!(!r.probe_errors.is_empty());
        assert_eq!(g.get("c1")["isActive"], json!(false));
        assert_eq!(l.open_holds().unwrap().len(), 1);
    }
    #[test]
    fn restore_after_reset_on_fresh_data() {
        let (g, l) = rig();
        let c = cfg(true);
        g.usage.borrow_mut().insert("c1".into(), usage(95.0, 120));
        run_tick(&c, &g, &l, now()).unwrap();
        let later = now() + Duration::minutes(200);
        let fresh = json!({"quotas": {
            "session (5h)": {"used": 1, "total": 100, "remainingPercentage": 99, "resetAt": (later+Duration::hours(4)).to_rfc3339(), "observedAt": later.to_rfc3339()},
            "weekly (7d)": {"used": 2, "total": 100, "remainingPercentage": 98, "resetAt": (later+Duration::days(6)).to_rfc3339(), "observedAt": later.to_rfc3339()}}});
        g.usage.borrow_mut().insert("c1".into(), fresh);
        run_tick(&c, &g, &l, later).unwrap();
        assert_eq!(g.get("c1")["isActive"], json!(true));
        assert!(l.open_holds().unwrap().is_empty());
    }
    #[test]
    fn pause_file_freezes_writes() {
        let (g, l) = rig();
        let c = cfg(true);
        std::fs::create_dir_all(c.state_path()).unwrap();
        std::fs::write(pause_file(&c), "").unwrap();
        g.usage.borrow_mut().insert("c1".into(), usage(99.0, 3000));
        let r = run_tick(&c, &g, &l, now()).unwrap();
        assert_eq!(r.mode, "paused");
        assert!(g.patches.borrow().is_empty());
    }
    #[test]
    fn lock_is_exclusive() {
        let c = cfg(false);
        let a = try_lock(&c).unwrap();
        assert!(a.is_some());
        assert!(try_lock(&c).unwrap().is_none());
    }
    #[test]
    fn burn_rate_ignores_resets_and_flat_series() {
        let l = Ledger::memory().unwrap();
        let mk = |t: DateTime<Utc>, u: f64| crate::model::WindowObs { provider: "p".into(), connection_id: "c".into(), subscription: "s".into(), kind: WindowKind::FiveHour, group: None, used_pct: u, reset_at: None, observed_at: t };
        for (m, u) in [(0, 50.0), (15, 56.0), (30, 62.0)] {
            l.record_obs(now(), &mk(now() - Duration::minutes(30 - m), u)).unwrap();
        }
        let b = burn_rates(&l, "c", now());
        assert!((b[&WindowKind::FiveHour] - 0.4).abs() < 1e-6);
        let l2 = Ledger::memory().unwrap();
        for (m, u) in [(0, 90.0), (30, 5.0)] {
            l2.record_obs(now(), &mk(now() - Duration::minutes(30 - m), u)).unwrap();
        }
        assert!(burn_rates(&l2, "c", now()).is_empty());
    }
}
