//! Pure policy engine: observations + connection state + ledger holds in, actions out. No I/O.
//!
//! Rules (spec v0.6): one open hold per connection whose `window` is the strictest triggered window
//! (Monthly > Weekly > 5h), so a 5h expiry can never clear a weekly/monthly hold. Fail-closed asymmetry:
//! a stale-but-high reading may still hold; a stale-or-missing reading never releases. Foreign
//! (user) disables are never touched.
use crate::config::Thresholds;
use crate::ledger::HoldRow;
use crate::marker;
use crate::model::{Conn, WindowKind, WindowObs};
use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct ConnInput {
    pub conn: Conn,
    pub subscription: String,
    pub obs: Vec<WindowObs>,
    pub unavailable: Option<String>,
    /// Burn rate in percentage points per minute, by window, from the ledger (positive only).
    pub burn: BTreeMap<WindowKind, f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertKind {
    EvidenceMissing,
    StaleEvidence,
    WindowRolled,
    HoldExpired,
    MarkerMismatch,
    OrphanMarker,
    AllHeld,
    ManualOverride,
    PendingAbandoned,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Hold {
        conn_id: String,
        subscription: String,
        window: WindowKind,
        reset_at: Option<DateTime<Utc>>,
        max_until: DateTime<Utc>,
        predicted: bool,
        reason: String,
    },
    /// Change the window / reset of an open hold (escalate, de-escalate, or refresh reset time).
    Retarget {
        hold_id: String,
        conn_id: String,
        window: WindowKind,
        reset_at: Option<DateTime<Utc>>,
        max_until: DateTime<Utc>,
        reason: String,
    },
    /// Release the hold; `reenable` only when Sentinel owns it and its prior state was active.
    Release { hold_id: String, conn_id: String, reenable: bool, reason: String },
    /// The user re-enabled a held connection: release without touching it, suppress re-holding.
    ManualOverride { hold_id: String, conn_id: String, until: DateTime<Utc>, reason: String },
    /// Intent was logged but marker/disable were not verified (crash window): redo idempotently.
    ResumeApply { hold_id: String, conn_id: String },
    ClearMarker { conn_id: String },
    Alert { kind: AlertKind, conn_id: Option<String>, message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Freshness {
    Fresh,
    Suspect,
    Stale,
}

#[derive(Debug, Clone)]
struct Eval {
    used: f64,
    reset_at: Option<DateTime<Utc>>,
    fresh: Freshness,
}

fn default_len(w: WindowKind) -> Duration {
    match w {
        WindowKind::FiveHour => Duration::hours(5),
        WindowKind::Weekly => Duration::days(7),
        WindowKind::Monthly => Duration::days(31),
    }
}

fn freshness(age: Duration, th: &Thresholds) -> Freshness {
    if age <= Duration::minutes(th.fresh_minutes) {
        Freshness::Fresh
    } else if age <= Duration::minutes(th.stale_minutes) {
        Freshness::Suspect
    } else {
        Freshness::Stale
    }
}

/// Worst-of per window kind across groups/duplicates.
fn evaluate(c: &ConnInput, now: DateTime<Utc>, th: &Thresholds) -> BTreeMap<WindowKind, Eval> {
    let mut m: BTreeMap<WindowKind, Eval> = BTreeMap::new();
    for o in &c.obs {
        let e = Eval { used: o.used_pct, reset_at: o.reset_at, fresh: freshness(now - o.observed_at, th) };
        match m.get(&o.kind) {
            Some(cur) if cur.used >= e.used => {}
            _ => {
                m.insert(o.kind, e);
            }
        }
    }
    m
}

/// A refreshed reset time only counts as a change beyond jitter; otherwise we would rewrite the marker every tick
/// and could postpone release indefinitely.
fn reset_moved(old: Option<DateTime<Utc>>, new: Option<DateTime<Utc>>, th: &Thresholds) -> bool {
    match (old, new) {
        (Some(a), Some(b)) => (a - b).num_minutes().abs() > 2 * th.grace_minutes,
        (None, Some(_)) => true,
        _ => false,
    }
}

fn max_until(w: WindowKind, reset: Option<DateTime<Utc>>, now: DateTime<Utc>, th: &Thresholds) -> DateTime<Utc> {
    reset.unwrap_or(now + default_len(w)) + Duration::minutes(th.grace_minutes) + Duration::hours(th.max_hold_extra_hours)
}

/// Retargeting may never push a hold past one window length from when it was placed, so repeated large reset
/// shifts cannot postpone the expiry alarm forever.
fn bounded_max_until(w: WindowKind, reset: Option<DateTime<Utc>>, now: DateTime<Utc>, placed_at: DateTime<Utc>, th: &Thresholds) -> DateTime<Utc> {
    let cap = placed_at + default_len(w) + Duration::minutes(th.grace_minutes) + Duration::hours(th.max_hold_extra_hours);
    max_until(w, reset, now, th).min(cap)
}

struct Trigger {
    kind: WindowKind,
    reset_at: Option<DateTime<Utc>>,
    predicted: bool,
    detail: String,
}

/// Strictest window that should be held now. Rolled windows (reset already passed) are not trusted.
fn strictest_trigger(c: &ConnInput, ev: &BTreeMap<WindowKind, Eval>, now: DateTime<Utc>, th: &Thresholds, alerts: &mut Vec<Action>) -> Option<Trigger> {
    let horizon = (th.tick_minutes + th.latency_minutes) as f64;
    let mut best: Option<Trigger> = None;
    for (kind, e) in ev {
        if e.reset_at.is_some_and(|r| r <= now) {
            alerts.push(Action::Alert {
                kind: AlertKind::WindowRolled,
                conn_id: Some(c.conn.id.clone()),
                message: format!("{} {} reading predates its reset; not trusted", c.subscription, kind.as_str()),
            });
            continue;
        }
        let extra = if e.fresh == Freshness::Fresh { 0.0 } else { th.suspect_extra_margin_pct };
        let reactive = e.used + extra >= th.block_pct;
        let burn = c.burn.get(kind).copied().unwrap_or(0.0);
        let predicted = !reactive && burn > 0.0 && e.used + extra + burn * horizon + th.guard_margin_pct >= th.block_pct;
        if reactive || predicted {
            let t = Trigger {
                kind: *kind,
                reset_at: e.reset_at,
                predicted: !reactive,
                detail: format!(
                    "{} {}: used {:.1}%{}{}",
                    c.subscription,
                    kind.as_str(),
                    e.used,
                    if extra > 0.0 { format!(" (+{extra:.0} margin, evidence {:?})", e.fresh) } else { String::new() },
                    if predicted { format!(", projected {:.1}% in {horizon:.0} min", e.used + burn * horizon + th.guard_margin_pct) } else { String::new() }
                ),
            };
            if best.as_ref().is_none_or(|b| t.kind > b.kind) {
                best = Some(t);
            }
        }
    }
    best
}

pub struct PolicyInput<'a> {
    pub now: DateTime<Utc>,
    pub th: &'a Thresholds,
    pub conns: &'a [ConnInput],
    pub holds: &'a [HoldRow],
    pub overrides: &'a BTreeMap<String, DateTime<Utc>>,
}

/// Subscription-level evidence: connections that share one subscription (for example grok-cli and xai-oauth) must
/// be held and released together, so each is judged on the WORST reading any sibling reports per window, with the
/// OLDEST observation time. A sibling that reports nothing inherits its siblings' readings; a lone connection is
/// unchanged. This removes the "one row held, the other keeps serving" partial-hold gap.
fn merge_siblings(conns: &[ConnInput]) -> Vec<ConnInput> {
    let mut by_sub: BTreeMap<&str, BTreeMap<WindowKind, WindowObs>> = BTreeMap::new();
    let mut burn: BTreeMap<&str, BTreeMap<WindowKind, f64>> = BTreeMap::new();
    for c in conns {
        let m = by_sub.entry(c.subscription.as_str()).or_default();
        for o in &c.obs {
            match m.get_mut(&o.kind) {
                Some(cur) => {
                    let oldest = cur.observed_at.min(o.observed_at);
                    if o.used_pct > cur.used_pct {
                        *cur = o.clone();
                    }
                    cur.observed_at = oldest;
                }
                None => {
                    m.insert(o.kind, o.clone());
                }
            }
        }
        let b = burn.entry(c.subscription.as_str()).or_default();
        for (k, v) in &c.burn {
            let e = b.entry(*k).or_insert(0.0);
            if *v > *e {
                *e = *v;
            }
        }
    }
    conns
        .iter()
        .map(|c| {
            let mut n = c.clone();
            let merged = &by_sub[c.subscription.as_str()];
            if !merged.is_empty() {
                n.obs = merged
                    .values()
                    .map(|o| WindowObs { provider: c.conn.provider.clone(), connection_id: c.conn.id.clone(), group: None, ..o.clone() })
                    .collect();
                n.unavailable = None;
            }
            n.burn = burn[c.subscription.as_str()].clone();
            n
        })
        .collect()
}

pub fn decide(inp: &PolicyInput) -> Vec<Action> {
    let (now, th) = (inp.now, inp.th);
    let mut out: Vec<Action> = Vec::new();
    let mut held_conns: Vec<&str> = Vec::new();
    let merged = merge_siblings(inp.conns);

    for c in &merged {
        let id = c.conn.id.as_str();
        let hold = inp.holds.iter().find(|h| h.connection_id == id);
        let marker = marker::read(&c.conn.provider_specific_data);
        let ev = evaluate(c, now, th);
        let mut alerts = Vec::new();

        if let Some(h) = hold {
            held_conns.push(id);
            decide_with_hold(c, h, marker.as_ref().map(|m| m.id.as_str()), &ev, now, th, &mut out, &mut alerts);
        } else {
            decide_without_hold(c, marker.is_some(), &ev, inp.overrides.get(id).copied(), now, th, &mut out, &mut alerts);
            if out.iter().any(|a| matches!(a, Action::Hold { conn_id, .. } if conn_id == id)) {
                held_conns.push(id);
            }
        }
        out.extend(alerts);
    }

    // All subscriptions held => only free/local rungs remain; make that explicit (spec capacity floor).
    let mut subs: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for c in &merged {
        let e = subs.entry(c.subscription.as_str()).or_default();
        e.0 += 1;
        if held_conns.contains(&c.conn.id.as_str()) {
            e.1 += 1;
        }
    }
    if !subs.is_empty() && subs.values().all(|(n, h)| n == h) {
        out.push(Action::Alert { kind: AlertKind::AllHeld, conn_id: None, message: "every managed subscription is held; only free/local rungs remain".into() });
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn decide_with_hold(
    c: &ConnInput, h: &HoldRow, marker_id: Option<&str>, ev: &BTreeMap<WindowKind, Eval>, now: DateTime<Utc>,
    th: &Thresholds, out: &mut Vec<Action>, alerts: &mut Vec<Action>,
) {
    let conn_id = c.conn.id.clone();

    if !h.applied {
        // Crash window: intent logged, not verified. Redo only while still warranted.
        if strictest_trigger(c, ev, now, th, alerts).is_some() {
            // Active + unmarked + old is indistinguishable from "the user re-enabled it": never re-apply over that.
            // A negative age (clock stepped back) is NOT young: ambiguity resolves toward user intent.
            let age = now - h.placed_at;
            let young = age >= Duration::zero() && age <= Duration::minutes(2 * th.tick_minutes);
            if c.conn.is_active && marker_id.is_none() && !young {
                out.push(Action::ManualOverride { hold_id: h.id.clone(), conn_id: conn_id.clone(), until: now + Duration::hours(th.override_hours), reason: "pending hold is old and the connection is active: treating as a manual re-enable".into() });
                alerts.push(Action::Alert { kind: AlertKind::ManualOverride, conn_id: Some(conn_id), message: format!("{}: ambiguous pending hold treated as manual override", c.subscription) });
                return;
            }
            out.push(Action::ResumeApply { hold_id: h.id.clone(), conn_id });
        } else {
            // reenable only takes effect if the live marker still proves the disable is ours (checked by the actuator).
            out.push(Action::Release { hold_id: h.id.clone(), conn_id: conn_id.clone(), reenable: h.prior_active, reason: "pending hold no longer warranted".into() });
            alerts.push(Action::Alert { kind: AlertKind::PendingAbandoned, conn_id: Some(conn_id), message: "unverified hold abandoned".into() });
        }
        return;
    }

    if c.conn.is_active && marker_id.is_none() {
        // Either our own release landed but the ledger write did not (crash window), or the user re-enabled and
        // cleared it. Our release only ever happens on fresh, below-threshold evidence, so: if that is not true
        // now, this was not us.
        let ours = !ev.is_empty()
            && ev.values().all(|e| e.fresh == Freshness::Fresh)
            && strictest_trigger(c, ev, now, th, &mut Vec::new()).is_none();
        if ours {
            out.push(Action::Release { hold_id: h.id.clone(), conn_id, reenable: false, reason: "release already applied; closing ledger".into() });
        } else {
            out.push(Action::ManualOverride { hold_id: h.id.clone(), conn_id: conn_id.clone(), until: now + Duration::hours(th.override_hours), reason: "connection active without our marker while still exhausted: manual re-enable".into() });
            alerts.push(Action::Alert { kind: AlertKind::ManualOverride, conn_id: Some(conn_id), message: format!("{} re-enabled manually; not re-holding for {}h", c.subscription, th.override_hours) });
        }
        return;
    }
    if c.conn.is_active {
        out.push(Action::ManualOverride {
            hold_id: h.id.clone(),
            conn_id: conn_id.clone(),
            until: now + Duration::hours(th.override_hours),
            reason: "connection re-enabled outside Sentinel".into(),
        });
        alerts.push(Action::Alert { kind: AlertKind::ManualOverride, conn_id: Some(conn_id), message: format!("{} re-enabled manually; not re-holding for {}h", c.subscription, th.override_hours) });
        return;
    }
    if marker_id != Some(h.id.as_str()) {
        alerts.push(Action::Alert { kind: AlertKind::MarkerMismatch, conn_id: Some(conn_id), message: "ownership marker missing or different; treating as foreign, will not restore".into() });
        return;
    }
    if now > h.max_until {
        alerts.push(Action::Alert { kind: AlertKind::HoldExpired, conn_id: Some(conn_id.clone()), message: format!("{} hold exceeded its maximum duration; needs attention", c.subscription) });
    }
    if ev.is_empty() {
        alerts.push(Action::Alert {
            kind: AlertKind::EvidenceMissing,
            conn_id: Some(conn_id),
            message: format!("no quota evidence ({}); hold kept", c.unavailable.as_deref().unwrap_or("none")),
        });
        return;
    }

    let needed = strictest_trigger(c, ev, now, th, alerts);
    // Escalate / refresh while the quota is still exhausted.
    if let Some(t) = &needed {
        if t.kind > h.window || (t.kind == h.window && reset_moved(h.reset_at, t.reset_at, th)) {
            out.push(Action::Retarget {
                hold_id: h.id.clone(),
                conn_id: conn_id.clone(),
                window: t.kind,
                reset_at: t.reset_at,
                max_until: bounded_max_until(t.kind, t.reset_at, now, h.placed_at, th),
                reason: t.detail.clone(),
            });
        }
        if t.kind >= h.window {
            return;
        }
    }

    // Release evidence for the hold's window. Never on stale/suspect evidence, never on missing evidence.
    let all_fresh = ev.values().all(|e| e.fresh == Freshness::Fresh);
    let Some(eh) = ev.get(&h.window) else {
        alerts.push(Action::Alert { kind: AlertKind::EvidenceMissing, conn_id: Some(conn_id), message: format!("no {} reading; hold kept", h.window.as_str()) });
        return;
    };
    if !all_fresh {
        alerts.push(Action::Alert { kind: AlertKind::StaleEvidence, conn_id: Some(conn_id), message: "evidence not fresh; hold kept".into() });
        return;
    }
    let dwell_ok = now - h.placed_at >= Duration::minutes(th.dwell_minutes);
    let reset_passed = h.reset_at.is_some_and(|r| now >= r + Duration::minutes(th.grace_minutes));
    let after_reset = reset_passed && eh.used < th.block_pct;
    let early_5h = !h.window.is_long() && dwell_ok && eh.used < th.restore_pct;
    let unknown_reset_long = h.window.is_long() && h.reset_at.is_none() && dwell_ok && eh.used < th.restore_pct;
    if !(after_reset || early_5h || unknown_reset_long) {
        return;
    }

    // The hold's own window has cleared. If a lower window is still exhausted, narrow the hold instead.
    if let Some(t) = needed {
        out.push(Action::Retarget {
            hold_id: h.id.clone(),
            conn_id,
            window: t.kind,
            reset_at: t.reset_at,
            max_until: bounded_max_until(t.kind, t.reset_at, now, h.placed_at, th),
            reason: format!("{} cleared; {}", h.window.as_str(), t.detail),
        });
        return;
    }
    out.push(Action::Release {
        hold_id: h.id.clone(),
        conn_id,
        reenable: h.prior_active,
        reason: format!("{} window cleared on fresh evidence (used {:.1}%)", h.window.as_str(), eh.used),
    });
}

#[allow(clippy::too_many_arguments)]
fn decide_without_hold(
    c: &ConnInput, has_marker: bool, ev: &BTreeMap<WindowKind, Eval>, override_until: Option<DateTime<Utc>>,
    now: DateTime<Utc>, th: &Thresholds, out: &mut Vec<Action>, alerts: &mut Vec<Action>,
) {
    let conn_id = c.conn.id.clone();
    if has_marker {
        alerts.push(Action::Alert { kind: AlertKind::OrphanMarker, conn_id: Some(conn_id.clone()), message: "ownership marker without a ledger hold".into() });
        if c.conn.is_active {
            out.push(Action::ClearMarker { conn_id });
        }
        return;
    }
    if !c.conn.is_active {
        return; // foreign / never-activated: never touch
    }
    if override_until.is_some_and(|u| u > now) {
        return;
    }
    if ev.is_empty() {
        alerts.push(Action::Alert {
            kind: AlertKind::EvidenceMissing,
            conn_id: Some(conn_id),
            message: format!("{}: no quota evidence ({})", c.subscription, c.unavailable.as_deref().unwrap_or("none")),
        });
        return;
    }
    if ev.values().any(|e| e.fresh == Freshness::Stale) {
        alerts.push(Action::Alert { kind: AlertKind::StaleEvidence, conn_id: Some(conn_id.clone()), message: format!("{}: evidence older than {} min", c.subscription, th.stale_minutes) });
    }
    if let Some(t) = strictest_trigger(c, ev, now, th, alerts) {
        out.push(Action::Hold {
            conn_id,
            subscription: c.subscription.clone(),
            window: t.kind,
            reset_at: t.reset_at,
            max_until: max_until(t.kind, t.reset_at, now, th),
            predicted: t.predicted,
            reason: t.detail,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 7, 14, 0, 0).unwrap()
    }
    fn conn(id: &str, active: bool, psd: serde_json::Value) -> Conn {
        Conn { id: id.into(), provider: "claude".into(), name: "n".into(), is_active: active, provider_specific_data: psd }
    }
    fn ob(kind: WindowKind, used: f64, reset_in_min: i64, age_min: i64) -> WindowObs {
        WindowObs {
            provider: "claude".into(), connection_id: "c1".into(), subscription: "claude".into(), kind, group: None,
            used_pct: used, reset_at: Some(now() + Duration::minutes(reset_in_min)), observed_at: now() - Duration::minutes(age_min),
        }
    }
    fn ci(active: bool, psd: serde_json::Value, obs: Vec<WindowObs>) -> ConnInput {
        ConnInput { conn: conn("c1", active, psd), subscription: "claude".into(), obs, unavailable: None, burn: BTreeMap::new() }
    }
    fn marker_psd(id: &str) -> serde_json::Value {
        json!({"qsHold": {"id": id, "window": "weekly", "resetAt": null, "priorActive": true}})
    }
    fn hold(w: WindowKind, reset_in_min: Option<i64>, placed_ago_min: i64, applied: bool, prior: bool) -> HoldRow {
        HoldRow {
            id: "h1".into(), connection_id: "c1".into(), subscription: "claude".into(), window: w, reason: "r".into(), prior_active: prior,
            reset_at: reset_in_min.map(|m| now() + Duration::minutes(m)), max_until: now() + Duration::hours(40),
            placed_at: now() - Duration::minutes(placed_ago_min), released_at: None, release_reason: None, applied,
        }
    }
    fn run(cs: Vec<ConnInput>, hs: Vec<HoldRow>) -> Vec<Action> {
        let th = Thresholds::default();
        decide(&PolicyInput { now: now(), th: &th, conns: &cs, holds: &hs, overrides: &BTreeMap::new() })
    }
    fn has(a: &[Action], f: impl Fn(&Action) -> bool) -> bool {
        a.iter().any(f)
    }

    #[test]
    fn below_threshold_no_action() {
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::FiveHour, 50.0, 100, 1), ob(WindowKind::Weekly, 60.0, 5000, 1)])], vec![]);
        assert!(!has(&a, |x| matches!(x, Action::Hold { .. })));
    }
    #[test]
    fn weekly_90_holds_weekly() {
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 90.0, 5000, 1)])], vec![]);
        assert!(has(&a, |x| matches!(x, Action::Hold { window: WindowKind::Weekly, predicted: false, .. })));
    }
    #[test]
    fn strictest_window_wins_when_both_trigger() {
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::FiveHour, 95.0, 60, 1), ob(WindowKind::Monthly, 100.0, 9000, 1)])], vec![]);
        assert!(has(&a, |x| matches!(x, Action::Hold { window: WindowKind::Monthly, .. })));
    }
    #[test]
    fn five_hour_hold_has_five_hour_reset_not_weekly() {
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::FiveHour, 92.0, 60, 1), ob(WindowKind::Weekly, 10.0, 5000, 1)])], vec![]);
        let Some(Action::Hold { window, reset_at, .. }) = a.iter().find(|x| matches!(x, Action::Hold { .. })) else { panic!("no hold") };
        assert_eq!(*window, WindowKind::FiveHour);
        assert_eq!(*reset_at, Some(now() + Duration::minutes(60)));
    }
    #[test]
    fn predictive_hold_before_90() {
        let mut c = ci(true, json!({}), vec![ob(WindowKind::FiveHour, 80.0, 100, 1)]);
        c.burn.insert(WindowKind::FiveHour, 0.6); // 0.6 pt/min * 20 min = 12 => 94
        let a = run(vec![c], vec![]);
        assert!(has(&a, |x| matches!(x, Action::Hold { predicted: true, .. })));
    }
    #[test]
    fn no_predictive_hold_when_burn_small() {
        let mut c = ci(true, json!({}), vec![ob(WindowKind::FiveHour, 50.0, 100, 1)]);
        c.burn.insert(WindowKind::FiveHour, 0.1);
        assert!(!has(&run(vec![c], vec![]), |x| matches!(x, Action::Hold { .. })));
    }
    #[test]
    fn suspect_evidence_adds_margin() {
        // 86% used, 40 min old (suspect) -> +5 margin => 91 => hold
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 86.0, 5000, 40)])], vec![]);
        assert!(has(&a, |x| matches!(x, Action::Hold { .. })));
    }
    #[test]
    fn stale_low_reading_does_not_hold_but_alerts() {
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 40.0, 5000, 90)])], vec![]);
        assert!(!has(&a, |x| matches!(x, Action::Hold { .. })));
        assert!(has(&a, |x| matches!(x, Action::Alert { kind: AlertKind::StaleEvidence, .. })));
    }
    #[test]
    fn stale_high_reading_still_holds_fail_closed() {
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 95.0, 5000, 90)])], vec![]);
        assert!(has(&a, |x| matches!(x, Action::Hold { .. })));
    }
    #[test]
    fn rolled_window_reading_is_not_trusted() {
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 99.0, -10, 1)])], vec![]);
        assert!(!has(&a, |x| matches!(x, Action::Hold { .. })));
        assert!(has(&a, |x| matches!(x, Action::Alert { kind: AlertKind::WindowRolled, .. })));
    }
    #[test]
    fn missing_evidence_alerts_no_hold() {
        let mut c = ci(true, json!({}), vec![]);
        c.unavailable = Some("Usage not available".into());
        let a = run(vec![c], vec![]);
        assert!(has(&a, |x| matches!(x, Action::Alert { kind: AlertKind::EvidenceMissing, .. })));
    }
    #[test]
    fn foreign_disable_never_touched() {
        let a = run(vec![ci(false, json!({}), vec![ob(WindowKind::Weekly, 99.0, 5000, 1)])], vec![]);
        assert!(a.iter().all(|x| !matches!(x, Action::Hold { .. } | Action::Release { .. } | Action::ClearMarker { .. })));
    }
    #[test]
    fn override_suppresses_new_hold() {
        let th = Thresholds::default();
        let cs = vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 99.0, 5000, 1)])];
        let mut ov = BTreeMap::new();
        ov.insert("c1".to_string(), now() + Duration::hours(3));
        let a = decide(&PolicyInput { now: now(), th: &th, conns: &cs, holds: &[], overrides: &ov });
        assert!(!has(&a, |x| matches!(x, Action::Hold { .. })));
    }
    #[test]
    fn weekly_hold_not_released_before_reset_even_if_usage_drops() {
        let h = hold(WindowKind::Weekly, Some(300), 600, true, true);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::Weekly, 5.0, 300, 1)])], vec![h]);
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
    }
    #[test]
    fn weekly_hold_released_after_reset_on_fresh_evidence() {
        let h = hold(WindowKind::Weekly, Some(-30), 4000, true, true);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::Weekly, 3.0, 9000, 2)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Release { reenable: true, .. })));
    }
    #[test]
    fn reset_passed_but_still_high_is_not_released() {
        let h = hold(WindowKind::Weekly, Some(-30), 4000, true, true);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::Weekly, 95.0, 9000, 2)])], vec![h]);
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
    }
    #[test]
    fn reset_passed_but_stale_evidence_keeps_hold() {
        let h = hold(WindowKind::Weekly, Some(-30), 4000, true, true);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::Weekly, 3.0, 9000, 120)])], vec![h]);
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
        assert!(has(&a, |x| matches!(x, Action::Alert { kind: AlertKind::StaleEvidence, .. })));
    }
    #[test]
    fn missing_evidence_never_releases() {
        let h = hold(WindowKind::Weekly, Some(-30), 4000, true, true);
        let mut c = ci(false, marker_psd("h1"), vec![]);
        c.unavailable = Some("x".into());
        let a = run(vec![c], vec![h]);
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
    }
    #[test]
    fn five_hour_early_restore_needs_dwell_and_below_restore_pct() {
        let h = hold(WindowKind::FiveHour, Some(120), 10, true, true); // dwell not met
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::FiveHour, 30.0, 120, 1)])], vec![h]);
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
        let h = hold(WindowKind::FiveHour, Some(120), 60, true, true);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::FiveHour, 30.0, 120, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Release { reenable: true, .. })));
    }
    #[test]
    fn five_hour_expiry_cannot_clear_weekly_hold() {
        // hold is weekly; 5h window reset passed and is low, weekly still high+unreset
        let h = hold(WindowKind::Weekly, Some(3000), 400, true, true);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::FiveHour, 1.0, 200, 1), ob(WindowKind::Weekly, 95.0, 3000, 1)])], vec![h]);
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
    }
    #[test]
    fn weekly_cleared_but_5h_still_high_retargets_not_releases() {
        let h = hold(WindowKind::Weekly, Some(-30), 4000, true, true);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::Weekly, 3.0, 9000, 1), ob(WindowKind::FiveHour, 96.0, 100, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Retarget { window: WindowKind::FiveHour, .. })));
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
    }
    #[test]
    fn five_hour_hold_escalates_to_weekly() {
        let h = hold(WindowKind::FiveHour, Some(60), 20, true, true);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::FiveHour, 95.0, 60, 1), ob(WindowKind::Weekly, 91.0, 4000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Retarget { window: WindowKind::Weekly, .. })));
    }
    #[test]
    fn old_pending_hold_on_active_unmarked_connection_is_override_not_reapplied() {
        let h = hold(WindowKind::Weekly, Some(3000), 120, false, true); // 2h old, never verified
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 95.0, 3000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::ManualOverride { .. })));
        assert!(!has(&a, |x| matches!(x, Action::ResumeApply { .. })));
    }
    #[test]
    fn user_reenable_and_clear_while_still_exhausted_is_override_not_ledger_close() {
        let h = hold(WindowKind::Weekly, Some(3000), 400, true, true);
        let a = run(vec![ci(true, json!({"qsHold": null}), vec![ob(WindowKind::Weekly, 95.0, 3000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::ManualOverride { .. })));
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
    }
    #[test]
    fn active_unmarked_with_no_evidence_is_not_assumed_to_be_our_release() {
        let h = hold(WindowKind::Weekly, Some(3000), 400, true, true);
        let mut c = ci(true, json!({}), vec![]);
        c.unavailable = Some("x".into());
        let a = run(vec![c], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::ManualOverride { .. })));
    }
    #[test]
    fn reset_time_jitter_does_not_retarget_but_real_move_does() {
        let h = hold(WindowKind::Weekly, Some(3000), 400, true, true);
        let jitter = ob(WindowKind::Weekly, 95.0, 3000, 1);
        let mut j = jitter.clone();
        j.reset_at = Some(now() + Duration::minutes(3000) + Duration::seconds(30));
        let a = run(vec![ci(false, marker_psd("h1"), vec![j])], vec![h.clone()]);
        assert!(!has(&a, |x| matches!(x, Action::Retarget { .. })));
        let moved = ob(WindowKind::Weekly, 95.0, 3000 + 600, 1);
        let a = run(vec![ci(false, marker_psd("h1"), vec![moved])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Retarget { .. })));
    }
    #[test]
    fn negative_hold_age_after_clock_step_back_is_not_young() {
        let h = hold(WindowKind::Weekly, Some(3000), -30, false, true); // placed 30 min in the "future"
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 95.0, 3000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::ManualOverride { .. })));
        assert!(!has(&a, |x| matches!(x, Action::ResumeApply { .. })));
    }
    #[test]
    fn repeated_reset_shifts_cannot_extend_hold_beyond_one_window_from_placement() {
        let h = hold(WindowKind::FiveHour, Some(60), 290, true, true); // placed 4h50m ago
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::FiveHour, 96.0, 600, 1)])], vec![h.clone()]);
        let Some(Action::Retarget { max_until, .. }) = a.iter().find(|x| matches!(x, Action::Retarget { .. })) else { panic!("no retarget") };
        let cap = h.placed_at + Duration::hours(5) + Duration::minutes(5) + Duration::hours(2);
        assert!(*max_until <= cap, "{max_until} > {cap}");
    }
    fn ci_n(id: &str, sub: &str, active: bool, psd: serde_json::Value, obs: Vec<WindowObs>) -> ConnInput {
        let mut c = ci(active, psd, obs);
        c.conn.id = id.into();
        c.subscription = sub.into();
        c
    }
    fn holds_for(a: &[Action]) -> Vec<String> {
        let mut v: Vec<String> = a.iter().filter_map(|x| if let Action::Hold { conn_id, .. } = x { Some(conn_id.clone()) } else { None }).collect();
        v.sort();
        v
    }
    #[test]
    fn siblings_of_one_subscription_are_held_together_even_if_only_one_reports_high() {
        let hi = vec![ob(WindowKind::Weekly, 95.0, 3000, 1)];
        let lo = vec![ob(WindowKind::Weekly, 40.0, 3000, 1)];
        let a = run(vec![ci_n("g1", "grok", true, json!({}), hi), ci_n("g2", "grok", true, json!({}), lo)], vec![]);
        assert_eq!(holds_for(&a), vec!["g1".to_string(), "g2".to_string()]);
    }
    #[test]
    fn sibling_without_usage_inherits_the_other_siblings_reading() {
        let hi = vec![ob(WindowKind::Weekly, 95.0, 3000, 1)];
        let mut none = ci_n("g2", "grok", true, json!({}), vec![]);
        none.unavailable = Some("Usage not available".into());
        let a = run(vec![ci_n("g1", "grok", true, json!({}), hi), none], vec![]);
        assert_eq!(holds_for(&a), vec!["g1".to_string(), "g2".to_string()]);
        assert!(!has(&a, |x| matches!(x, Action::Alert { kind: AlertKind::EvidenceMissing, .. })));
    }
    #[test]
    fn different_subscriptions_never_influence_each_other() {
        let hi = vec![ob(WindowKind::Weekly, 95.0, 3000, 1)];
        let lo = vec![ob(WindowKind::Weekly, 10.0, 3000, 1)];
        let a = run(vec![ci_n("c1", "claude", true, json!({}), hi), ci_n("x1", "chatgpt", true, json!({}), lo)], vec![]);
        assert_eq!(holds_for(&a), vec!["c1".to_string()]);
    }
    #[test]
    fn siblings_release_together_on_worst_case_fresh_evidence() {
        let h1 = HoldRow { connection_id: "g1".into(), id: "h1".into(), ..hold(WindowKind::Weekly, Some(-30), 4000, true, true) };
        let h2 = HoldRow { connection_id: "g2".into(), id: "h2".into(), ..hold(WindowKind::Weekly, Some(-30), 4000, true, true) };
        let lo = || vec![ob(WindowKind::Weekly, 3.0, 9000, 1)];
        let a = run(vec![ci_n("g1", "grok", false, marker_psd("h1"), lo()), ci_n("g2", "grok", false, marker_psd("h2"), lo())], vec![h1.clone(), h2.clone()]);
        assert_eq!(a.iter().filter(|x| matches!(x, Action::Release { reenable: true, .. })).count(), 2);
        // if ONE sibling still reports high after the reset, NEITHER is released
        let a = run(vec![ci_n("g1", "grok", false, marker_psd("h1"), lo()), ci_n("g2", "grok", false, marker_psd("h2"), vec![ob(WindowKind::Weekly, 95.0, 9000, 1)])], vec![h1, h2]);
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
    }
    #[test]
    fn user_override_of_one_sibling_does_not_release_the_other() {
        let h1 = HoldRow { connection_id: "g1".into(), id: "h1".into(), ..hold(WindowKind::Weekly, Some(3000), 400, true, true) };
        let h2 = HoldRow { connection_id: "g2".into(), id: "h2".into(), ..hold(WindowKind::Weekly, Some(3000), 400, true, true) };
        let hi = || vec![ob(WindowKind::Weekly, 95.0, 3000, 1)];
        let a = run(vec![ci_n("g1", "grok", true, marker_psd("h1"), hi()), ci_n("g2", "grok", false, marker_psd("h2"), hi())], vec![h1, h2]);
        assert!(has(&a, |x| matches!(x, Action::ManualOverride { conn_id, .. } if conn_id == "g1")));
        assert!(!has(&a, |x| matches!(x, Action::Release { conn_id, .. } if conn_id == "g2")));
    }
    #[test]
    fn user_reenable_is_manual_override_not_fought() {
        let h = hold(WindowKind::Weekly, Some(3000), 400, true, true);
        let a = run(vec![ci(true, marker_psd("h1"), vec![ob(WindowKind::Weekly, 95.0, 3000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::ManualOverride { .. })));
        assert!(!has(&a, |x| matches!(x, Action::Hold { .. })));
    }
    #[test]
    fn marker_mismatch_blocks_restore() {
        let h = hold(WindowKind::Weekly, Some(-30), 4000, true, true);
        let a = run(vec![ci(false, marker_psd("other"), vec![ob(WindowKind::Weekly, 3.0, 9000, 1)])], vec![h]);
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
        assert!(has(&a, |x| matches!(x, Action::Alert { kind: AlertKind::MarkerMismatch, .. })));
    }
    #[test]
    fn prior_inactive_hold_never_reenables() {
        let h = hold(WindowKind::Weekly, Some(-30), 4000, true, false);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::Weekly, 3.0, 9000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Release { reenable: false, .. })));
    }
    #[test]
    fn unapplied_hold_resumes_when_still_warranted() {
        let h = hold(WindowKind::Weekly, Some(3000), 5, false, true);
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 95.0, 3000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::ResumeApply { .. })));
        assert!(!has(&a, |x| matches!(x, Action::ManualOverride { .. })));
    }
    #[test]
    fn unapplied_hold_abandoned_when_not_warranted() {
        let h = hold(WindowKind::Weekly, Some(3000), 500, false, true);
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 10.0, 3000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Release { reenable: true, .. })));
    }
    #[test]
    fn active_without_marker_and_open_hold_is_crash_recovery_not_override() {
        let h = hold(WindowKind::Weekly, Some(-30), 4000, true, true);
        let a = run(vec![ci(true, json!({"qsHold": null}), vec![ob(WindowKind::Weekly, 3.0, 9000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Release { reenable: false, .. })));
        assert!(!has(&a, |x| matches!(x, Action::ManualOverride { .. })));
    }
    #[test]
    fn orphan_marker_on_active_connection_is_cleared() {
        let a = run(vec![ci(true, marker_psd("zzz"), vec![ob(WindowKind::Weekly, 10.0, 3000, 1)])], vec![]);
        assert!(has(&a, |x| matches!(x, Action::ClearMarker { .. })));
    }
    #[test]
    fn expired_hold_alerts_and_stays() {
        let mut h = hold(WindowKind::Weekly, Some(3000), 400, true, true);
        h.max_until = now() - Duration::hours(1);
        let a = run(vec![ci(false, marker_psd("h1"), vec![ob(WindowKind::Weekly, 95.0, 3000, 1)])], vec![h]);
        assert!(has(&a, |x| matches!(x, Action::Alert { kind: AlertKind::HoldExpired, .. })));
        assert!(!has(&a, |x| matches!(x, Action::Release { .. })));
    }
    #[test]
    fn all_subscriptions_held_is_explicit() {
        let a = run(vec![ci(true, json!({}), vec![ob(WindowKind::Weekly, 95.0, 5000, 1)])], vec![]);
        assert!(has(&a, |x| matches!(x, Action::Alert { kind: AlertKind::AllHeld, .. })));
    }
}
