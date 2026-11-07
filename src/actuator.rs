//! Applies policy [`Action`]s to the gateway. The gateway has no compare-and-set (verified), so every write is
//! re-read first, patches ONLY `isActive` and our own `qsHold` key, is read back afterwards, and aborts on any
//! disagreement. Shadow mode (not live for the subscription, or PAUSE file present) records and writes nothing.
use crate::gateway::Gateway;
use crate::ledger::{HoldRow, Ledger};
use crate::marker::{self, QsHold};
use crate::model::{Conn, WindowKind};
use crate::policy::Action;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Applied,
    Shadow,
    Aborted(String),
    Failed(String),
    Noted,
}

pub struct Actuator<'a> {
    pub gw: &'a dyn Gateway,
    pub ledger: &'a Ledger,
    pub now: DateTime<Utc>,
    /// conn id -> (subscription, live?)
    pub live: &'a BTreeMap<String, (String, bool)>,
    pub paused: bool,
    pub tick: i64,
}

fn new_id(now: DateTime<Utc>, conn: &str) -> String {
    format!("h{}-{}", now.format("%Y%m%dT%H%M%S"), &conn.chars().take(8).collect::<String>())
}

impl<'a> Actuator<'a> {
    fn is_live(&self, conn_id: &str) -> bool {
        !self.paused && self.live.get(conn_id).is_some_and(|(_, l)| *l)
    }
    fn sub(&self, conn_id: &str) -> Option<String> {
        self.live.get(conn_id).map(|(s, _)| s.clone())
    }
    fn log(&self, conn: Option<&str>, action: &str, w: Option<WindowKind>, reason: &str, applied: bool, detail: &str) {
        let sub = conn.and_then(|c| self.sub(c));
        let _ = self.ledger.record_decision(self.now, self.tick, conn, sub.as_deref(), action, w, reason, applied, detail);
    }

    fn read(&self, id: &str) -> Result<Option<Conn>> {
        Ok(self.gw.connection(id)?.and_then(|v| Conn::from_json(&v)))
    }

    /// PATCH, then verify by reading back. An error from PATCH is ambiguous (it may have applied), so we always read back.
    fn write_verified(&self, id: &str, body: &Value, ok: impl Fn(&Conn) -> bool) -> Result<bool> {
        let r = self.gw.patch_connection(id, body);
        let back = self.read(id)?;
        match (r, back) {
            (_, Some(c)) if ok(&c) => Ok(true),
            (Err(e), _) => Err(e),
            (Ok(()), _) => Ok(false),
        }
    }

    pub fn apply_all(&self, actions: &[Action]) -> Vec<(Action, Outcome)> {
        actions
            .iter()
            .map(|a| {
                let o = match self.apply(a) {
                    Ok(o) => o,
                    Err(e) => {
                        let m = format!("{e:#}");
                        self.log(action_conn(a), action_name(a), None, "error", false, &m);
                        Outcome::Failed(m)
                    }
                };
                (a.clone(), o)
            })
            .collect()
    }

    pub fn apply(&self, a: &Action) -> Result<Outcome> {
        match a {
            Action::Alert { kind, conn_id, message } => {
                self.log(conn_id.as_deref(), &format!("alert:{kind:?}"), None, message, false, "");
                Ok(Outcome::Noted)
            }
            Action::Hold { conn_id, subscription, window, reset_at, max_until, predicted, reason } => {
                let tag = if *predicted { "hold:predicted" } else { "hold" };
                if !self.is_live(conn_id) {
                    self.log(Some(conn_id), tag, Some(*window), reason, false, "shadow: would hold");
                    return Ok(Outcome::Shadow);
                }
                let Some(c) = self.read(conn_id)? else { return Ok(Outcome::Aborted("connection vanished".into())) };
                if !c.is_active || marker::read(&c.provider_specific_data).is_some() {
                    let m = "state changed since decision (inactive or already marked); aborting";
                    self.log(Some(conn_id), tag, Some(*window), m, false, "");
                    return Ok(Outcome::Aborted(m.into()));
                }
                let id = new_id(self.now, conn_id);
                // Last read immediately before the write to shrink (not close) the no-CAS race window.
                // Ledger intent first (applied=false) so a crash between here and the write is recoverable.
                self.ledger.insert_hold(&HoldRow {
                    id: id.clone(), connection_id: conn_id.clone(), subscription: subscription.clone(), window: *window,
                    reason: reason.clone(), prior_active: true, reset_at: *reset_at, max_until: *max_until, placed_at: self.now,
                    released_at: None, release_reason: None, applied: false,
                })?;
                match self.read(conn_id)? {
                    Some(c2) if c2.is_active && marker::read(&c2.provider_specific_data).is_none() => {}
                    _ => {
                        let m = "connection changed between read and write; aborting (pending hold will be reconciled)";
                        self.log(Some(conn_id), tag, Some(*window), m, false, &id);
                        return Ok(Outcome::Aborted(m.into()));
                    }
                }
                let m = QsHold { id: id.clone(), window: window.as_str().into(), reset_at: *reset_at, prior_active: true };
                let body = json!({"isActive": false, "providerSpecificData": {marker::KEY: marker::to_value(&m)}});
                let want = id.clone();
                let ok = self.write_verified(conn_id, &body, move |c| {
                    !c.is_active && marker::read(&c.provider_specific_data).is_some_and(|x| x.id == want)
                });
                match ok {
                    Ok(true) => {
                        self.ledger.mark_applied(&id)?;
                        self.log(Some(conn_id), tag, Some(*window), reason, true, &id);
                        Ok(Outcome::Applied)
                    }
                    Ok(false) => {
                        self.log(Some(conn_id), tag, Some(*window), "write not confirmed; will retry next tick", false, &id);
                        Ok(Outcome::Failed("write not confirmed".into()))
                    }
                    Err(e) => {
                        self.log(Some(conn_id), tag, Some(*window), "write failed; will retry next tick", false, &format!("{e:#}"));
                        Ok(Outcome::Failed(format!("{e:#}")))
                    }
                }
            }
            Action::ResumeApply { hold_id, conn_id } => {
                if !self.is_live(conn_id) {
                    self.log(Some(conn_id), "resume", None, "shadow", false, hold_id);
                    return Ok(Outcome::Shadow);
                }
                let Some(h) = self.ledger.open_holds()?.into_iter().find(|h| &h.id == hold_id) else { return Ok(Outcome::Noted) };
                let Some(c) = self.read(conn_id)? else { return Ok(Outcome::Aborted("connection vanished".into())) };
                let mk = marker::read(&c.provider_specific_data);
                match (c.is_active, mk) {
                    (false, Some(m)) if m.id == h.id => {
                        self.ledger.mark_applied(&h.id)?;
                        self.log(Some(conn_id), "resume", Some(h.window), "write had landed; ledger confirmed", true, hold_id);
                        Ok(Outcome::Applied)
                    }
                    (true, None) => {
                        let m = QsHold { id: h.id.clone(), window: h.window.as_str().into(), reset_at: h.reset_at, prior_active: h.prior_active };
                        let body = json!({"isActive": false, "providerSpecificData": {marker::KEY: marker::to_value(&m)}});
                        let want = h.id.clone();
                        let done = self.write_verified(conn_id, &body, move |c| !c.is_active && marker::read(&c.provider_specific_data).is_some_and(|x| x.id == want))?;
                        if done {
                            self.ledger.mark_applied(&h.id)?;
                        }
                        self.log(Some(conn_id), "resume", Some(h.window), "re-applied hold", done, hold_id);
                        Ok(if done { Outcome::Applied } else { Outcome::Failed("not confirmed".into()) })
                    }
                    _ => {
                        let m = "connection state does not match the pending hold (foreign change); abandoning, not touching";
                        self.ledger.release_hold(&h.id, self.now, "abandoned: foreign state")?;
                        self.log(Some(conn_id), "resume", Some(h.window), m, false, hold_id);
                        Ok(Outcome::Aborted(m.into()))
                    }
                }
            }
            Action::Retarget { hold_id, conn_id, window, reset_at, max_until, reason } => {
                if !self.is_live(conn_id) {
                    self.log(Some(conn_id), "retarget", Some(*window), reason, false, "shadow");
                    return Ok(Outcome::Shadow);
                }
                let Some(c) = self.read(conn_id)? else { return Ok(Outcome::Aborted("connection vanished".into())) };
                if marker::read(&c.provider_specific_data).is_none_or(|m| &m.id != hold_id) {
                    return Ok(Outcome::Aborted("marker mismatch; not retargeting".into()));
                }
                self.ledger.update_hold_window(hold_id, *window, *reset_at, *max_until)?;
                let m = QsHold { id: hold_id.clone(), window: window.as_str().into(), reset_at: *reset_at, prior_active: true };
                // Marker-only write. isActive is deliberately NOT part of the body.
                let body = json!({"providerSpecificData": {marker::KEY: marker::to_value(&m)}});
                let (hid, w) = (hold_id.clone(), window.as_str().to_string());
                let done = self.write_verified(conn_id, &body, move |c| marker::read(&c.provider_specific_data).is_some_and(|x| x.id == hid && x.window == w))?;
                self.log(Some(conn_id), "retarget", Some(*window), reason, done, hold_id);
                Ok(if done { Outcome::Applied } else { Outcome::Failed("not confirmed".into()) })
            }
            Action::Release { hold_id, conn_id, reenable, reason } => {
                if !self.is_live(conn_id) {
                    self.log(Some(conn_id), "release", None, reason, false, "shadow");
                    return Ok(Outcome::Shadow);
                }
                let Some(c) = self.read(conn_id)? else {
                    self.ledger.release_hold(hold_id, self.now, "connection vanished")?;
                    return Ok(Outcome::Aborted("connection vanished".into()));
                };
                let mk = marker::read(&c.provider_specific_data);
                let ours = mk.as_ref().is_some_and(|m| &m.id == hold_id);
                if c.is_active && mk.is_none() {
                    // Release already landed (crash window) or the user cleared everything: only close the ledger.
                    self.ledger.release_hold(hold_id, self.now, reason)?;
                    self.log(Some(conn_id), "release", None, reason, true, "ledger closed; connection untouched");
                    return Ok(Outcome::Applied);
                }
                if !ours {
                    let m = "marker is not ours; refusing to change the connection";
                    self.log(Some(conn_id), "release", None, m, false, hold_id);
                    return Ok(Outcome::Aborted(m.into()));
                }
                let body = if *reenable {
                    json!({"isActive": true, "providerSpecificData": {marker::KEY: null}})
                } else {
                    json!({"providerSpecificData": {marker::KEY: null}})
                };
                let re = *reenable;
                let done = self.write_verified(conn_id, &body, move |c| marker::read(&c.provider_specific_data).is_none() && (!re || c.is_active))?;
                if done {
                    self.ledger.release_hold(hold_id, self.now, reason)?;
                }
                self.log(Some(conn_id), "release", None, reason, done, hold_id);
                Ok(if done { Outcome::Applied } else { Outcome::Failed("not confirmed".into()) })
            }
            Action::ManualOverride { hold_id, conn_id, until, reason } => {
                // Bookkeeping always runs (even in shadow it would be wrong to keep a hold the user already removed),
                // but only the marker cleanup touches the gateway, and only when live.
                self.ledger.release_hold(hold_id, self.now, "manual override")?;
                self.ledger.set_override(conn_id, *until, reason)?;
                if self.is_live(conn_id) {
                    let _ = self.gw.patch_connection(conn_id, &json!({"providerSpecificData": {marker::KEY: null}}));
                }
                self.log(Some(conn_id), "manual_override", None, reason, true, &until.to_rfc3339());
                Ok(Outcome::Applied)
            }
            Action::ClearMarker { conn_id } => {
                if !self.is_live(conn_id) {
                    self.log(Some(conn_id), "clear_marker", None, "orphan marker", false, "shadow");
                    return Ok(Outcome::Shadow);
                }
                let done = self.write_verified(conn_id, &json!({"providerSpecificData": {marker::KEY: null}}), |c| marker::read(&c.provider_specific_data).is_none())?;
                self.log(Some(conn_id), "clear_marker", None, "orphan marker", done, "");
                Ok(if done { Outcome::Applied } else { Outcome::Failed("not confirmed".into()) })
            }
        }
    }
}

fn action_conn(a: &Action) -> Option<&str> {
    match a {
        Action::Hold { conn_id, .. } | Action::Retarget { conn_id, .. } | Action::Release { conn_id, .. }
        | Action::ManualOverride { conn_id, .. } | Action::ResumeApply { conn_id, .. } | Action::ClearMarker { conn_id } => Some(conn_id),
        Action::Alert { conn_id, .. } => conn_id.as_deref(),
    }
}
fn action_name(a: &Action) -> &'static str {
    match a {
        Action::Hold { .. } => "hold",
        Action::Retarget { .. } => "retarget",
        Action::Release { .. } => "release",
        Action::ManualOverride { .. } => "manual_override",
        Action::ResumeApply { .. } => "resume",
        Action::ClearMarker { .. } => "clear_marker",
        Action::Alert { .. } => "alert",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::fake::FakeGateway;
    use chrono::{Duration, TimeZone};

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 7, 14, 0, 0).unwrap()
    }
    fn rig(live: bool) -> (FakeGateway, Ledger, BTreeMap<String, (String, bool)>) {
        let g = FakeGateway::default();
        g.add("c1", "claude", true);
        let l = Ledger::memory().unwrap();
        let mut m = BTreeMap::new();
        m.insert("c1".to_string(), ("claude".to_string(), live));
        (g, l, m)
    }
    fn hold_action() -> Action {
        Action::Hold { conn_id: "c1".into(), subscription: "claude".into(), window: WindowKind::Weekly, reset_at: Some(now() + Duration::hours(30)), max_until: now() + Duration::hours(40), predicted: false, reason: "t".into() }
    }
    fn act<'a>(g: &'a FakeGateway, l: &'a Ledger, m: &'a BTreeMap<String, (String, bool)>, paused: bool) -> Actuator<'a> {
        Actuator { gw: g, ledger: l, now: now(), live: m, paused, tick: 1 }
    }

    #[test]
    fn shadow_mode_writes_nothing() {
        let (g, l, m) = rig(false);
        assert_eq!(act(&g, &l, &m, false).apply(&hold_action()).unwrap(), Outcome::Shadow);
        assert!(g.patches.borrow().is_empty());
        assert!(l.open_holds().unwrap().is_empty());
    }
    #[test]
    fn pause_file_blocks_all_writes_even_when_live() {
        let (g, l, m) = rig(true);
        assert_eq!(act(&g, &l, &m, true).apply(&hold_action()).unwrap(), Outcome::Shadow);
        assert!(g.patches.borrow().is_empty());
    }
    #[test]
    fn hold_writes_disable_and_marker_in_one_patch_and_ledger_applied() {
        let (g, l, m) = rig(true);
        assert_eq!(act(&g, &l, &m, false).apply(&hold_action()).unwrap(), Outcome::Applied);
        let p = g.patches.borrow();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].1["isActive"], json!(false));
        assert!(p[0].1["providerSpecificData"]["qsHold"]["id"].is_string());
        // only our two keys are ever sent
        assert_eq!(p[0].1.as_object().unwrap().len(), 2);
        assert_eq!(p[0].1["providerSpecificData"].as_object().unwrap().len(), 1);
        let h = l.open_holds().unwrap();
        assert!(h.len() == 1 && h[0].applied && h[0].prior_active);
        assert_eq!(g.get("c1")["isActive"], json!(false));
    }
    #[test]
    fn hold_aborts_if_connection_changed_since_decision() {
        let (g, l, m) = rig(true);
        g.add("c1", "claude", false); // user disabled it meanwhile
        assert!(matches!(act(&g, &l, &m, false).apply(&hold_action()).unwrap(), Outcome::Aborted(_)));
        assert!(g.patches.borrow().is_empty() && l.open_holds().unwrap().is_empty());
    }
    #[test]
    fn failed_patch_leaves_unapplied_intent_for_resume() {
        let (g, l, m) = rig(true);
        *g.fail_patch.borrow_mut() = true;
        assert!(matches!(act(&g, &l, &m, false).apply(&hold_action()).unwrap(), Outcome::Failed(_)));
        let h = l.open_holds().unwrap();
        assert!(h.len() == 1 && !h[0].applied);
        assert_eq!(g.get("c1")["isActive"], json!(true));
        // next tick: resume re-applies
        let a = Action::ResumeApply { hold_id: h[0].id.clone(), conn_id: "c1".into() };
        assert_eq!(act(&g, &l, &m, false).apply(&a).unwrap(), Outcome::Applied);
        assert_eq!(g.get("c1")["isActive"], json!(false));
        assert!(l.open_holds().unwrap()[0].applied);
    }
    #[test]
    fn ambiguous_timeout_is_confirmed_by_readback_not_retried_blindly() {
        let (g, l, m) = rig(true);
        *g.apply_but_err.borrow_mut() = true; // applied but reported error
        assert_eq!(act(&g, &l, &m, false).apply(&hold_action()).unwrap(), Outcome::Applied);
        assert_eq!(g.patches.borrow().len(), 1);
        assert!(l.open_holds().unwrap()[0].applied);
    }
    #[test]
    fn crash_after_write_before_ledger_applied_is_recovered_without_second_write() {
        let (g, l, m) = rig(true);
        act(&g, &l, &m, false).apply(&hold_action()).unwrap();
        let h = l.open_holds().unwrap()[0].clone();
        l.db.execute("UPDATE holds SET applied=0 WHERE id=?", [&h.id]).unwrap(); // simulate crash before mark_applied
        let n = g.patches.borrow().len();
        let a = Action::ResumeApply { hold_id: h.id.clone(), conn_id: "c1".into() };
        assert_eq!(act(&g, &l, &m, false).apply(&a).unwrap(), Outcome::Applied);
        assert_eq!(g.patches.borrow().len(), n, "no extra write");
    }
    fn held() -> (FakeGateway, Ledger, BTreeMap<String, (String, bool)>, String) {
        let (g, l, m) = rig(true);
        act(&g, &l, &m, false).apply(&hold_action()).unwrap();
        let id = l.open_holds().unwrap()[0].id.clone();
        (g, l, m, id)
    }
    #[test]
    fn release_reenables_clears_marker_and_closes_ledger() {
        let (g, l, m, id) = held();
        let a = Action::Release { hold_id: id, conn_id: "c1".into(), reenable: true, reason: "cleared".into() };
        assert_eq!(act(&g, &l, &m, false).apply(&a).unwrap(), Outcome::Applied);
        assert_eq!(g.get("c1")["isActive"], json!(true));
        assert!(marker::read(&g.get("c1")["providerSpecificData"]).is_none());
        assert!(l.open_holds().unwrap().is_empty());
    }
    #[test]
    fn release_with_foreign_marker_touches_nothing() {
        let (g, l, m, id) = held();
        g.conns.borrow_mut().get_mut("c1").unwrap()["providerSpecificData"]["qsHold"]["id"] = json!("someone-else");
        let n = g.patches.borrow().len();
        let a = Action::Release { hold_id: id, conn_id: "c1".into(), reenable: true, reason: "x".into() };
        assert!(matches!(act(&g, &l, &m, false).apply(&a).unwrap(), Outcome::Aborted(_)));
        assert_eq!(g.patches.borrow().len(), n);
        assert_eq!(g.get("c1")["isActive"], json!(false));
        assert_eq!(l.open_holds().unwrap().len(), 1);
    }
    #[test]
    fn release_when_write_fails_keeps_hold_open() {
        let (g, l, m, id) = held();
        *g.fail_patch.borrow_mut() = true;
        let a = Action::Release { hold_id: id, conn_id: "c1".into(), reenable: true, reason: "x".into() };
        let r = act(&g, &l, &m, false).apply_all(&[a]);
        assert!(matches!(r[0].1, Outcome::Failed(_)));
        assert_eq!(l.open_holds().unwrap().len(), 1);
        assert_eq!(g.get("c1")["isActive"], json!(false));
    }
    #[test]
    fn release_crash_window_active_without_marker_only_closes_ledger() {
        let (g, l, m, id) = held();
        // simulate: re-enable PATCH landed, ledger write did not
        g.conns.borrow_mut().get_mut("c1").unwrap()["isActive"] = json!(true);
        g.conns.borrow_mut().get_mut("c1").unwrap()["providerSpecificData"]["qsHold"] = json!(null);
        let n = g.patches.borrow().len();
        let a = Action::Release { hold_id: id, conn_id: "c1".into(), reenable: false, reason: "closing".into() };
        assert_eq!(act(&g, &l, &m, false).apply(&a).unwrap(), Outcome::Applied);
        assert_eq!(g.patches.borrow().len(), n);
        assert!(l.open_holds().unwrap().is_empty());
    }
    #[test]
    fn manual_override_releases_ledger_and_suppresses() {
        let (g, l, m, id) = held();
        let a = Action::ManualOverride { hold_id: id, conn_id: "c1".into(), until: now() + Duration::hours(24), reason: "user".into() };
        assert_eq!(act(&g, &l, &m, false).apply(&a).unwrap(), Outcome::Applied);
        assert!(l.open_holds().unwrap().is_empty());
        assert!(l.override_until("c1").unwrap().unwrap() > now());
    }
    #[test]
    fn retarget_sends_marker_only_never_isactive() {
        let (g, l, m, id) = held();
        let a = Action::Retarget { hold_id: id.clone(), conn_id: "c1".into(), window: WindowKind::Monthly, reset_at: None, max_until: now() + Duration::days(40), reason: "esc".into() };
        assert_eq!(act(&g, &l, &m, false).apply(&a).unwrap(), Outcome::Applied);
        let last = g.patches.borrow().last().unwrap().1.clone();
        assert!(last.get("isActive").is_none());
        assert_eq!(l.open_holds().unwrap()[0].window, WindowKind::Monthly);
    }
    #[test]
    fn competing_writer_removes_marker_between_decision_and_release_is_not_overwritten() {
        let (g, l, m, id) = held();
        // someone else re-activates and wipes our marker; release must not re-disable or re-enable anything
        *g.drift.borrow_mut() = Some(Box::new(|c: &mut Value| {
            c["isActive"] = json!(true);
            c["providerSpecificData"]["qsHold"] = json!(null);
        }));
        let n = g.patches.borrow().len();
        let a = Action::Release { hold_id: id, conn_id: "c1".into(), reenable: true, reason: "x".into() };
        act(&g, &l, &m, false).apply(&a).unwrap();
        assert_eq!(g.patches.borrow().len(), n, "no write on top of a competing writer");
        assert_eq!(g.get("c1")["isActive"], json!(true));
    }
    #[test]
    fn user_disables_between_final_read_and_write_is_not_claimed_as_ours() {
        let (g, l, m) = rig(true);
        // first read (policy-time check) sees active; the competing write lands before the pre-PATCH re-read
        let calls = std::cell::Cell::new(0);
        let _ = &calls;
        *g.drift.borrow_mut() = None;
        let a = act(&g, &l, &m, false);
        // run the first read manually, then inject drift for the second one
        assert!(a.read("c1").unwrap().unwrap().is_active);
        *g.drift.borrow_mut() = Some(Box::new(|c: &mut Value| c["isActive"] = json!(false)));
        let r = a.apply(&hold_action()).unwrap();
        assert!(matches!(r, Outcome::Aborted(_)), "{r:?}");
        assert!(g.patches.borrow().is_empty(), "must not write on top of the user's change");
    }
    #[test]
    fn hold_never_sent_for_unmanaged_connection() {
        let (g, l, m) = rig(true);
        g.add("free1", "openrouter", true);
        let a = Action::Hold { conn_id: "free1".into(), subscription: "x".into(), window: WindowKind::Weekly, reset_at: None, max_until: now(), predicted: false, reason: "t".into() };
        assert_eq!(act(&g, &l, &m, false).apply(&a).unwrap(), Outcome::Shadow);
        assert!(g.patches.borrow().is_empty());
    }
}
