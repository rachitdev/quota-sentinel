//! Append-only SQLite ledger: observations, ticks, decisions, and holds Sentinel placed.
use crate::model::{WindowKind, WindowObs};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

pub struct Ledger {
    pub db: Connection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HoldRow {
    pub id: String,
    pub connection_id: String,
    pub subscription: String,
    pub window: WindowKind,
    pub reason: String,
    pub prior_active: bool,
    pub reset_at: Option<DateTime<Utc>>,
    pub max_until: DateTime<Utc>,
    pub placed_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
    pub release_reason: Option<String>,
    /// false = intent logged but marker/disable not yet verified (crash window); see policy `ResumeApply`.
    pub applied: bool,
}

fn ts(d: DateTime<Utc>) -> String {
    d.to_rfc3339()
}
fn pts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).map(|d| d.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now())
}

impl Ledger {
    pub fn open(path: &Path) -> anyhow::Result<Ledger> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(path)?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        let l = Ledger { db };
        l.migrate()?;
        Ok(l)
    }

    pub fn memory() -> anyhow::Result<Ledger> {
        let l = Ledger { db: Connection::open_in_memory()? };
        l.migrate()?;
        Ok(l)
    }

    fn migrate(&self) -> anyhow::Result<()> {
        self.db.execute_batch(
            "CREATE TABLE IF NOT EXISTS obs(
               id INTEGER PRIMARY KEY, at TEXT NOT NULL, observed_at TEXT NOT NULL, provider TEXT NOT NULL,
               connection_id TEXT NOT NULL, subscription TEXT NOT NULL, win TEXT NOT NULL, grp TEXT,
               used_pct REAL NOT NULL, reset_at TEXT);
             CREATE INDEX IF NOT EXISTS obs_conn ON obs(connection_id, win, at);
             CREATE TABLE IF NOT EXISTS ticks(
               id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT, mode TEXT NOT NULL,
               ok INTEGER, note TEXT);
             CREATE TABLE IF NOT EXISTS decisions(
               id INTEGER PRIMARY KEY, at TEXT NOT NULL, tick_id INTEGER, connection_id TEXT, subscription TEXT,
               action TEXT NOT NULL, window TEXT, reason TEXT, applied INTEGER NOT NULL, detail TEXT);
             CREATE TABLE IF NOT EXISTS holds(
               id TEXT PRIMARY KEY, connection_id TEXT NOT NULL, subscription TEXT NOT NULL, win TEXT NOT NULL,
               reason TEXT NOT NULL, prior_active INTEGER NOT NULL, reset_at TEXT, max_until TEXT NOT NULL,
               placed_at TEXT NOT NULL, released_at TEXT, release_reason TEXT, applied INTEGER NOT NULL DEFAULT 0);
             CREATE INDEX IF NOT EXISTS holds_open ON holds(connection_id) WHERE released_at IS NULL;
             CREATE TABLE IF NOT EXISTS overrides(
               connection_id TEXT PRIMARY KEY, until TEXT NOT NULL, reason TEXT);",
        )?;
        Ok(())
    }

    pub fn record_obs(&self, now: DateTime<Utc>, o: &WindowObs) -> anyhow::Result<()> {
        self.db.execute(
            "INSERT INTO obs(at,observed_at,provider,connection_id,subscription,win,grp,used_pct,reset_at)
             VALUES(?,?,?,?,?,?,?,?,?)",
            params![ts(now), ts(o.observed_at), o.provider, o.connection_id, o.subscription, o.kind.as_str(),
                    o.group, o.used_pct, o.reset_at.map(ts)],
        )?;
        Ok(())
    }

    pub fn start_tick(&self, now: DateTime<Utc>, mode: &str) -> anyhow::Result<i64> {
        self.db.execute("INSERT INTO ticks(started_at,mode) VALUES(?,?)", params![ts(now), mode])?;
        Ok(self.db.last_insert_rowid())
    }
    pub fn finish_tick(&self, id: i64, now: DateTime<Utc>, ok: bool, note: &str) -> anyhow::Result<()> {
        self.db.execute("UPDATE ticks SET finished_at=?, ok=?, note=? WHERE id=?", params![ts(now), ok as i64, note, id])?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_decision(
        &self, now: DateTime<Utc>, tick: i64, conn: Option<&str>, sub: Option<&str>, action: &str,
        window: Option<WindowKind>, reason: &str, applied: bool, detail: &str,
    ) -> anyhow::Result<()> {
        self.db.execute(
            "INSERT INTO decisions(at,tick_id,connection_id,subscription,action,window,reason,applied,detail)
             VALUES(?,?,?,?,?,?,?,?,?)",
            params![ts(now), tick, conn, sub, action, window.map(|w| w.as_str()), reason, applied as i64, detail],
        )?;
        Ok(())
    }

    pub fn insert_hold(&self, h: &HoldRow) -> anyhow::Result<()> {
        self.db.execute(
            "INSERT INTO holds(id,connection_id,subscription,win,reason,prior_active,reset_at,max_until,placed_at,applied)
             VALUES(?,?,?,?,?,?,?,?,?,?)",
            params![h.id, h.connection_id, h.subscription, h.window.as_str(), h.reason, h.prior_active as i64,
                    h.reset_at.map(ts), ts(h.max_until), ts(h.placed_at), h.applied as i64],
        )?;
        Ok(())
    }

    pub fn update_hold_reset(&self, id: &str, reset_at: Option<DateTime<Utc>>, max_until: DateTime<Utc>) -> anyhow::Result<()> {
        self.db.execute("UPDATE holds SET reset_at=?, max_until=? WHERE id=?", params![reset_at.map(ts), ts(max_until), id])?;
        Ok(())
    }

    pub fn update_hold_window(&self, id: &str, w: WindowKind, reset_at: Option<DateTime<Utc>>, max_until: DateTime<Utc>) -> anyhow::Result<()> {
        self.db.execute("UPDATE holds SET win=?, reset_at=?, max_until=? WHERE id=?", params![w.as_str(), reset_at.map(ts), ts(max_until), id])?;
        Ok(())
    }

    pub fn mark_applied(&self, id: &str) -> anyhow::Result<()> {
        self.db.execute("UPDATE holds SET applied=1 WHERE id=?", params![id])?;
        Ok(())
    }

    pub fn release_hold(&self, id: &str, now: DateTime<Utc>, reason: &str) -> anyhow::Result<()> {
        self.db.execute(
            "UPDATE holds SET released_at=?, release_reason=? WHERE id=? AND released_at IS NULL",
            params![ts(now), reason, id],
        )?;
        Ok(())
    }

    pub fn open_holds(&self) -> anyhow::Result<Vec<HoldRow>> {
        let mut st = self.db.prepare(
            "SELECT id,connection_id,subscription,win,reason,prior_active,reset_at,max_until,placed_at,released_at,release_reason,applied
             FROM holds WHERE released_at IS NULL ORDER BY placed_at",
        )?;
        let rows = st.query_map([], |r| {
            Ok(HoldRow {
                id: r.get(0)?,
                connection_id: r.get(1)?,
                subscription: r.get(2)?,
                window: WindowKind::from_db(&r.get::<_, String>(3)?).unwrap_or(WindowKind::Weekly),
                reason: r.get(4)?,
                prior_active: r.get::<_, i64>(5)? != 0,
                reset_at: r.get::<_, Option<String>>(6)?.map(|s| pts(&s)),
                max_until: pts(&r.get::<_, String>(7)?),
                placed_at: pts(&r.get::<_, String>(8)?),
                released_at: None,
                release_reason: None,
                applied: r.get::<_, i64>(11)? != 0,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn set_override(&self, conn: &str, until: DateTime<Utc>, reason: &str) -> anyhow::Result<()> {
        self.db.execute(
            "INSERT INTO overrides(connection_id,until,reason) VALUES(?,?,?)
             ON CONFLICT(connection_id) DO UPDATE SET until=excluded.until, reason=excluded.reason",
            params![conn, ts(until), reason],
        )?;
        Ok(())
    }
    pub fn override_until(&self, conn: &str) -> anyhow::Result<Option<DateTime<Utc>>> {
        Ok(self
            .db
            .query_row("SELECT until FROM overrides WHERE connection_id=?", params![conn], |r| r.get::<_, String>(0))
            .optional()?
            .map(|s| pts(&s)))
    }

    /// Highest used_pct seen in the last `since` for a connection/window (for the burn-rate guard band).
    pub fn recent_series(&self, conn: &str, w: WindowKind, since: DateTime<Utc>) -> anyhow::Result<Vec<(DateTime<Utc>, f64)>> {
        let mut st = self.db.prepare(
            "SELECT observed_at, MAX(used_pct) FROM obs WHERE connection_id=? AND win=? AND observed_at>=?
             GROUP BY observed_at ORDER BY observed_at",
        )?;
        let rows = st.query_map(params![conn, w.as_str(), ts(since)], |r| Ok((pts(&r.get::<_, String>(0)?), r.get::<_, f64>(1)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
}
