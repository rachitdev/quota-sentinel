//! Ownership marker stored in the connection's `providerSpecificData.qsHold`.
//! Spec v0.6: the marker is written BEFORE `isActive=false` and only a marker whose id matches an open ledger
//! hold, with `priorActive=true`, ever authorises an automatic re-enable.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const KEY: &str = "qsHold";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QsHold {
    pub id: String,
    pub window: String,
    pub reset_at: Option<DateTime<Utc>>,
    pub prior_active: bool,
}

pub fn read(psd: &Value) -> Option<QsHold> {
    serde_json::from_value(psd.get(KEY)?.clone()).ok()
}

pub fn to_value(h: &QsHold) -> Value {
    serde_json::to_value(h).expect("marker serialises")
}
