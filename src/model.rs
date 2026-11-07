//! Core data types shared by every module.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Quota window class. Names differ per vendor; see [`WindowKind::classify`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WindowKind {
    FiveHour,
    Weekly,
    Monthly,
}

impl WindowKind {
    pub fn as_str(self) -> &'static str {
        match self {
            WindowKind::FiveHour => "5h",
            WindowKind::Weekly => "weekly",
            WindowKind::Monthly => "monthly",
        }
    }

    pub fn from_db(s: &str) -> Option<Self> {
        match s {
            "5h" => Some(WindowKind::FiveHour),
            "weekly" => Some(WindowKind::Weekly),
            "monthly" => Some(WindowKind::Monthly),
            _ => None,
        }
    }

    /// Weekly and monthly windows are "remove completely until reset"; the 5h window is a routing exclusion.
    pub fn is_long(self) -> bool {
        matches!(self, WindowKind::Weekly | WindowKind::Monthly)
    }

    /// Map a gateway window name ("session (5h)", "weekly (7d)", "mcp_monthly", "session") to a kind.
    /// Unknown names (for example "product_grok_build") return `None` and are ignored, never guessed.
    pub fn classify(name: &str) -> Option<Self> {
        let n = name.to_ascii_lowercase();
        if n.contains("month") {
            Some(WindowKind::Monthly)
        } else if n.contains("week") || n.contains("7d") {
            Some(WindowKind::Weekly)
        } else if n.contains("session") || n.contains("5h") || n.contains("5-hour") || n.contains("hour") {
            Some(WindowKind::FiveHour)
        } else {
            None
        }
    }
}

/// One normalised quota reading for one window of one connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowObs {
    pub provider: String,
    pub connection_id: String,
    pub subscription: String,
    pub kind: WindowKind,
    pub group: Option<String>,
    /// 0..=100
    pub used_pct: f64,
    pub reset_at: Option<DateTime<Utc>>,
    /// When the gateway observed the value (not when we fetched it) when it says so.
    pub observed_at: DateTime<Utc>,
}

/// Minimal view of a gateway provider connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Conn {
    pub id: String,
    pub provider: String,
    pub name: String,
    pub is_active: bool,
    pub provider_specific_data: serde_json::Value,
}

impl Conn {
    pub fn from_json(v: &serde_json::Value) -> Option<Conn> {
        Some(Conn {
            id: v.get("id")?.as_str()?.to_string(),
            provider: v.get("provider")?.as_str()?.to_string(),
            name: v.get("name").and_then(|x| x.as_str()).unwrap_or("-").to_string(),
            is_active: v.get("isActive").and_then(|x| x.as_bool()).unwrap_or(false),
            provider_specific_data: v
                .get("providerSpecificData")
                .cloned()
                .unwrap_or(serde_json::Value::Object(Default::default())),
        })
    }
}
