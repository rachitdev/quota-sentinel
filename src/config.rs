//! Configuration with safe defaults. Everything is OFF until the user opts in (spec v0.6 staged rollout).
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Thresholds {
    pub block_pct: f64,
    pub restore_pct: f64,
    pub grace_minutes: i64,
    pub fresh_minutes: i64,
    pub stale_minutes: i64,
    pub dwell_minutes: i64,
    pub max_hold_extra_hours: i64,
    pub tick_minutes: i64,
    pub latency_minutes: i64,
    pub guard_margin_pct: f64,
    pub suspect_extra_margin_pct: f64,
    pub dispute_pct: f64,
    pub override_hours: i64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Thresholds {
            block_pct: 90.0,
            restore_pct: 70.0,
            grace_minutes: 5,
            fresh_minutes: 30,
            stale_minutes: 45,
            dwell_minutes: 30,
            max_hold_extra_hours: 2,
            tick_minutes: 15,
            latency_minutes: 5,
            guard_margin_pct: 2.0,
            suspect_extra_margin_pct: 5.0,
            dispute_pct: 10.0,
            override_hours: 24,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Master switch. false = shadow mode: observe, decide, log, never write to the gateway.
    pub enabled: bool,
    /// Subscriptions allowed to be enforced live (others stay shadow). Rollout: chatgpt, grok, opencode-go, claude, gemini.
    pub live_subscriptions: Vec<String>,
    /// Connection ids allowed to be written. A connection is live only if its subscription is in
    /// `live_subscriptions` AND its id is listed here (subscription mapping alone never authorises a write).
    pub live_connections: Vec<String>,
    pub gateway_url: String,
    pub key_file: String,
    pub state_dir: String,
    pub smtp_file: String,
    pub thresholds: Thresholds,
    /// gateway provider id -> subscription name. Providers not listed are never managed (free/local rungs).
    pub subscriptions: BTreeMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        let subs: BTreeMap<String, String> = [
            ("claude", "claude"),
            ("codex", "chatgpt"),
            ("codex-app-server", "chatgpt"),
            ("grok-cli", "grok"),
            ("xai-oauth", "grok"),
            ("agy", "gemini"),
            ("opencode-go", "opencode-go"),
        ]
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
        Config {
            enabled: false,
            live_subscriptions: vec![],
            live_connections: vec![],
            gateway_url: "http://localhost:20128".into(),
            key_file: "~/.dsh/secrets/omniroute-mcp.env".into(),
            state_dir: "~/.dsh/state/quota-sentinel".into(),
            smtp_file: "~/.dsh/secrets/quota-sentinel-smtp.env".into(),
            thresholds: Thresholds::default(),
            subscriptions: subs,
        }
    }
}

pub fn expand(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(h) = std::env::var_os("HOME") {
            return PathBuf::from(h).join(rest);
        }
    }
    PathBuf::from(p)
}

impl Config {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Config> {
        let default_path = expand("~/.dsh/etc/quota-sentinel.toml");
        let p = path.map(Path::to_path_buf).unwrap_or(default_path);
        if !p.exists() {
            return Ok(Config::default());
        }
        Ok(toml::from_str(&std::fs::read_to_string(&p)?)?)
    }
    pub fn state_path(&self) -> PathBuf {
        expand(&self.state_dir)
    }
    pub fn subscription_of(&self, provider: &str) -> Option<&str> {
        self.subscriptions.get(provider).map(String::as_str)
    }
    pub fn is_live(&self, subscription: &str, connection_id: &str) -> bool {
        self.enabled
            && self.live_subscriptions.iter().any(|s| s == subscription)
            && self.live_connections.iter().any(|c| c == connection_id)
    }
}
