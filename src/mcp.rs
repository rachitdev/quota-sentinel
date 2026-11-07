//! Minimal stdio MCP server (newline-delimited JSON-RPC 2.0). Read-mostly: the only writes are `qs_pause`,
//! `qs_resume` and `qs_override`, which touch Sentinel's own state, never the gateway.
use crate::config::Config;
use crate::ledger::Ledger;
use crate::report;
use crate::tick;
use anyhow::Result;
use chrono::{Duration, Utc};
use serde_json::{json, Value};
use std::io::{BufRead, Write};

const PROTOCOL: &str = "2024-11-05";

fn tools() -> Value {
    let obj = |props: Value, req: &[&str]| json!({"type":"object","properties":props,"required":req});
    json!([
        {"name":"qs_status","description":"Quota Sentinel mode (live/shadow/paused), open holds, last tick and counters.","inputSchema":obj(json!({}),&[])},
        {"name":"qs_holds","description":"List open holds placed by Quota Sentinel (subscription, window, reset time, applied/pending).","inputSchema":obj(json!({}),&[])},
        {"name":"qs_stats","description":"Utilisation, actions and self-telemetry for the last 24h, as the daily report text.","inputSchema":obj(json!({}),&[])},
        {"name":"qs_decisions","description":"Recent decisions (holds, releases, alerts) from the ledger.","inputSchema":obj(json!({"limit":{"type":"integer","description":"max rows (default 20)"}}),&[])},
        {"name":"qs_pause","description":"Freeze every gateway write by Quota Sentinel. Existing holds stay; nothing is bulk re-enabled.","inputSchema":obj(json!({}),&[])},
        {"name":"qs_resume","description":"Remove the PAUSE file.","inputSchema":obj(json!({}),&[])},
        {"name":"qs_override","description":"Do not place new holds on a connection for N hours (use after re-enabling a connection by hand).","inputSchema":obj(json!({"connection":{"type":"string"},"hours":{"type":"integer"}}),&["connection"])}
    ])
}

fn text(s: String) -> Value {
    json!({"content":[{"type":"text","text":s}],"isError":false})
}
fn err(s: String) -> Value {
    json!({"content":[{"type":"text","text":s}],"isError":true})
}

pub fn call(cfg: &Config, name: &str, args: &Value) -> Value {
    let run = || -> Result<Value> {
        let l = Ledger::open(&cfg.state_path().join("qs.db"))?;
        let mode = if tick::pause_file(cfg).exists() { "paused" } else if cfg.enabled { "live" } else { "shadow" };
        Ok(match name {
            "qs_status" => {
                let holds = l.open_holds()?;
                let last: Option<(String, String, Option<i64>, Option<String>)> = l
                    .db
                    .query_row("SELECT started_at, mode, ok, note FROM ticks ORDER BY id DESC LIMIT 1", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                    .ok();
                text(serde_json::to_string_pretty(&json!({
                    "mode": mode, "enabled": cfg.enabled, "liveSubscriptions": cfg.live_subscriptions, "openHolds": holds.len(),
                    "lastTick": last.map(|t| json!({"startedAt":t.0,"mode":t.1,"ok":t.2,"note":t.3}))
                }))?)
            }
            "qs_holds" => text(serde_json::to_string_pretty(&l.open_holds()?.iter().map(|h| json!({
                "id":h.id,"subscription":h.subscription,"window":h.window.as_str(),"applied":h.applied,
                "resetAt":h.reset_at,"maxUntil":h.max_until,"reason":h.reason})).collect::<Vec<_>>())?),
            "qs_stats" => text(report::build(&l, Utc::now(), mode)?.text),
            "qs_decisions" => {
                let n = args.get("limit").and_then(|x| x.as_i64()).unwrap_or(20).clamp(1, 200);
                let mut st = l.db.prepare("SELECT at, action, subscription, window, applied, reason FROM decisions ORDER BY id DESC LIMIT ?")?;
                let rows: Vec<Value> = st
                    .query_map([n], |r| Ok(json!({"at":r.get::<_,String>(0)?,"action":r.get::<_,String>(1)?,"subscription":r.get::<_,Option<String>>(2)?,"window":r.get::<_,Option<String>>(3)?,"applied":r.get::<_,i64>(4)?==1,"reason":r.get::<_,Option<String>>(5)?})))?
                    .collect::<Result<_, _>>()?;
                text(serde_json::to_string_pretty(&rows)?)
            }
            "qs_pause" => {
                std::fs::create_dir_all(cfg.state_path())?;
                std::fs::write(tick::pause_file(cfg), Utc::now().to_rfc3339())?;
                text("paused".into())
            }
            "qs_resume" => {
                let _ = std::fs::remove_file(tick::pause_file(cfg));
                text("resumed".into())
            }
            "qs_override" => {
                let c = args.get("connection").and_then(|x| x.as_str()).unwrap_or_default();
                if c.is_empty() {
                    return Ok(err("connection is required".into()));
                }
                let h = args.get("hours").and_then(|x| x.as_i64()).unwrap_or(24).clamp(1, 24 * 14);
                l.set_override(c, Utc::now() + Duration::hours(h), "mcp override")?;
                text(format!("{c}: no new holds for {h}h"))
            }
            other => err(format!("unknown tool {other}")),
        })
    };
    run().unwrap_or_else(|e| err(format!("{e:#}")))
}

pub fn handle(cfg: &Config, req: &Value) -> Option<Value> {
    let id = req.get("id")?.clone();
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let result = match method {
        "initialize" => json!({"protocolVersion": PROTOCOL, "capabilities": {"tools": {}}, "serverInfo": {"name": "quota-sentinel", "version": env!("CARGO_PKG_VERSION")}}),
        "tools/list" => json!({"tools": tools()}),
        "tools/call" => {
            let p = req.get("params").cloned().unwrap_or(Value::Null);
            call(cfg, p.get("name").and_then(|n| n.as_str()).unwrap_or(""), p.get("arguments").unwrap_or(&Value::Null))
        }
        "ping" => json!({}),
        _ => return Some(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":format!("method not found: {method}")}})),
    };
    Some(json!({"jsonrpc":"2.0","id":id,"result":result}))
}

pub fn serve(cfg: &Config) -> Result<()> {
    let stdin = std::io::stdin();
    let mut out = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<Value>(&line) {
            Ok(v) => handle(cfg, &v),
            Err(e) => Some(json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":format!("parse error: {e}")}})),
        };
        if let Some(r) = resp {
            writeln!(out, "{r}")?;
            out.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cfg() -> Config {
        let mut c = Config::default();
        c.state_dir = tempfile::tempdir().unwrap().keep().to_string_lossy().into();
        c
    }
    #[test]
    fn initialize_list_and_unknown_method() {
        let c = cfg();
        let r = handle(&c, &json!({"jsonrpc":"2.0","id":1,"method":"initialize"})).unwrap();
        assert_eq!(r["result"]["serverInfo"]["name"], "quota-sentinel");
        let r = handle(&c, &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})).unwrap();
        assert_eq!(r["result"]["tools"].as_array().unwrap().len(), 7);
        assert!(handle(&c, &json!({"jsonrpc":"2.0","method":"notifications/initialized"})).is_none());
        let r = handle(&c, &json!({"jsonrpc":"2.0","id":3,"method":"nope"})).unwrap();
        assert_eq!(r["error"]["code"], -32601);
    }
    #[test]
    fn pause_resume_status_roundtrip() {
        let c = cfg();
        let st = |c: &Config| call(c, "qs_status", &Value::Null)["content"][0]["text"].as_str().unwrap().to_string();
        assert!(st(&c).contains("\"shadow\""));
        call(&c, "qs_pause", &Value::Null);
        assert!(st(&c).contains("\"paused\""));
        call(&c, "qs_resume", &Value::Null);
        assert!(st(&c).contains("\"shadow\""));
    }
    #[test]
    fn override_validates_input_and_persists() {
        let c = cfg();
        assert_eq!(call(&c, "qs_override", &json!({}))["isError"], true);
        assert_eq!(call(&c, "qs_override", &json!({"connection":"c1","hours":2}))["isError"], false);
        let l = Ledger::open(&c.state_path().join("qs.db")).unwrap();
        assert!(l.override_until("c1").unwrap().is_some());
    }
    #[test]
    fn stats_and_decisions_work_on_empty_ledger() {
        let c = cfg();
        assert!(call(&c, "qs_stats", &Value::Null)["content"][0]["text"].as_str().unwrap().contains("QUOTA SENTINEL"));
        assert_eq!(call(&c, "qs_decisions", &json!({"limit": 5}))["isError"], false);
        assert_eq!(call(&c, "nope", &Value::Null)["isError"], true);
    }
}
