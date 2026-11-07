mod actuator;
mod config;
mod gateway;
mod ledger;
mod mail;
mod mcp;
mod marker;
mod model;
mod normalize;
mod policy;
mod report;
mod tick;

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "qs", version, about = "Quota Sentinel: keep paid LLM subscriptions below their quota limits")]
struct Cli {
    /// Config file (default ~/.dsh/etc/quota-sentinel.toml)
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run one tick: read quotas, decide, act (shadow unless enabled), log. Exits.
    Tick {
        #[arg(long)]
        json: bool,
    },
    /// Current quota per managed connection and open holds (read-only).
    Status,
    /// List open holds.
    Holds,
    /// Stop Sentinel from holding a connection for N hours (also use after re-enabling by hand).
    Override { connection: String, #[arg(long, default_value_t = 24)] hours: i64 },
    /// Release ONE hold by id (explicit operator action; ownership-checked; never bulk).
    Release { hold_id: String },
    /// Freeze all gateway writes (never bulk re-enables).
    Pause,
    /// Remove the PAUSE file.
    Resume,
    /// Build the daily report; print it (--dry) or send it by email.
    Report {
        #[arg(long)]
        dry: bool,
    },
    /// Serve MCP tools over stdio (status, holds, stats, decisions, pause/resume, override).
    Mcp,
    /// Print the effective configuration.
    Config,
}

fn open(cfg: &config::Config) -> Result<(gateway::HttpGateway, ledger::Ledger)> {
    let key = gateway::read_key(&config::expand(&cfg.key_file))?;
    let gw = gateway::HttpGateway::new(&cfg.gateway_url, key)?;
    let l = ledger::Ledger::open(&cfg.state_path().join("qs.db"))?;
    Ok((gw, l))
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::Config::load(cli.config.as_deref())?;
    match cli.cmd {
        Cmd::Mcp => mcp::serve(&cfg)?,
        Cmd::Config => println!("{}", toml::to_string_pretty(&cfg)?),
        Cmd::Report { dry } => {
            let l = ledger::Ledger::open(&cfg.state_path().join("qs.db"))?;
            let mode = if tick::pause_file(&cfg).exists() { "paused" } else if cfg.enabled { "live" } else { "shadow" };
            let r = report::build(&l, Utc::now(), mode)?;
            if dry {
                println!("Subject: {}\n\n{}", r.subject, r.text);
            } else {
                let smtp = mail::load(&config::expand(&cfg.smtp_file))?;
                mail::send(&smtp, &r.subject, &r.text, &r.html)?;
                println!("sent: {}", r.subject);
            }
        }
        Cmd::Release { hold_id } => {
            let (gw, l) = open(&cfg)?;
            let Some(h) = l.open_holds()?.into_iter().find(|h| h.id == hold_id) else {
                anyhow::bail!("no open hold {hold_id}");
            };
            let mut live = std::collections::BTreeMap::new();
            live.insert(h.connection_id.clone(), (h.subscription.clone(), true));
            let act = policy::Action::Release { hold_id: h.id.clone(), conn_id: h.connection_id.clone(), reenable: h.prior_active, reason: "operator release".into() };
            let tick = l.start_tick(Utc::now(), "release")?;
            // An explicit operator command: not gated by `enabled`/live list, but PAUSE still freezes it.
            let a = actuator::Actuator { gw: &gw, ledger: &l, now: Utc::now(), live: &live, paused: false, pause_file: Some(tick::pause_file(&cfg)), tick };
            let r = a.apply_all(&[act]);
            l.finish_tick(tick, Utc::now(), true, "operator release")?;
            println!("{:?}", r[0].1);
        }
        Cmd::Pause => {
            std::fs::create_dir_all(cfg.state_path())?;
            std::fs::write(tick::pause_file(&cfg), Utc::now().to_rfc3339())?;
            println!("paused: all gateway writes frozen (existing holds stay)");
        }
        Cmd::Resume => {
            let _ = std::fs::remove_file(tick::pause_file(&cfg));
            println!("resumed");
        }
        Cmd::Override { connection, hours } => {
            let (_, l) = open(&cfg)?;
            l.set_override(&connection, Utc::now() + Duration::hours(hours), "qs override")?;
            println!("{connection}: no new holds for {hours}h");
        }
        Cmd::Holds => {
            let (_, l) = open(&cfg)?;
            for h in l.open_holds()? {
                println!("{}  {}  {}  {}  reset={:?}  until={}", h.id, h.subscription, h.window.as_str(), if h.applied { "applied" } else { "PENDING" }, h.reset_at.map(|d| d.to_rfc3339()), h.max_until.to_rfc3339());
            }
        }
        Cmd::Status => {
            let (gw, l) = open(&cfg)?;
            use gateway::Gateway;
            for v in gw.connections()? {
                let Some(c) = model::Conn::from_json(&v) else { continue };
                let Some(sub) = cfg.subscription_of(&c.provider) else { continue };
                let n = match gw.usage(&c.id) {
                    Ok(u) => normalize::normalize(&c.provider, &c.id, sub, &u, Utc::now()),
                    Err(e) => { println!("{:<12} {:<11} probe error: {e:#}", c.provider, sub); continue }
                };
                let mut parts: Vec<String> = n.obs.iter().map(|o| format!("{}{}={:.0}%", o.kind.as_str(), o.group.as_deref().map(|g| format!("[{g}]")).unwrap_or_default(), o.used_pct)).collect();
                if let Some(u) = n.unavailable { parts.push(format!("unavailable: {u}")); }
                println!("{:<12} {:<11} {} {}", c.provider, sub, if c.is_active { "active  " } else { "DISABLED" }, parts.join(" "));
            }
            println!("open holds: {}", l.open_holds()?.len());
        }
        Cmd::Tick { json } => {
            let Some(_lock) = tick::try_lock(&cfg)? else {
                println!("another tick is running; exiting");
                return Ok(());
            };
            let (gw, l) = open(&cfg).context("opening gateway/ledger")?;
            let r = tick::run_tick(&cfg, &gw, &l, Utc::now())?;
            if json {
                println!("{}", serde_json::json!({"tick": r.tick_id, "mode": r.mode, "managed": r.managed, "probeErrors": r.probe_errors.len(), "actions": r.results.iter().filter(|(a, _)| !matches!(a, policy::Action::Alert { .. })).count()}));
            } else {
                println!("tick {} [{}] managed={} probe_errors={}", r.tick_id, r.mode, r.managed, r.probe_errors.len());
                for (a, o) in &r.results {
                    println!("  {:?} -> {:?}", a, o);
                }
            }
        }
    }
    Ok(())
}
