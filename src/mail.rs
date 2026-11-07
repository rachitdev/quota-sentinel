//! Daily-report delivery through the Resend HTTP API (same mechanism as ~/.dsh/bin/decision-forge-trial-report).
//! The API key is read from a 0600 env file (default ~/.dsh/.env); it is never logged or stored in the ledger.
use anyhow::{anyhow, bail, Context, Result};
use serde_json::json;
use std::path::Path;
use std::time::Duration;

pub const RESEND_BASE: &str = "https://api.resend.com";

#[derive(Debug, Clone)]
pub struct Resend {
    pub key: String,
    pub from: String,
    pub to: String,
    pub base: String,
}

/// Read RESEND_API_KEY from a KEY=VALUE file. Refuses a file readable by group/others (it holds secrets).
pub fn load(env_file: &Path, from: &str, to: &str) -> Result<Resend> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(env_file).with_context(|| format!("missing env file {}", env_file.display()))?;
    if meta.permissions().mode() & 0o077 != 0 {
        bail!("{} holds secrets and must be chmod 600", env_file.display());
    }
    let text = std::fs::read_to_string(env_file)?;
    let key = text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("RESEND_API_KEY="))
        .map(|v| v.trim().trim_matches(['\'', '"']).to_string())
        .find(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("RESEND_API_KEY not found in {}", env_file.display()))?;
    Ok(Resend { key, from: from.into(), to: to.into(), base: RESEND_BASE.into() })
}

fn redact(s: &str, key: &str) -> String {
    if key.is_empty() { s.to_string() } else { s.replace(key, "[redacted]") }
}

/// Send one email. Success requires an `id` in Resend's reply (a 2xx without an id is treated as failure).
pub fn send(r: &Resend, subject: &str, text: &str, html: &str) -> Result<String> {
    if !r.base.starts_with("https://") && !r.base.starts_with("http://127.0.0.1") {
        bail!("refusing to send the Resend key to a non-https endpoint");
    }
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(30)))
        .build()
        .into();
    let mut resp = agent
        .post(format!("{}/emails", r.base))
        .header("Authorization", format!("Bearer {}", r.key))
        .send_json(json!({"from": r.from, "to": [r.to], "subject": subject, "text": text, "html": html}))
        .map_err(|e| anyhow!("Resend request failed: {}", redact(&e.to_string(), &r.key)))?;
    let code = resp.status().as_u16();
    let body: serde_json::Value = resp.body_mut().read_json().unwrap_or(serde_json::Value::Null);
    match body.get("id").and_then(|x| x.as_str()) {
        Some(id) if (200..300).contains(&code) => Ok(id.to_string()),
        _ => bail!("Resend rejected the message: HTTP {code} {}", redact(&body.to_string(), &r.key).chars().take(200).collect::<String>()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt;

    fn server(status: &str, body: &'static str) -> (String, std::thread::JoinHandle<String>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://127.0.0.1:{}", l.local_addr().unwrap().port());
        let status = status.to_string();
        let h = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            loop {
                let n = s.read(&mut tmp).unwrap();
                buf.extend_from_slice(&tmp[..n]);
                let t = String::from_utf8_lossy(&buf).to_string();
                if let Some(i) = t.find("\r\n\r\n") {
                    let cl = t[..i].lines().find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").and_then(|v| v.trim().parse::<usize>().ok())).unwrap_or(0);
                    if buf.len() >= i + 4 + cl { break }
                }
            }
            write!(s, "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).unwrap();
            String::from_utf8_lossy(&buf).to_string()
        });
        (base, h)
    }
    fn rs(base: &str) -> Resend {
        Resend { key: "re_secretkey123".into(), from: "DSH <onboarding@resend.dev>".into(), to: "me@example.com".into(), base: base.into() }
    }

    #[test]
    fn sends_expected_request_and_returns_id() {
        let (base, h) = server("200 OK", r#"{"id":"abc-123"}"#);
        assert_eq!(send(&rs(&base), "Subj", "plain", "<b>h</b>").unwrap(), "abc-123");
        let req = h.join().unwrap();
        assert!(req.starts_with("POST /emails"), "{req}");
        assert!(req.to_ascii_lowercase().contains("authorization: bearer re_secretkey123"));
        let body: serde_json::Value = serde_json::from_str(req.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(body["to"], serde_json::json!(["me@example.com"]));
        assert_eq!((body["subject"].as_str(), body["html"].as_str(), body["text"].as_str()), (Some("Subj"), Some("<b>h</b>"), Some("plain")));
        assert_eq!(body["from"], "DSH <onboarding@resend.dev>");
    }
    #[test]
    fn http_error_is_failure_and_never_leaks_key() {
        let (base, h) = server("403 Forbidden", r#"{"message":"bad key re_secretkey123"}"#);
        let e = send(&rs(&base), "s", "t", "h").unwrap_err().to_string();
        h.join().unwrap();
        assert!(e.contains("403") && !e.contains("re_secretkey123"), "{e}");
    }
    #[test]
    fn two_hundred_without_id_is_failure() {
        let (base, h) = server("200 OK", r#"{"ok":true}"#);
        assert!(send(&rs(&base), "s", "t", "h").is_err());
        h.join().unwrap();
    }
    #[test]
    fn refuses_plain_http_to_non_loopback() {
        let mut r = rs("http://example.com");
        assert!(send(&r, "s", "t", "h").unwrap_err().to_string().contains("non-https"));
        r.base = "https://api.resend.com".into();
        assert!(r.base.starts_with("https://"));
    }
    #[test]
    fn env_file_must_be_private_and_contain_key() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join(".env");
        std::fs::write(&p, "OTHER=1\nRESEND_API_KEY=\"re_abc\"\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load(&p, "f", "t").unwrap_err().to_string().contains("chmod 600"));
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(load(&p, "f", "t").unwrap().key, "re_abc");
        std::fs::write(&p, "OTHER=1\n").unwrap();
        assert!(load(&p, "f", "t").unwrap_err().to_string().contains("not found"));
    }
}
