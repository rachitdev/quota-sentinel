//! SMTP sender (lettre). Credentials live in a 0600 file under ~/.dsh/secrets, never in config or the ledger.
use anyhow::{bail, Context, Result};
use lettre::message::{header::ContentType, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Smtp {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub from: String,
    pub to: String,
    /// "starttls" (587), "tls" (465) or "plain" (tests only, loopback)
    pub security: String,
}

/// KEY=VALUE file: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_FROM, MAIL_TO, SMTP_SECURITY.
pub fn load(path: &Path) -> Result<Smtp> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path).with_context(|| format!("missing SMTP credentials file {}", path.display()))?;
    if meta.permissions().mode() & 0o077 != 0 {
        bail!("{} holds a password and must be chmod 600", path.display());
    }
    let text = std::fs::read_to_string(path)?;
    let get = |k: &str| text.lines().filter_map(|l| l.trim().strip_prefix(&format!("{k}="))).next().map(|v| v.trim().trim_matches(['\'', '"']).to_string());
    let user = get("SMTP_USER").unwrap_or_default();
    Ok(Smtp {
        host: get("SMTP_HOST").unwrap_or_else(|| "smtp.gmail.com".into()),
        port: get("SMTP_PORT").and_then(|p| p.parse().ok()).unwrap_or(587),
        pass: get("SMTP_PASS").unwrap_or_default().replace(' ', ""),
        from: get("MAIL_FROM").unwrap_or_else(|| user.clone()),
        to: get("MAIL_TO").unwrap_or_else(|| user.clone()),
        security: get("SMTP_SECURITY").unwrap_or_else(|| "starttls".into()),
        user,
    })
}

pub fn send(s: &Smtp, subject: &str, text: &str, html: &str) -> Result<()> {
    let msg = Message::builder()
        .from(s.from.parse().context("MAIL_FROM")?)
        .to(s.to.parse().context("MAIL_TO")?)
        .subject(subject)
        .multipart(MultiPart::alternative().singlepart(SinglePart::builder().header(ContentType::TEXT_PLAIN).body(text.to_string())).singlepart(SinglePart::builder().header(ContentType::TEXT_HTML).body(html.to_string())))?;
    let b = match s.security.as_str() {
        "tls" => SmtpTransport::relay(&s.host)?,
        "plain" => SmtpTransport::builder_dangerous(&s.host),
        _ => SmtpTransport::starttls_relay(&s.host)?,
    };
    let mut b = b.port(s.port).timeout(Some(std::time::Duration::from_secs(30)));
    if !s.user.is_empty() && s.security != "plain" {
        b = b.credentials(Credentials::new(s.user.clone(), s.pass.clone()));
    }
    b.build().send(&msg).map(|_| ()).map_err(|e| anyhow::anyhow!("SMTP send failed: {}", e.to_string().replace(&s.pass, "[redacted]")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt;

    /// Minimal one-shot SMTP sink; returns the DATA it received.
    fn sink() -> (u16, std::thread::JoinHandle<String>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let h = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut r = BufReader::new(s.try_clone().unwrap());
            s.write_all(b"220 sink\r\n").unwrap();
            let (mut data, mut in_data) = (String::new(), false);
            loop {
                let mut line = String::new();
                if r.read_line(&mut line).unwrap() == 0 { break }
                if in_data {
                    if line == ".\r\n" { in_data = false; s.write_all(b"250 queued\r\n").unwrap(); } else { data.push_str(&line) }
                    continue;
                }
                let up = line.to_ascii_uppercase();
                if up.starts_with("EHLO") || up.starts_with("HELO") { s.write_all(b"250 sink\r\n").unwrap() }
                else if up.starts_with("DATA") { in_data = true; s.write_all(b"354 go\r\n").unwrap() }
                else if up.starts_with("QUIT") { s.write_all(b"221 bye\r\n").unwrap(); break }
                else { s.write_all(b"250 ok\r\n").unwrap() }
            }
            data
        });
        (port, h)
    }

    #[test]
    fn sends_multipart_over_smtp() {
        let (port, h) = sink();
        let s = Smtp { host: "127.0.0.1".into(), port, user: String::new(), pass: String::new(), from: "a@example.com".into(), to: "b@example.com".into(), security: "plain".into() };
        send(&s, "Subj", "plain body", "<b>html body</b>").unwrap();
        let data = h.join().unwrap();
        assert!(data.contains("Subject: Subj") && data.contains("plain body") && data.contains("html body"), "{data}");
    }
    #[test]
    fn rejects_world_readable_credentials_file() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("smtp.env");
        std::fs::write(&p, "SMTP_USER=x@gmail.com\nSMTP_PASS=abcd efgh\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load(&p).unwrap_err().to_string().contains("chmod 600"));
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        let s = load(&p).unwrap();
        assert_eq!(s.pass, "abcdefgh");
        assert_eq!((s.host.as_str(), s.port, s.to.as_str()), ("smtp.gmail.com", 587, "x@gmail.com"));
    }
}
