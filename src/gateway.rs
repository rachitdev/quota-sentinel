//! OmniRoute REST client behind a trait so the actuator can be fault-injection tested.
//! Loopback-only: the management key must never leave this machine.
use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use std::time::Duration;

pub trait Gateway {
    fn connections(&self) -> Result<Vec<Value>>;
    fn connection(&self, id: &str) -> Result<Option<Value>>;
    fn usage(&self, id: &str) -> Result<Value>;
    /// Model ids the connection currently serves (GET /api/providers/<id>/models).
    fn models(&self, id: &str) -> Result<Vec<String>>;
    /// PATCH /api/providers/<id>. Callers send ONLY `isActive` and/or `providerSpecificData.qsHold`.
    fn patch_connection(&self, id: &str, body: &Value) -> Result<()>;
}

pub struct HttpGateway {
    base: String,
    key: String,
    agent: ureq::Agent,
}

fn is_loopback(url: &str) -> bool {
    let rest = url.strip_prefix("http://").unwrap_or("");
    let host = rest.split(['/', ':']).next().unwrap_or("");
    matches!(host, "localhost" | "127.0.0.1" | "[::1]")
}

pub fn read_key(path: &std::path::Path) -> Result<String> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading key file {}", path.display()))?;
    for line in text.lines() {
        if let Some(v) = line.trim().strip_prefix("OMNIROUTE_API_KEY=") {
            let v = v.trim().trim_matches(['\'', '"']);
            if !v.is_empty() {
                return Ok(v.to_string());
            }
        }
    }
    bail!("OMNIROUTE_API_KEY not found in {}", path.display())
}

impl HttpGateway {
    pub fn new(base: &str, key: String) -> Result<Self> {
        if !is_loopback(base) {
            bail!("gateway_url must be http://localhost or 127.0.0.1 (refusing to send the management key elsewhere)");
        }
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(20)))
            .build()
            .into();
        Ok(HttpGateway { base: base.trim_end_matches('/').to_string(), key, agent })
    }

    fn get(&self, path: &str) -> Result<(u16, Value)> {
        let mut r = self
            .agent
            .get(format!("{}{}", self.base, path))
            .header("Authorization", format!("Bearer {}", self.key))
            .call()
            .map_err(|e| anyhow!("GET {path}: {}", redact(&e.to_string(), &self.key)))?;
        let code = r.status().as_u16();
        let v: Value = r.body_mut().read_json().unwrap_or(Value::Null);
        Ok((code, v))
    }
}

fn redact(s: &str, key: &str) -> String {
    if key.is_empty() { s.to_string() } else { s.replace(key, "[redacted]") }
}

impl Gateway for HttpGateway {
    fn connections(&self) -> Result<Vec<Value>> {
        let (c, v) = self.get("/api/providers")?;
        if c != 200 {
            bail!("GET /api/providers -> {c}");
        }
        Ok(v.get("connections").and_then(|x| x.as_array()).cloned().unwrap_or_default())
    }

    fn connection(&self, id: &str) -> Result<Option<Value>> {
        let (c, v) = self.get(&format!("/api/providers/{id}"))?;
        match c {
            200 => Ok(v.get("connection").cloned()),
            404 => Ok(None),
            _ => bail!("GET connection {id} -> {c}"),
        }
    }

    fn usage(&self, id: &str) -> Result<Value> {
        let (c, v) = self.get(&format!("/api/usage/{id}"))?;
        // 400 "Usage not available" is a real, expected answer: return it as data so it is never read as recovery.
        if c == 200 || c == 400 {
            return Ok(v);
        }
        bail!("GET usage {id} -> {c}")
    }

    fn models(&self, id: &str) -> Result<Vec<String>> {
        let (c, v) = self.get(&format!("/api/providers/{id}/models"))?;
        if c != 200 {
            bail!("GET models {id} -> {c}");
        }
        Ok(v.get("models").and_then(|m| m.as_array()).map(|a| a.iter().filter_map(|m| m.get("id")?.as_str().map(String::from)).collect()).unwrap_or_default())
    }

    fn patch_connection(&self, id: &str, body: &Value) -> Result<()> {
        let mut r = self
            .agent
            .patch(format!("{}/api/providers/{id}", self.base))
            .header("Authorization", format!("Bearer {}", self.key))
            .send_json(body)
            .map_err(|e| anyhow!("PATCH {id}: {}", redact(&e.to_string(), &self.key)))?;
        let code = r.status().as_u16();
        if code != 200 {
            let msg: Value = r.body_mut().read_json().unwrap_or(Value::Null);
            bail!("PATCH {id} -> {code} {}", msg.to_string().chars().take(200).collect::<String>());
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod fake {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[derive(Default)]
    pub struct FakeGateway {
        pub conns: RefCell<BTreeMap<String, Value>>,
        pub usage: RefCell<BTreeMap<String, Value>>,
        pub models: RefCell<BTreeMap<String, Vec<String>>>,
        pub patches: RefCell<Vec<(String, Value)>>,
        /// next PATCH fails and is NOT applied
        pub fail_patch: RefCell<bool>,
        /// next PATCH is applied but reports an error (ambiguous timeout)
        pub apply_but_err: RefCell<bool>,
        /// before the next connection() read, run this mutation (a competing writer)
        pub drift: RefCell<Option<Box<dyn Fn(&mut Value)>>>,
    }

    impl FakeGateway {
        pub fn add(&self, id: &str, provider: &str, active: bool) {
            self.conns.borrow_mut().insert(
                id.into(),
                json!({"id": id, "provider": provider, "name": id, "isActive": active, "providerSpecificData": {}}),
            );
        }
        pub fn get(&self, id: &str) -> Value {
            self.conns.borrow()[id].clone()
        }
        fn apply(&self, id: &str, body: &Value) {
            let mut m = self.conns.borrow_mut();
            let c = m.get_mut(id).unwrap();
            if let Some(a) = body.get("isActive").and_then(|x| x.as_bool()) {
                c["isActive"] = json!(a);
            }
            if let Some(p) = body.get("providerSpecificData").and_then(|x| x.as_object()) {
                for (k, v) in p {
                    c["providerSpecificData"][k] = v.clone();
                }
            }
        }
    }

    impl Gateway for FakeGateway {
        fn connections(&self) -> Result<Vec<Value>> {
            Ok(self.conns.borrow().values().cloned().collect())
        }
        fn connection(&self, id: &str) -> Result<Option<Value>> {
            if let Some(f) = self.drift.borrow_mut().take() {
                if let Some(c) = self.conns.borrow_mut().get_mut(id) {
                    f(c);
                }
            }
            Ok(self.conns.borrow().get(id).cloned())
        }
        fn usage(&self, id: &str) -> Result<Value> {
            self.usage.borrow().get(id).cloned().ok_or_else(|| anyhow!("no usage"))
        }
        fn models(&self, id: &str) -> Result<Vec<String>> {
            self.models.borrow().get(id).cloned().ok_or_else(|| anyhow!("no models"))
        }
        fn patch_connection(&self, id: &str, body: &Value) -> Result<()> {
            self.patches.borrow_mut().push((id.into(), body.clone()));
            if self.fail_patch.replace(false) {
                bail!("injected failure");
            }
            self.apply(id, body);
            if self.apply_but_err.replace(false) {
                bail!("injected ambiguous timeout");
            }
            Ok(())
        }
    }
}
