//! "Free-only" holds: instead of disabling a whole connection at the quota threshold, exclude every paid model on it
//! (OmniRoute `providerSpecificData.excludedModels`) so only quota-free models stay routable.
//!
//! Why not just disable it: a disabled connection also removes the free models, and a paid-model request that reaches the
//! upstream after the pool is exhausted makes the upstream answer 429, which puts the WHOLE connection in cooldown (free
//! models included). Excluding paid models up front keeps them from ever reaching the upstream.
//!
//! OmniRoute exclusion patterns support `*` and `?` only (no negation), so the paid set is listed explicitly from the
//! connection's synced model list. A model that appears after the last sync is not covered until the next reconcile.
use serde_json::Value;

/// Which subscriptions are held "free-only", and which model ids count as free.
#[derive(Clone, Copy)]
pub struct FreeOnly<'a> {
    pub subs: &'a [String],
    pub patterns: &'a [String],
}

impl<'a> FreeOnly<'a> {
    pub fn none() -> Self {
        FreeOnly { subs: &[], patterns: &[] }
    }
    pub fn applies(&self, subscription: &str) -> bool {
        self.subs.iter().any(|s| s == subscription)
    }
}

/// Case-insensitive `*` / `?` wildcard match, mirroring OmniRoute's `wildcardMatch` for the characters we use.
pub fn wildcard(text: &str, pattern: &str) -> bool {
    let (t, p): (Vec<char>, Vec<char>) = (text.to_lowercase().chars().collect(), pattern.to_lowercase().chars().collect());
    let (mut ti, mut pi, mut star, mut mark) = (0usize, 0usize, None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn is_free(model: &str, free_patterns: &[String]) -> bool {
    free_patterns.iter().any(|p| wildcard(model, p))
}

/// Exclusion patterns that block every non-free model in `models`.
/// Each paid id is listed, plus `<id>-*` for effort variants (`glm-5.2-high`) unless that would also hit a free model.
pub fn paid_exclusions(models: &[String], free_patterns: &[String]) -> Vec<String> {
    let free: Vec<&String> = models.iter().filter(|m| is_free(m, free_patterns)).collect();
    let mut out: Vec<String> = Vec::new();
    for m in models.iter().filter(|m| !is_free(m, free_patterns)) {
        out.push(m.clone());
        let variants = format!("{m}-*");
        if !free.iter().any(|f| wildcard(f, &variants)) {
            out.push(variants);
        }
    }
    out.sort();
    out.dedup();
    out
}

/// The connection's own `excludedModels` (array or comma-separated string), as OmniRoute reads it.
pub fn excluded_of(psd: &Value) -> Vec<String> {
    match psd.get("excludedModels").or_else(|| psd.get("excluded_models")) {
        Some(Value::Array(a)) => a.iter().filter_map(|v| v.as_str()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
        Some(Value::String(s)) => s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect(),
        _ => vec![],
    }
}

/// `current` with `ours` added (keeps the user's own exclusions).
pub fn with_ours(current: &[String], ours: &[String]) -> Vec<String> {
    let mut v: Vec<String> = current.to_vec();
    v.extend(ours.iter().cloned());
    v.sort();
    v.dedup();
    v
}

/// `current` with `ours` removed, as a PATCH value: an empty result is `null` so the key is cleared.
pub fn without_ours(current: &[String], ours: &[String]) -> Value {
    let v: Vec<&String> = current.iter().filter(|c| !ours.contains(c)).collect();
    if v.is_empty() { Value::Null } else { serde_json::json!(v) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn wildcard_matches_like_omniroute() {
        assert!(wildcard("crouton-alpha-free", "*-free"));
        assert!(wildcard("Crouton-Alpha-FREE", "*-free"));
        assert!(!wildcard("kimi-k3", "*-free"));
        assert!(wildcard("glm-5.2-high", "glm-5.2-*"));
        assert!(!wildcard("glm-5.2", "glm-5.2-*"));
        assert!(wildcard("anything", "*"));
        assert!(wildcard("a-b", "a?b"));
    }

    #[test]
    fn free_models_are_never_excluded_and_every_paid_one_is() {
        let models = s(&["deepseek-v4.1-flash", "glm-5.2", "crouton-alpha-free", "iceman-contributor-free", "beam-free"]);
        let ex = paid_exclusions(&models, &s(&["*-free"]));
        for paid in ["deepseek-v4.1-flash", "glm-5.2", "glm-5.2-high", "deepseek-v4.1-flash-max"] {
            assert!(ex.iter().any(|p| wildcard(paid, p)), "{paid} must be blocked");
        }
        for free in ["crouton-alpha-free", "iceman-contributor-free", "beam-free"] {
            assert!(!ex.iter().any(|p| wildcard(free, p)), "{free} must stay routable");
        }
    }

    #[test]
    fn variant_pattern_is_dropped_when_it_would_hit_a_free_model() {
        let models = s(&["kimi-k3", "kimi-k3-free"]);
        let ex = paid_exclusions(&models, &s(&["*-free"]));
        assert_eq!(ex, s(&["kimi-k3"]));
        assert!(!ex.iter().any(|p| wildcard("kimi-k3-free", p)));
    }

    #[test]
    fn excluded_of_reads_array_and_comma_string() {
        assert_eq!(excluded_of(&json!({"excludedModels": ["a", " b "]})), s(&["a", "b"]));
        assert_eq!(excluded_of(&json!({"excludedModels": "a, b,,"})), s(&["a", "b"]));
        assert!(excluded_of(&json!({})).is_empty());
    }

    #[test]
    fn user_exclusions_survive_add_and_remove() {
        let user = s(&["my-bad-model"]);
        let ours = s(&["glm-5.2", "glm-5.2-*"]);
        let both = with_ours(&user, &ours);
        assert!(both.contains(&"my-bad-model".to_string()) && both.contains(&"glm-5.2".to_string()));
        assert_eq!(without_ours(&both, &ours), json!(["my-bad-model"]));
        assert_eq!(without_ours(&ours, &ours), Value::Null);
    }
}
