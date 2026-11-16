//! Message templates with personalization tokens.
//!
//! `{{first_name}}`, `{{attributes.plan}}`, `{{discount}}` are filled from the message's
//! variables; `{{first_name|Customer}}` falls back to "Customer" when the value is
//! missing or empty. Shared by the campaign service (preview) and the workers (send),
//! so what marketers preview is exactly what is sent.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What the worker needs to render a message (cached from Redis).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Template {
    /// Email subject / push title. None for SMS.
    #[serde(default)]
    pub title: Option<String>,
    pub body: String,
    /// Don't store the rendered text in delivery history (e.g. OTP codes).
    #[serde(default)]
    pub hide_content: bool,
}

pub fn redis_key(channel: &str, key: &str) -> String {
    format!("tpl:{channel}:{key}")
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Token {
    pub path: String,
    pub default: Option<String>,
}

pub const MAX_BODY: usize = 5_000;

/// Walks the text, replacing each `{{ path | default }}` with `f(token)`.
fn scan(text: &str, mut f: impl FnMut(&Token) -> String) -> Result<String, String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find("{{") {
        out.push_str(&rest[..i]);
        let after = &rest[i + 2..];
        let j = after.find("}}").ok_or("a {{ is never closed with }}")?;
        let inner = after[..j].trim();
        let (path, default) = match inner.split_once('|') {
            Some((p, d)) => (p.trim(), Some(d.trim().to_string())),
            None => (inner, None),
        };
        let valid = !path.is_empty()
            && path.len() <= 64
            && path.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
            && !path.starts_with('.')
            && !path.ends_with('.')
            && !path.contains("..");
        if !valid {
            return Err(format!("invalid token {{{{{inner}}}}}: use letters, digits, _ and . (e.g. {{{{first_name}}}})"));
        }
        if default.as_ref().is_some_and(|d| d.len() > 200 || d.contains('{') || d.contains('}')) {
            return Err(format!("invalid default in {{{{{inner}}}}}"));
        }
        out.push_str(&f(&Token { path: path.to_string(), default }));
        rest = &after[j + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// The tokens a text uses (validates syntax).
pub fn tokens(text: &str) -> Result<Vec<Token>, String> {
    let mut found = Vec::new();
    scan(text, |t| {
        if !found.iter().any(|f: &Token| f.path == t.path) {
            found.push(t.clone());
        }
        String::new()
    })?;
    Ok(found)
}

pub fn lookup<'a>(vars: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(vars, |v, k| v.get(k))
}

fn as_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) if s.trim().is_empty() => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        other => Some(other.to_string()),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Rendered {
    pub text: String,
    /// Tokens with no value and no default (rendered as empty).
    pub missing: Vec<String>,
}

pub fn render(text: &str, vars: &Value) -> Result<Rendered, String> {
    let mut missing = Vec::new();
    let text = scan(text, |t| match lookup(vars, &t.path).and_then(as_text) {
        Some(v) => v,
        None => match &t.default {
            Some(d) => d.clone(),
            None => {
                if !missing.contains(&t.path) {
                    missing.push(t.path.clone());
                }
                String::new()
            }
        },
    })?;
    Ok(Rendered { text, missing })
}

/// Validates a template before it is saved.
pub fn validate(title: Option<&str>, body: &str) -> Result<Vec<Token>, String> {
    if body.trim().is_empty() {
        return Err("message text is empty".into());
    }
    if body.len() > MAX_BODY {
        return Err(format!("message text is over {MAX_BODY} characters"));
    }
    let mut all = tokens(body)?;
    if let Some(t) = title {
        if t.len() > 300 {
            return Err("title is over 300 characters".into());
        }
        for tok in tokens(t)? {
            if !all.iter().any(|a| a.path == tok.path) {
                all.push(tok);
            }
        }
    }
    Ok(all)
}

// ------------------------------------------------------------------ SMS length

const GSM_BASIC: &str =
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà";
const GSM_EXTENDED: &str = "^{}\\[~]|€\u{000C}";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SmsInfo {
    /// "GSM-7" (160 per SMS) or "Unicode" (70 per SMS: Bengali, emoji, curly quotes...).
    pub encoding: &'static str,
    /// Characters as the network counts them.
    pub length: usize,
    /// Billable SMS parts.
    pub segments: usize,
    pub per_segment: usize,
}

pub fn sms_info(text: &str) -> SmsInfo {
    let mut units = 0usize;
    let gsm = text.chars().all(|c| {
        if GSM_BASIC.contains(c) {
            units += 1;
            true
        } else if GSM_EXTENDED.contains(c) {
            units += 2; // escape + char
            true
        } else {
            false
        }
    });
    if gsm {
        let segments = match units {
            0 => 0,
            1..=160 => 1,
            n => n.div_ceil(153),
        };
        SmsInfo { encoding: "GSM-7", length: units, segments, per_segment: if segments > 1 { 153 } else { 160 } }
    } else {
        let u = text.encode_utf16().count();
        let segments = match u {
            0 => 0,
            1..=70 => 1,
            n => n.div_ceil(67),
        };
        SmsInfo { encoding: "Unicode", length: u, segments, per_segment: if segments > 1 { 67 } else { 70 } }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_tokens_defaults_and_nested_attributes() {
        let vars = json!({"first_name": "Rahim", "discount": 20, "attributes": {"plan": "gold"}, "empty": ""});
        let r =
            render("Hi {{first_name}}, {{ discount }}% off for {{attributes.plan}} members. {{empty|-}} {{city|Dhaka}}", &vars).unwrap();
        assert_eq!(r.text, "Hi Rahim, 20% off for gold members. - Dhaka");
        assert!(r.missing.is_empty());
    }

    #[test]
    fn missing_tokens_are_reported_once() {
        let r = render("{{last_name}} {{last_name}} {{order_id}}", &json!({})).unwrap();
        assert_eq!(r.text, "  ");
        assert_eq!(r.missing, vec!["last_name", "order_id"]);
    }

    #[test]
    fn rejects_bad_syntax() {
        assert!(render("Hi {{first_name", &json!({})).is_err());
        assert!(render("Hi {{first name}}", &json!({})).is_err());
        assert!(render("Hi {{.x}}", &json!({})).is_err());
        assert!(render("Hi {{x|{{y}}}}", &json!({})).is_err());
        assert!(validate(None, "   ").is_err());
    }

    #[test]
    fn values_are_inserted_as_text_not_reparsed() {
        // A value that looks like a token must not be expanded again.
        let r = render("Hi {{first_name}}", &json!({"first_name": "{{secret}}"})).unwrap();
        assert_eq!(r.text, "Hi {{secret}}");
    }

    #[test]
    fn lists_tokens() {
        let t = validate(Some("{{first_name}}, a deal"), "Hi {{first_name|there}}: {{attributes.plan}}").unwrap();
        let paths: Vec<_> = t.iter().map(|t| t.path.as_str()).collect();
        assert_eq!(paths, vec!["first_name", "attributes.plan"]);
    }

    #[test]
    fn sms_segments() {
        assert_eq!(sms_info(&"a".repeat(160)).segments, 1);
        assert_eq!(sms_info(&"a".repeat(161)).segments, 2);
        assert_eq!(sms_info(&"a".repeat(306)).segments, 2);
        assert_eq!(sms_info(&"a".repeat(307)).segments, 3);
        let ext = sms_info("[x]");
        assert_eq!((ext.encoding, ext.length), ("GSM-7", 5));
        let bn = sms_info("আপনার কোড 4821");
        assert_eq!(bn.encoding, "Unicode");
        assert_eq!(bn.segments, 1);
        assert_eq!(sms_info(&"ক".repeat(71)).segments, 2);
        assert_eq!(sms_info("Hi 😀").encoding, "Unicode");
    }
}
