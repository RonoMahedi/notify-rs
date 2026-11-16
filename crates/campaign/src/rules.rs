//! Cohort rule engine: a small JSON DSL compiled to parameterized SQL.
//!
//! ```json
//! {"all": [
//!   {"field": "country", "op": "eq", "value": "BD"},
//!   {"any": [
//!     {"field": "attributes.plan", "op": "in", "value": ["premium", "gold"]},
//!     {"field": "attributes.orders", "op": "gte", "value": 5}
//!   ]},
//!   {"field": "last_active_at", "op": "within", "value": "30d"}
//! ]}
//! ```
//!
//! Safety: field names are matched against an allow-list (or a strict
//! `attributes.<key>` pattern) and every value goes through `push_bind`.
//! No user-supplied text is ever concatenated into SQL.

use chrono::{DateTime, NaiveDate, Utc};
use common::model::Channel;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};

const MAX_DEPTH: usize = 6;
const MAX_CONDITIONS: usize = 50;
const MAX_LIST: usize = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Rule {
    All {
        all: Vec<Rule>,
    },
    Any {
        any: Vec<Rule>,
    },
    Not {
        not: Box<Rule>,
    },
    Cond {
        field: String,
        op: Op,
        #[serde(default)]
        value: Value,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Eq,
    Ne,
    In,
    NotIn,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    Exists,
    NotExists,
    /// Time field is within the last N (e.g. "30d", "12h", "2w").
    Within,
    /// Time field is older than N.
    OlderThan,
}

enum Field {
    Text(&'static str),
    Time(&'static str),
    Attr(String),
}

fn parse_field(f: &str) -> Result<Field, String> {
    Ok(match f {
        "user_id" => Field::Text("c.user_id"),
        "email" => Field::Text("c.email"),
        "phone" => Field::Text("c.phone"),
        "first_name" => Field::Text("c.first_name"),
        "last_name" => Field::Text("c.last_name"),
        "country" => Field::Text("c.country"),
        "language" => Field::Text("c.language"),
        "timezone" => Field::Text("c.timezone"),
        "source" => Field::Text("c.source"),
        "last_active_at" => Field::Time("c.last_active_at"),
        "created_at" => Field::Time("c.created_at"),
        "updated_at" => Field::Time("c.updated_at"),
        other => match other.strip_prefix("attributes.") {
            Some(k) if !k.is_empty() && k.len() <= 64 && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') => {
                Field::Attr(k.to_string())
            }
            _ => {
                return Err(format!(
                    "unknown field '{other}'. Use a contact field (country, language, timezone, \
                     source, email, phone, first_name, last_name, user_id, last_active_at, \
                     created_at, updated_at) or attributes.<key>"
                ))
            }
        },
    })
}

fn as_text(v: &Value) -> Result<String, String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err(format!("expected a text value, got {v}")),
    }
}

fn as_text_list(v: &Value) -> Result<Vec<String>, String> {
    let arr = v.as_array().ok_or("expected a list value")?;
    if arr.is_empty() || arr.len() > MAX_LIST {
        return Err(format!("list must have 1..={MAX_LIST} items"));
    }
    arr.iter().map(as_text).collect()
}

fn like_pattern(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
    format!("%{escaped}%")
}

pub fn parse_time(v: &Value) -> Result<DateTime<Utc>, String> {
    let s = v.as_str().ok_or("expected a date string (RFC 3339 or YYYY-MM-DD)")?;
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Ok(t.with_timezone(&Utc));
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc()).map_err(|_| format!("invalid date '{s}'"))
}

/// "30d" / "12h" / "90m" / "45s" / "2w" -> seconds.
pub fn parse_duration(v: &Value) -> Result<i64, String> {
    let s = v.as_str().ok_or("expected a duration like \"30d\"")?.trim();
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let n: i64 = num.parse().map_err(|_| format!("invalid duration '{s}'"))?;
    let mult = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        _ => return Err(format!("invalid duration unit in '{s}' (use s, m, h, d, w)")),
    };
    if n <= 0 || n > 3650 * 86_400 / mult {
        return Err(format!("duration '{s}' out of range"));
    }
    Ok(n * mult)
}

fn cmp_sym(op: Op) -> &'static str {
    match op {
        Op::Gt => " > ",
        Op::Gte => " >= ",
        Op::Lt => " < ",
        Op::Lte => " <= ",
        _ => unreachable!(),
    }
}

fn cond(field: &str, op: Op, value: &Value, qb: &mut QueryBuilder<'_, Postgres>) -> Result<(), String> {
    let unsupported = || Err(format!("operator '{op:?}' is not supported for field '{field}'"));
    match parse_field(field)? {
        Field::Text(col) => match op {
            Op::Eq => {
                qb.push(col).push(" = ").push_bind(as_text(value)?);
            }
            Op::Ne => {
                qb.push(col).push(" IS DISTINCT FROM ").push_bind(as_text(value)?);
            }
            Op::In => {
                qb.push(col).push(" = ANY(").push_bind(as_text_list(value)?).push(")");
            }
            Op::NotIn => {
                qb.push("COALESCE(").push(col).push(" <> ALL(").push_bind(as_text_list(value)?).push("), TRUE)");
            }
            Op::Contains => {
                qb.push(col).push(" ILIKE ").push_bind(like_pattern(&as_text(value)?));
            }
            Op::Exists => {
                qb.push(col).push(" IS NOT NULL");
            }
            Op::NotExists => {
                qb.push(col).push(" IS NULL");
            }
            _ => return unsupported(),
        },
        Field::Time(col) => match op {
            Op::Gt | Op::Gte | Op::Lt | Op::Lte => {
                qb.push(col).push(cmp_sym(op)).push_bind(parse_time(value)?);
            }
            Op::Within => {
                qb.push(col).push(" >= now() - make_interval(secs => ").push_bind(parse_duration(value)? as f64).push(")");
            }
            Op::OlderThan => {
                qb.push(col).push(" < now() - make_interval(secs => ").push_bind(parse_duration(value)? as f64).push(")");
            }
            Op::Exists => {
                qb.push(col).push(" IS NOT NULL");
            }
            Op::NotExists => {
                qb.push(col).push(" IS NULL");
            }
            _ => return unsupported(),
        },
        Field::Attr(key) => match op {
            // `@>` is served by the GIN (jsonb_path_ops) index.
            Op::Eq => {
                qb.push("c.attributes @> ").push_bind(json!({ key: value }));
            }
            Op::Ne => {
                qb.push("NOT (c.attributes @> ").push_bind(json!({ key: value })).push(")");
            }
            Op::In | Op::NotIn => {
                let arr = value.as_array().ok_or("expected a list value")?;
                if arr.is_empty() || arr.len() > MAX_LIST {
                    return Err(format!("list must have 1..={MAX_LIST} items"));
                }
                if op == Op::NotIn {
                    qb.push("NOT ");
                }
                qb.push("(").push_bind(value.clone()).push(" @> jsonb_build_array(c.attributes -> ").push_bind(key).push("))");
            }
            Op::Gt | Op::Gte | Op::Lt | Op::Lte => {
                if !value.is_number() {
                    return Err(format!("'{field}' {op:?} needs a numeric value"));
                }
                // Type guard first: non-numeric attribute values simply don't match.
                qb.push("(jsonb_typeof(c.attributes -> ")
                    .push_bind(key.clone())
                    .push(") = 'number' AND (c.attributes -> ")
                    .push_bind(key)
                    .push(")")
                    .push(cmp_sym(op))
                    .push_bind(value.clone())
                    .push(")");
            }
            Op::Contains => {
                qb.push("(c.attributes ->> ").push_bind(key).push(") ILIKE ").push_bind(like_pattern(&as_text(value)?));
            }
            Op::Exists => {
                qb.push("c.attributes ? ").push_bind(key);
            }
            Op::NotExists => {
                qb.push("NOT (c.attributes ? ").push_bind(key).push(")");
            }
            Op::Within | Op::OlderThan => return unsupported(),
        },
    }
    Ok(())
}

fn compile_inner(rule: &Rule, qb: &mut QueryBuilder<'_, Postgres>, depth: usize, n: &mut usize) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err(format!("rule nesting deeper than {MAX_DEPTH}"));
    }
    let group = |items: &Vec<Rule>, sep: &str, empty: &str, qb: &mut QueryBuilder<'_, Postgres>, n: &mut usize| {
        if items.is_empty() {
            qb.push(empty);
            return Ok(());
        }
        qb.push("(");
        for (i, r) in items.iter().enumerate() {
            if i > 0 {
                qb.push(sep);
            }
            compile_inner(r, qb, depth + 1, n)?;
        }
        qb.push(")");
        Ok(())
    };
    match rule {
        Rule::All { all } => group(all, " AND ", "TRUE", qb, n),
        Rule::Any { any } => group(any, " OR ", "FALSE", qb, n),
        Rule::Not { not } => {
            qb.push("NOT (");
            compile_inner(not, qb, depth + 1, n)?;
            qb.push(")");
            Ok(())
        }
        Rule::Cond { field, op, value } => {
            *n += 1;
            if *n > MAX_CONDITIONS {
                return Err(format!("more than {MAX_CONDITIONS} conditions"));
            }
            cond(field, *op, value, qb)
        }
    }
}

pub fn compile(rule: &Rule, qb: &mut QueryBuilder<'_, Postgres>) -> Result<(), String> {
    compile_inner(rule, qb, 0, &mut 0)
}

/// Parse + fully validate a rule without touching the database.
pub fn validate(v: &Value) -> Result<Rule, String> {
    let rule: Rule = serde_json::from_value(v.clone()).map_err(|_| {
        "invalid rule: expected {\"all\":[..]}, {\"any\":[..]}, {\"not\":{..}} or \
         {\"field\":..,\"op\":..,\"value\":..}"
            .to_string()
    })?;
    let mut qb = QueryBuilder::<Postgres>::new("");
    compile(&rule, &mut qb)?;
    Ok(rule)
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Segment {
    pub id: uuid::Uuid,
    pub name: String,
    pub kind: String,
    pub rule: Option<Value>,
    pub created_at: DateTime<Utc>,
}

/// Reachability on a channel, as a boolean SQL expression over alias `c`.
/// Static SQL only; no user input.
pub fn reach_sql(ch: Channel) -> &'static str {
    match ch {
        Channel::Email => "(c.email IS NOT NULL AND NOT ('email' = ANY(c.unsubscribed)))",
        Channel::Sms => "(c.phone IS NOT NULL AND NOT ('sms' = ANY(c.unsubscribed)))",
        Channel::Push => {
            "(EXISTS (SELECT 1 FROM devices d WHERE d.user_id = c.user_id) \
             AND NOT ('push' = ANY(c.unsubscribed)))"
        }
        Channel::InApp => "(NOT ('in_app' = ANY(c.unsubscribed)))",
    }
}

/// Appends `WHERE ...` selecting the segment's reachable, active contacts (alias `c`).
pub fn push_audience(qb: &mut QueryBuilder<'_, Postgres>, seg: &Segment, channel: Option<Channel>) -> Result<(), String> {
    qb.push(" WHERE c.status = 'active'");
    if let Some(ch) = channel {
        qb.push(" AND ").push(reach_sql(ch));
    }
    match seg.kind.as_str() {
        "all" => {}
        "static" => {
            qb.push(" AND EXISTS (SELECT 1 FROM segment_members m WHERE m.segment_id = ")
                .push_bind(seg.id)
                .push(" AND m.user_id = c.user_id)");
        }
        "rule" => {
            let rule = validate(seg.rule.as_ref().ok_or("rule segment without rule")?)?;
            qb.push(" AND ");
            compile(&rule, qb)?;
        }
        k => return Err(format!("unknown segment kind {k}")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql(v: Value) -> Result<String, String> {
        let rule = validate(&v)?;
        let mut qb = QueryBuilder::<Postgres>::new("");
        compile(&rule, &mut qb)?;
        Ok(qb.sql().to_string())
    }

    #[test]
    fn compiles_nested_rule_with_binds_only() {
        let s = sql(json!({"all": [
            {"field": "country", "op": "eq", "value": "BD"},
            {"any": [
                {"field": "attributes.plan", "op": "in", "value": ["premium", "gold"]},
                {"field": "attributes.orders", "op": "gte", "value": 5}
            ]},
            {"field": "last_active_at", "op": "within", "value": "30d"}
        ]}))
        .unwrap();
        assert!(s.starts_with("(c.country = $1 AND ("));
        assert!(s.contains("make_interval(secs => $"));
        assert!(!s.contains("BD") && !s.contains("premium"), "values must be bound: {s}");
    }

    #[test]
    fn rejects_injection_in_field_names() {
        assert!(sql(json!({"field": "country; DROP TABLE contacts", "op": "eq", "value": 1})).is_err());
        assert!(sql(json!({"field": "attributes.a'b", "op": "eq", "value": 1})).is_err());
        assert!(sql(json!({"field": "attributes.", "op": "exists"})).is_err());
    }

    #[test]
    fn rejects_bad_ops_and_values() {
        assert!(sql(json!({"field": "country", "op": "gt", "value": "A"})).is_err());
        assert!(sql(json!({"field": "attributes.orders", "op": "gt", "value": "5"})).is_err());
        assert!(sql(json!({"field": "last_active_at", "op": "within", "value": "30x"})).is_err());
        assert!(sql(json!({"field": "country", "op": "in", "value": []})).is_err());
        assert!(sql(json!({"bogus": 1})).is_err());
    }

    #[test]
    fn limits_depth() {
        let mut r = json!({"field": "country", "op": "eq", "value": "BD"});
        for _ in 0..10 {
            r = json!({"all": [r]});
        }
        assert!(sql(r).unwrap_err().contains("nesting"));
    }

    #[test]
    fn durations_and_dates() {
        assert_eq!(parse_duration(&json!("30d")).unwrap(), 30 * 86_400);
        assert_eq!(parse_duration(&json!("2w")).unwrap(), 14 * 86_400);
        assert!(parse_duration(&json!("0d")).is_err());
        assert!(parse_time(&json!("2026-01-31")).is_ok());
        assert!(parse_time(&json!("2026-01-31T10:00:00+06:00")).is_ok());
        assert!(parse_time(&json!("31/01/2026")).is_err());
    }

    #[test]
    fn like_is_escaped() {
        assert_eq!(like_pattern("50%_off"), "%50\\%\\_off%");
    }
}
