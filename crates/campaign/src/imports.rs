//! Excel (.xlsx/.xls/.ods) and CSV audience import.
//!
//! Header row required. Known columns (case/space-insensitive, common aliases):
//! user_id, email, phone, first_name, last_name, country, language, timezone.
//! Every other column becomes `attributes.<column>` and is usable in cohort rules.
//! Each row needs at least one of user_id / email / phone.

use std::collections::HashMap;
use std::io::Cursor;

use axum::{
    extract::{Multipart, Path, State},
    http::StatusCode,
    Json,
};
use calamine::{open_workbook_auto_from_rs, Data, Reader};
use metrics::counter;
use serde::Serialize;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::{error::*, App};

const CHUNK: usize = 5_000;
const MAX_ERRORS: usize = 1_000;
const MAX_ROWS: usize = 2_000_000;

#[derive(Clone)]
pub struct PhoneCfg {
    /// Country calling code applied to national numbers, e.g. "880" (Bangladesh).
    pub cc: String,
    /// National significant number length, e.g. 10 for BD (1712345678).
    pub national_len: usize,
}

/// Normalizes to E.164 (`+8801712345678`). Accepts `+880…`, `00880…`, `880…`,
/// `01712345678` and `1712345678` (Excel drops the leading 0 of numeric cells).
pub fn normalize_phone(raw: &str, cfg: &PhoneCfg) -> Option<String> {
    let t = raw.trim();
    if t.is_empty() || t.chars().any(|c| c.is_alphabetic()) {
        return None;
    }
    let digits: String = t.chars().filter(|c| c.is_ascii_digit()).collect();
    let e164 = if t.starts_with('+') {
        digits
    } else if let Some(rest) = digits.strip_prefix("00") {
        rest.to_string()
    } else if digits.starts_with(&cfg.cc) && digits.len() == cfg.cc.len() + cfg.national_len {
        digits
    } else if digits.starts_with('0') && digits.len() == cfg.national_len + 1 {
        format!("{}{}", cfg.cc, &digits[1..])
    } else if digits.len() == cfg.national_len {
        format!("{}{}", cfg.cc, digits)
    } else {
        return None;
    };
    (8..=15).contains(&e164.len()).then(|| format!("+{e164}"))
}

pub fn normalize_email(raw: &str) -> Option<String> {
    let e = raw.trim().to_lowercase();
    let (local, domain) = e.split_once('@')?;
    let ok = !local.is_empty()
        && e.len() <= 254
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !domain.contains('@')
        && !e.chars().any(char::is_whitespace);
    ok.then_some(e)
}

fn norm_header(h: &str) -> String {
    let s: String = h.trim().to_lowercase().chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }).collect();
    let s = s.trim_matches('_').to_string();
    match s.as_str() {
        "mobile" | "mobile_number" | "mobile_no" | "phone_number" | "phone_no" | "msisdn" | "cell" => "phone".into(),
        "e_mail" | "email_address" | "mail" => "email".into(),
        "firstname" | "first" | "given_name" | "name" => "first_name".into(),
        "lastname" | "last" | "surname" | "family_name" => "last_name".into(),
        "userid" | "id" | "user" | "customer_id" => "user_id".into(),
        "lang" | "locale" => "language".into(),
        "tz" | "time_zone" => "timezone".into(),
        _ => s,
    }
}

#[derive(Debug, Default, Clone)]
pub struct ContactRow {
    pub user_id: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub country: Option<String>,
    pub language: Option<String>,
    pub timezone: Option<String>,
    pub attributes: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RowError {
    pub row: usize,
    pub error: String,
}

fn cell_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.trim().to_string()).filter(|s| !s.is_empty()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn xlsx_cell(c: &Data) -> Option<Value> {
    match c {
        Data::Empty | Data::Error(_) => None,
        Data::String(s) => Some(s.trim()).filter(|s| !s.is_empty()).map(|s| json!(s)),
        Data::Int(i) => Some(json!(i)),
        Data::Float(f) if f.fract() == 0.0 && f.abs() < 1e15 => Some(json!(*f as i64)),
        Data::Float(f) => Some(json!(f)),
        Data::Bool(b) => Some(json!(b)),
        Data::DateTime(d) => d.as_datetime().map(|dt| json!(dt.format("%Y-%m-%dT%H:%M:%S").to_string())),
        Data::DateTimeIso(s) | Data::DurationIso(s) => Some(json!(s)),
    }
}

fn csv_cell(s: &str) -> Option<Value> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    // Keep leading-zero / plus strings (phones, zip codes) as text.
    if !t.starts_with('0') && !t.starts_with('+') {
        if let Ok(i) = t.parse::<i64>() {
            return Some(json!(i));
        }
        if let Ok(f) = t.parse::<f64>() {
            return Some(json!(f));
        }
    }
    match t.to_lowercase().as_str() {
        "true" | "yes" => Some(json!(true)),
        "false" | "no" => Some(json!(false)),
        _ => Some(json!(t)),
    }
}

/// Returns (header, rows of cells).
fn read_grid(filename: &str, bytes: Vec<u8>) -> Result<(Vec<String>, Vec<Vec<Option<Value>>>), String> {
    let lower = filename.to_lowercase();
    if lower.ends_with(".csv") || lower.ends_with(".txt") {
        let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(Cursor::new(bytes));
        let header = rdr.headers().map_err(|e| format!("csv header: {e}"))?.iter().map(String::from).collect();
        let mut rows = Vec::new();
        for rec in rdr.records() {
            let rec = rec.map_err(|e| format!("csv: {e}"))?;
            rows.push(rec.iter().map(csv_cell).collect());
            if rows.len() > MAX_ROWS {
                return Err(format!("file has more than {MAX_ROWS} rows; split it"));
            }
        }
        return Ok((header, rows));
    }
    let mut wb = open_workbook_auto_from_rs(Cursor::new(bytes)).map_err(|e| format!("not a readable Excel/ODS file: {e}"))?;
    let range = wb.worksheet_range_at(0).ok_or("workbook has no sheets")?.map_err(|e| format!("reading first sheet: {e}"))?;
    let mut it = range.rows();
    let header = it.next().ok_or("sheet is empty")?.iter().map(|c| xlsx_cell(c).and_then(|v| cell_text(&v)).unwrap_or_default()).collect();
    let rows: Vec<Vec<Option<Value>>> = it.map(|r| r.iter().map(xlsx_cell).collect()).collect();
    if rows.len() > MAX_ROWS {
        return Err(format!("file has more than {MAX_ROWS} rows; split it"));
    }
    Ok((header, rows))
}

pub fn parse_rows(
    header: &[String],
    rows: Vec<Vec<Option<Value>>>,
    phone: &PhoneCfg,
) -> Result<(Vec<(usize, ContactRow)>, Vec<RowError>), String> {
    let cols: Vec<String> = header.iter().map(|h| norm_header(h)).collect();
    if !cols.iter().any(|c| c == "user_id" || c == "email" || c == "phone") {
        return Err("header must include at least one of: user_id, email, phone".into());
    }
    let mut ok = Vec::with_capacity(rows.len());
    let mut errs = Vec::new();
    for (i, row) in rows.into_iter().enumerate() {
        let line = i + 2; // 1-based + header row
        if row.iter().all(Option::is_none) {
            continue;
        }
        let mut c = ContactRow::default();
        let mut err = None;
        for (col, cell) in cols.iter().zip(row) {
            let Some(v) = cell else { continue };
            let text = cell_text(&v);
            match col.as_str() {
                "user_id" => c.user_id = text.filter(|t| t.len() <= 128),
                "email" => match text.as_deref().map(normalize_email) {
                    Some(Some(e)) => c.email = Some(e),
                    Some(None) => err = Some(format!("invalid email '{}'", text.unwrap_or_default())),
                    None => {}
                },
                "phone" => match text.as_deref().map(|t| normalize_phone(t, phone)) {
                    Some(Some(p)) => c.phone = Some(p),
                    Some(None) => err = Some(format!("invalid phone '{}'", text.unwrap_or_default())),
                    None => {}
                },
                "first_name" => c.first_name = text,
                "last_name" => c.last_name = text,
                "country" => c.country = text.map(|t| t.to_uppercase()),
                "language" => c.language = text.map(|t| t.to_lowercase()),
                "timezone" => c.timezone = text,
                "" => {}
                key if key.len() <= 64 => {
                    c.attributes.insert(key.to_string(), v);
                }
                _ => {}
            }
        }
        if err.is_none() && c.user_id.is_none() && c.email.is_none() && c.phone.is_none() {
            err = Some("row needs user_id, email or phone".into());
        }
        match err {
            Some(e) => errs.push(RowError { row: line, error: e }),
            None => ok.push((line, c)),
        }
    }
    Ok((ok, errs))
}

/// Fills missing user_ids: reuse the existing contact with the same email/phone,
/// otherwise a deterministic id so re-importing the same file never duplicates.
async fn resolve_ids(app: &App, rows: &mut [ContactRow]) -> anyhow::Result<()> {
    let emails: Vec<String> = rows.iter().filter(|r| r.user_id.is_none()).filter_map(|r| r.email.clone()).collect();
    let phones: Vec<String> = rows.iter().filter(|r| r.user_id.is_none()).filter_map(|r| r.phone.clone()).collect();
    if emails.is_empty() && phones.is_empty() {
        return Ok(());
    }
    let found: Vec<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT user_id, email, phone FROM contacts WHERE email = ANY($1) OR phone = ANY($2)")
            .bind(&emails)
            .bind(&phones)
            .fetch_all(&app.db)
            .await?;
    let mut by_email = HashMap::new();
    let mut by_phone = HashMap::new();
    for (id, e, p) in found {
        if let Some(e) = e {
            by_email.insert(e, id.clone());
        }
        if let Some(p) = p {
            by_phone.insert(p, id);
        }
    }
    for r in rows.iter_mut().filter(|r| r.user_id.is_none()) {
        let existing = r.email.as_ref().and_then(|e| by_email.get(e)).or_else(|| r.phone.as_ref().and_then(|p| by_phone.get(p)));
        r.user_id = Some(match existing {
            Some(id) => id.clone(),
            None => {
                let key =
                    r.email.as_ref().map(|e| format!("email:{e}")).or_else(|| r.phone.as_ref().map(|p| format!("phone:{p}"))).unwrap();
                format!("imp-{}", Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()))
            }
        });
    }
    Ok(())
}

/// Upsert; import/API data never blanks existing fields and merges attributes.
/// Identity-synced contacts keep identity's email/phone/name (identity is the source
/// of truth); imports can only add attributes and the fields identity doesn't own.
pub async fn upsert_contacts(app: &App, rows: &[ContactRow], source: &str) -> anyhow::Result<u64> {
    let col = |f: fn(&ContactRow) -> &Option<String>| rows.iter().map(|r| f(r).clone()).collect::<Vec<_>>();
    let ids: Vec<String> = rows.iter().map(|r| r.user_id.clone().expect("resolved")).collect();
    let attrs: Vec<Value> = rows.iter().map(|r| Value::Object(r.attributes.clone())).collect();
    let res = sqlx::query(
        "INSERT INTO contacts (user_id, email, phone, first_name, last_name, country, language, timezone, attributes, source)
         SELECT u, e, p, f, l, co, la, tz, a, $10
         FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::jsonb[])
              AS t(u, e, p, f, l, co, la, tz, a)
         ON CONFLICT (user_id) DO UPDATE SET
           email      = CASE WHEN contacts.source = 'identity' THEN contacts.email ELSE COALESCE(EXCLUDED.email, contacts.email) END,
           phone      = CASE WHEN contacts.source = 'identity' THEN contacts.phone ELSE COALESCE(EXCLUDED.phone, contacts.phone) END,
           first_name = CASE WHEN contacts.source = 'identity' THEN contacts.first_name ELSE COALESCE(EXCLUDED.first_name, contacts.first_name) END,
           last_name  = CASE WHEN contacts.source = 'identity' THEN contacts.last_name ELSE COALESCE(EXCLUDED.last_name, contacts.last_name) END,
           country    = COALESCE(EXCLUDED.country, contacts.country),
           language   = COALESCE(EXCLUDED.language, contacts.language),
           timezone   = COALESCE(EXCLUDED.timezone, contacts.timezone),
           attributes = contacts.attributes || EXCLUDED.attributes,
           updated_at = now()",
    )
    .bind(&ids)
    .bind(col(|r| &r.email))
    .bind(col(|r| &r.phone))
    .bind(col(|r| &r.first_name))
    .bind(col(|r| &r.last_name))
    .bind(col(|r| &r.country))
    .bind(col(|r| &r.language))
    .bind(col(|r| &r.timezone))
    .bind(&attrs)
    .bind(source)
    .execute(&app.db)
    .await?;
    Ok(res.rows_affected())
}

/// Dedupes by user_id within a chunk (Postgres rejects two upserts of one key in a statement).
pub fn dedupe(rows: Vec<ContactRow>) -> Vec<ContactRow> {
    let mut idx: HashMap<String, usize> = HashMap::new();
    let mut out: Vec<ContactRow> = Vec::with_capacity(rows.len());
    for r in rows {
        let k = r.user_id.clone().expect("resolved");
        match idx.get(&k) {
            Some(&i) => {
                let prev = &mut out[i];
                let mut merged = r;
                for (k, v) in std::mem::take(&mut prev.attributes) {
                    merged.attributes.entry(k).or_insert(v);
                }
                merged.email = merged.email.or(prev.email.take());
                merged.phone = merged.phone.or(prev.phone.take());
                *prev = merged;
            }
            None => {
                idx.insert(k, out.len());
                out.push(r);
            }
        }
    }
    out
}

async fn process(app: App, import_id: Uuid, filename: String, bytes: Vec<u8>, segment_id: Option<Uuid>) {
    if let Err(e) = process_inner(&app, import_id, &filename, bytes, segment_id).await {
        tracing::error!(%import_id, err = %e, "import failed");
        let _ = sqlx::query(
            "UPDATE imports SET status = 'failed', finished_at = now(),
             errors = errors || jsonb_build_array(jsonb_build_object('row', 0, 'error', $2::text)) WHERE id = $1",
        )
        .bind(import_id)
        .bind(e.to_string())
        .execute(&app.db)
        .await;
    }
}

async fn process_inner(app: &App, import_id: Uuid, filename: &str, bytes: Vec<u8>, segment_id: Option<Uuid>) -> anyhow::Result<()> {
    let (header, grid) = read_grid(filename, bytes).map_err(anyhow::Error::msg)?;
    let (rows, errors) = parse_rows(&header, grid, &app.phone).map_err(anyhow::Error::msg)?;
    let total = (rows.len() + errors.len()) as i64;
    let errs_json = serde_json::to_value(errors.iter().take(MAX_ERRORS).collect::<Vec<_>>())?;
    sqlx::query("UPDATE imports SET total_rows = $2, failed_rows = $3, errors = $4 WHERE id = $1")
        .bind(import_id)
        .bind(total)
        .bind(errors.len() as i64)
        .bind(errs_json)
        .execute(&app.db)
        .await?;
    counter!("import_rows_total", "result" => "invalid").increment(errors.len() as u64);

    let mut rows: Vec<ContactRow> = rows.into_iter().map(|(_, r)| r).collect();
    for chunk in rows.chunks_mut(CHUNK) {
        resolve_ids(app, chunk).await?;
        let chunk = dedupe(chunk.to_vec());
        upsert_contacts(app, &chunk, "import").await?;
        if let Some(seg) = segment_id {
            let ids: Vec<String> = chunk.iter().filter_map(|r| r.user_id.clone()).collect();
            sqlx::query(
                "INSERT INTO segment_members (segment_id, user_id) SELECT $1, u FROM UNNEST($2::text[]) u
                 ON CONFLICT DO NOTHING",
            )
            .bind(seg)
            .bind(&ids)
            .execute(&app.db)
            .await?;
        }
        sqlx::query("UPDATE imports SET imported_rows = imported_rows + $2 WHERE id = $1")
            .bind(import_id)
            .bind(chunk.len() as i64)
            .execute(&app.db)
            .await?;
        counter!("import_rows_total", "result" => "imported").increment(chunk.len() as u64);
    }
    sqlx::query("UPDATE imports SET status = 'completed', finished_at = now() WHERE id = $1").bind(import_id).execute(&app.db).await?;
    tracing::info!(%import_id, total, "import completed");
    Ok(())
}

#[derive(Serialize, sqlx::FromRow)]
pub struct Import {
    id: Uuid,
    filename: String,
    status: String,
    total_rows: i64,
    imported_rows: i64,
    failed_rows: i64,
    errors: Value,
    segment_id: Option<Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// POST /v1/imports  (multipart: file=<xlsx|csv>, segment_name=<optional static cohort name>)
pub async fn create(State(app): State<App>, mut mp: Multipart) -> ApiResult<(StatusCode, Json<Value>)> {
    let mut file: Option<(String, Vec<u8>)> = None;
    let mut segment_name: Option<String> = None;
    while let Some(field) = mp.next_field().await.map_err(|e| AppError::bad(format!("multipart: {e}")))? {
        match field.name() {
            Some("file") => {
                let name = field.file_name().unwrap_or("upload.xlsx").to_string();
                let bytes = field.bytes().await.map_err(|e| AppError::bad(format!("upload: {e}")))?;
                file = Some((name, bytes.to_vec()));
            }
            Some("segment_name") => {
                let t = field.text().await.map_err(|e| AppError::bad(e.to_string()))?;
                segment_name = Some(t.trim().to_string()).filter(|s| !s.is_empty());
            }
            _ => {}
        }
    }
    let (filename, bytes) = file.ok_or_else(|| AppError::bad("missing 'file' field"))?;
    if bytes.is_empty() {
        return Err(AppError::bad("file is empty"));
    }

    let segment_id = match &segment_name {
        Some(name) => {
            let id = Uuid::now_v7();
            sqlx::query("INSERT INTO segments (id, name, kind) VALUES ($1, $2, 'static')").bind(id).bind(name).execute(&app.db).await?;
            Some(id)
        }
        None => None,
    };
    let id = Uuid::now_v7();
    sqlx::query("INSERT INTO imports (id, filename, status, segment_id) VALUES ($1, $2, 'processing', $3)")
        .bind(id)
        .bind(&filename)
        .bind(segment_id)
        .execute(&app.db)
        .await?;
    tokio::spawn(process(app.clone(), id, filename, bytes, segment_id));
    Ok((StatusCode::ACCEPTED, Json(json!({ "id": id, "segment_id": segment_id, "status": "processing" }))))
}

pub async fn get(State(app): State<App>, Path(id): Path<Uuid>) -> ApiResult<Json<Import>> {
    let row = sqlx::query_as::<_, Import>("SELECT * FROM imports WHERE id = $1").bind(id).fetch_one(&app.db).await?;
    Ok(Json(row))
}

pub async fn list(State(app): State<App>) -> ApiResult<Json<Vec<Import>>> {
    let rows = sqlx::query_as::<_, Import>(
        "SELECT id, filename, status, total_rows, imported_rows, failed_rows, '[]'::jsonb AS errors,
                segment_id, created_at, finished_at
         FROM imports ORDER BY created_at DESC LIMIT 100",
    )
    .fetch_all(&app.db)
    .await?;
    Ok(Json(rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bd() -> PhoneCfg {
        PhoneCfg { cc: "880".into(), national_len: 10 }
    }

    #[test]
    fn phones() {
        let want = Some("+8801712345678".to_string());
        for raw in ["+8801712345678", "008801712345678", "8801712345678", "01712345678", "1712345678", "017-1234 5678"] {
            assert_eq!(normalize_phone(raw, &bd()), want, "{raw}");
        }
        assert_eq!(normalize_phone("+14155550123", &bd()).as_deref(), Some("+14155550123"));
        assert_eq!(normalize_phone("12345", &bd()), None);
        assert_eq!(normalize_phone("call me", &bd()), None);
    }

    #[test]
    fn emails() {
        assert_eq!(normalize_email("  Rahim@Example.COM ").as_deref(), Some("rahim@example.com"));
        for bad in ["rahim", "@x.com", "a@b", "a b@x.com", "a@.com", "a@x.com."] {
            assert_eq!(normalize_email(bad), None, "{bad}");
        }
    }

    #[test]
    fn headers() {
        assert_eq!(norm_header(" Mobile Number "), "phone");
        assert_eq!(norm_header("E-mail"), "email");
        assert_eq!(norm_header("Total Orders"), "total_orders");
    }

    #[test]
    fn parses_rows_with_attributes_and_errors() {
        let header: Vec<String> = ["Name", "Mobile", "Email", "Plan", "Orders"].map(String::from).to_vec();
        let rows = vec![
            vec![Some(json!("Rahim")), Some(json!(1712345678)), None, Some(json!("gold")), Some(json!(7))],
            vec![Some(json!("Bad")), Some(json!("12")), None, None, None],
            vec![None, None, None, None, None],
            vec![Some(json!("NoContact")), None, None, Some(json!("x")), None],
        ];
        let (ok, errs) = parse_rows(&header, rows, &bd()).unwrap();
        assert_eq!(ok.len(), 1);
        let (line, c) = &ok[0];
        assert_eq!(*line, 2);
        assert_eq!(c.phone.as_deref(), Some("+8801712345678"));
        assert_eq!(c.first_name.as_deref(), Some("Rahim"));
        assert_eq!(c.attributes["plan"], json!("gold"));
        assert_eq!(c.attributes["orders"], json!(7));
        assert_eq!(errs.iter().map(|e| e.row).collect::<Vec<_>>(), vec![3, 5]);
    }

    #[test]
    fn csv_cells_keep_phone_text() {
        assert_eq!(csv_cell("01712345678"), Some(json!("01712345678")));
        assert_eq!(csv_cell("42"), Some(json!(42)));
        assert_eq!(csv_cell("yes"), Some(json!(true)));
    }

    #[test]
    fn dedupes_and_merges() {
        let a = ContactRow {
            user_id: Some("u1".into()),
            email: Some("a@x.com".into()),
            attributes: Map::from_iter([("p".to_string(), json!(1))]),
            ..Default::default()
        };
        let b = ContactRow { user_id: Some("u1".into()), phone: Some("+8801712345678".into()), ..Default::default() };
        let out = dedupe(vec![a, b]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].email.as_deref(), Some("a@x.com"));
        assert_eq!(out[0].phone.as_deref(), Some("+8801712345678"));
        assert_eq!(out[0].attributes["p"], json!(1));
    }
}
