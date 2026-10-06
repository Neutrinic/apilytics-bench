use async_compression::tokio::bufread::GzipDecoder;
use axum::{
    Router, body::Body, extract::{Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, path::PathBuf, sync::{Arc, Mutex, RwLock}, time::{Duration, Instant, SystemTime}};
use tokio::{fs::File, io::BufReader};
use tokio_util::io::ReaderStream;

#[derive(Clone, Deserialize)]
#[serde(default)]
struct Profile {
    latency_ms: u64,
    rate_per_second: u64,
    retry_after: u64,
    retry_format: String,
    errors: f64,
    seed: u64,
}
impl Default for Profile {
    fn default() -> Self {
        Self { latency_ms: 0, rate_per_second: 0, retry_after: 1, retry_format: "seconds".into(), errors: 0.0, seed: 42 }
    }
}
#[derive(Clone, Deserialize)]
struct Manifest { row_count: u64 }
struct Counters { window: u64, in_window: u64, requests: u64 }
struct App {
    root: PathBuf,
    public_base: String,
    started: Instant,
    counters: Mutex<Counters>,
    profile: RwLock<Option<(SystemTime, Profile)>>,
    manifests: RwLock<HashMap<String, (SystemTime, Manifest)>>,
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(json!({"error": message}))).into_response()
}
fn add_header(response: &mut Response, name: &'static str, value: &str) {
    response.headers_mut().insert(name, HeaderValue::from_str(value).expect("validated header"));
}
fn fixture(path: &str) -> Option<(String, String)> {
    let mut parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    if parts.len() == 2 { parts = vec![parts[0], "100mb", "clean", parts[1]]; }
    if parts.len() != 4 || !["taxi", "lineitem"].contains(&parts[0])
        || !["100mb", "2gb", "10gb"].contains(&parts[1])
        || !["clean", "messy"].contains(&parts[2]) { return None; }
    Some((parts[..3].join("/"), parts[3].into()))
}
fn accepts_gzip(headers: &HeaderMap) -> bool {
    let mut wildcard = false;
    for value in headers.get_all(header::ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else { continue; };
        for coding in value.split(',') {
            let mut parts = coding.split(';');
            let name = parts.next().unwrap_or_default().trim();
            let mut quality = 1.0;
            for parameter in parts {
                if let Some(q) = parameter.trim().strip_prefix("q=") { quality = q.parse::<f64>().unwrap_or(0.0); }
            }
            if name.eq_ignore_ascii_case("gzip") { return quality > 0.0; }
            if name == "*" { wildcard = quality > 0.0; }
        }
    }
    wildcard
}
fn pagination(query: &HashMap<String, String>, cursor: bool) -> Result<(u64, u64), &'static str> {
    let limit = query.get("limit").map(|s| s.parse::<u64>()).unwrap_or(Ok(5000)).map_err(|_| "invalid pagination")?;
    let offset = query.get(if cursor { "cursor" } else { "offset" }).map(|s| s.parse::<u64>()).unwrap_or(Ok(0)).map_err(|_| "invalid pagination")?;
    if ![500, 5000].contains(&limit) { return Err("limit must be 500 or 5000; offset nonnegative"); }
    Ok((limit, offset))
}
// Reproducible per-request sampling; no cryptographic use. Atomic under the counters lock.
fn sample(seed: u64, counter: u64) -> f64 {
    let mut x = seed.wrapping_add(counter).wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    ((x ^ (x >> 31)) >> 11) as f64 / ((1u64 << 53) as f64)
}
impl App {
    async fn profile(&self) -> Result<Profile, String> {
        let path = self.root.join("profile.json");
        let stamp = match tokio::fs::metadata(&path).await {
            Ok(m) => m.modified().map_err(|e| e.to_string())?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Profile::default()),
            Err(e) => return Err(e.to_string()),
        };
        if let Some((cached, profile)) = self.profile.read().unwrap().as_ref() {
            if *cached == stamp { return Ok(profile.clone()); }
        }
        let profile: Profile = serde_json::from_slice(&tokio::fs::read(path).await.map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        if !profile.errors.is_finite() || !(0.0..=1.0).contains(&profile.errors)
            || profile.retry_after < 1 || !["seconds", "date"].contains(&profile.retry_format.as_str()) {
            return Err("invalid fault profile".into());
        }
        *self.profile.write().unwrap() = Some((stamp, profile.clone()));
        Ok(profile)
    }
    async fn manifest(&self, key: &str) -> Result<Manifest, StatusCode> {
        let path = self.root.join("data").join(key).join("manifest.json");
        let stamp = tokio::fs::metadata(&path).await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
            .modified().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Some((cached, manifest)) = self.manifests.read().unwrap().get(key) {
            if *cached == stamp { return Ok(manifest.clone()); }
        }
        let manifest: Manifest = serde_json::from_slice(&tokio::fs::read(path).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        self.manifests.write().unwrap().insert(key.to_owned(), (stamp, manifest.clone()));
        Ok(manifest)
    }
}

async fn file_response(path: PathBuf, kind: &'static str, compressible: bool, gzip: bool, head: bool) -> Response {
    let plain = !compressible || tokio::fs::try_exists(&path).await.unwrap_or(false);
    let compressed_path = PathBuf::from(format!("{}.gz", path.display()));
    let send_gzip = compressible && gzip;
    let chosen = if send_gzip || !plain { compressed_path } else { path };
    let file = match File::open(chosen).await {
        Ok(f) => f,
        Err(e) => return error(if e.kind() == std::io::ErrorKind::NotFound { StatusCode::NOT_FOUND } else { StatusCode::INTERNAL_SERVER_ERROR }, "fixture file unavailable"),
    };
    let length = file.metadata().await.ok().map(|m| m.len());
    let body = if head { Body::empty() }
        else if !plain && !send_gzip {
            Body::from_stream(ReaderStream::with_capacity(GzipDecoder::new(BufReader::with_capacity(256 * 1024, file)), 256 * 1024))
        } else { Body::from_stream(ReaderStream::with_capacity(file, 256 * 1024)) };
    let mut response = body.into_response();
    add_header(&mut response, "content-type", kind);
    if compressible { add_header(&mut response, "vary", "Accept-Encoding"); }
    if send_gzip { add_header(&mut response, "content-encoding", "gzip"); }
    if plain || send_gzip {
        if let Some(length) = length { add_header(&mut response, "content-length", &length.to_string()); }
    }
    response
}

async fn serve(State(app): State<Arc<App>>, method: Method, uri: Uri, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response {
    if method != Method::GET && method != Method::HEAD {
        let mut response = error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
        add_header(&mut response, "allow", "GET, HEAD");
        return response;
    }
    if uri.path().trim_matches('/') == "health" {
        let mut ready = 0;
        for d in ["taxi", "lineitem"] { for s in ["100mb", "2gb", "10gb"] { for v in ["clean", "messy"] {
            if tokio::fs::try_exists(app.root.join(format!("data/{d}/{s}/{v}/manifest.json"))).await.unwrap_or(false) { ready += 1; }
        } } }
        return axum::Json(json!({"status":"ok", "ready":ready})).into_response();
    }
    let Some((key, style)) = fixture(uri.path()) else { return error(StatusCode::NOT_FOUND, "not found"); };
    let manifest = match app.manifest(&key).await {
        Ok(m) => m,
        Err(status) => return error(status, "not ready"),
    };
    let root = app.root.join("data").join(&key);
    if ["manifest.json", "openapi.yaml"].contains(&style.as_str()) {
        return file_response(root.join(&style), if style == "openapi.yaml" { "application/yaml" } else { "application/json" }, false, false, method == Method::HEAD).await;
    }
    let profile = match app.profile().await {
        Ok(p) => p,
        Err(e) => { eprintln!("profile: {e}"); return error(StatusCode::SERVICE_UNAVAILABLE, "invalid profile"); }
    };
    if profile.latency_ms > 0 { tokio::time::sleep(Duration::from_millis(profile.latency_ms)).await; }
    let (limited, fault) = {
        let mut counters = app.counters.lock().unwrap();
        let window = app.started.elapsed().as_secs();
        if window != counters.window { counters.window = window; counters.in_window = 0; }
        counters.in_window += 1;
        counters.requests = counters.requests.wrapping_add(1);
        (profile.rate_per_second > 0 && counters.in_window > profile.rate_per_second, sample(profile.seed, counters.requests) < profile.errors)
    };
    if limited {
        let mut response = error(StatusCode::TOO_MANY_REQUESTS, "synthetic rate limit");
        let retry = if profile.retry_format == "date" {
            httpdate::fmt_http_date(SystemTime::now().checked_add(Duration::from_secs(profile.retry_after)).unwrap_or(SystemTime::now()))
        } else { profile.retry_after.to_string() };
        add_header(&mut response, "retry-after", &retry);
        return response;
    }
    if fault { return error(StatusCode::INTERNAL_SERVER_ERROR, "synthetic fault"); }
    let gzip = accepts_gzip(&headers);
    if style == "export.ndjson" { return file_response(root.join("export.ndjson"), "application/x-ndjson", true, gzip, method == Method::HEAD).await; }
    if !["offset", "cursor", "link"].contains(&style.as_str()) { return error(StatusCode::NOT_FOUND, "not found"); }
    let (limit, offset) = match pagination(&query, style == "cursor") {
        Ok(p) => p,
        Err(e) => return error(StatusCode::BAD_REQUEST, e),
    };
    if offset >= manifest.row_count {
        return axum::Json(if style == "cursor" { json!({"items":[],"next":""}) } else { json!({"results":[]}) }).into_response();
    }
    if offset % limit != 0 { return error(StatusCode::BAD_REQUEST, "static fixture offsets must align with limit"); }
    let file = root.join(format!("{}/{limit}/{offset}.json", if style == "cursor" { "cursor" } else { "offset" }));
    let mut response = file_response(file, "application/json", true, gzip, method == Method::HEAD).await;
    if style == "link" && offset.saturating_add(limit) < manifest.row_count && response.status().is_success() {
        add_header(&mut response, "link", &format!("<{}/{key}/link?offset={}&limit={limit}>; rel=\"next\"", app.public_base, offset + limit));
    }
    response
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The root holds data/ and profile.json; the public base is the URL clients use, for Link headers.
    let root = PathBuf::from(std::env::var("SYNTHETIC_ROOT").unwrap_or_else(|_| ".".into()));
    let address = std::env::var("SYNTHETIC_LISTEN").unwrap_or_else(|_| "127.0.0.1:18601".into());
    let public_base = std::env::var("SYNTHETIC_PUBLIC_BASE").unwrap_or_else(|_| "http://127.0.0.1:18600".into()).trim_end_matches('/').to_owned();
    // Refuse invalid header values at startup rather than panic on a user request.
    HeaderValue::from_str(&public_base)?;
    let app = Arc::new(App { root, public_base, started: Instant::now(),
        counters: Mutex::new(Counters { window: 0, in_window: 0, requests: 0 }),
        profile: RwLock::new(None), manifests: RwLock::new(HashMap::new()) });
    let router = Router::new().fallback(serve).with_state(app);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("synthetic-rest-axum listening on {address}");
    axum::serve(listener, router).with_graceful_shutdown(async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn restrict_paths() {
        assert_eq!(fixture("/taxi/offset"), Some(("taxi/100mb/clean".into(), "offset".into())));
        assert!(fixture("/taxi/../clean/offset").is_none());
        assert!(fixture("/taxi/100mb/messy/offset/extra").is_none());
    }
    #[test]
    fn reject_bad_pagination() {
        assert_eq!(pagination(&HashMap::new(), false).unwrap(), (5000, 0));
        assert!(pagination(&HashMap::from([("offset".into(), "-1".into())]), false).is_err());
        assert!(pagination(&HashMap::from([("limit".into(), "42".into())]), false).is_err());
    }
    #[test]
    fn gzip_negotiation() {
        for (value, expected) in [("gzip", true), ("gzip;q=0", false), ("br, gzip;q=0.5", true), ("identity", false), ("*;q=1, gzip;q=0", false)] {
            let mut h = HeaderMap::new(); h.insert(header::ACCEPT_ENCODING, value.parse().unwrap());
            assert_eq!(accepts_gzip(&h), expected);
        }
    }
}
