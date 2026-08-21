//! Google sign-in for the public listener.
//!
//! The shape is one sentence, and that is the point: **the internal port has no
//! authentication and is reachable only from inside the cluster; the public port
//! requires a session on every route.** Not "every route except the control
//! plane" — every route. There is no path table to reason about, so no ingress
//! rule can expose something by accident, and the same router serves both ports
//! unchanged.
//!
//! Two exemptions, both structural rather than chosen: `/auth/*`, because you
//! cannot require a session in order to obtain one, and the static shell, which
//! has to render the page the sign-in button lives on.
//!
//! The claim checks mirror `queen-proxy`'s `validate_google_claims`, including
//! its `hd` OR email-domain form. Diverging would give two products in one house
//! two different answers to "who may sign in", and the proxy's version is the
//! one that works for an adopter whose domain is not a Workspace.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const GOOGLE_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";
const GOOGLE_ISSUERS: [&str; 2] = ["https://accounts.google.com", "accounts.google.com"];

const JWKS_TTL: Duration = Duration::from_secs(3600);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// The signed `state` only has to survive one round trip through Google.
const STATE_TTL_S: i64 = 300;
pub const COOKIE: &str = "gate_session";

#[derive(Clone)]
pub struct AuthConfig {
    pub client_id: String,
    pub client_secret: String,
    /// Empty means every Google account is accepted, which is almost never what
    /// anyone wants — so an empty list is refused at boot rather than treated
    /// as "allow all" the way a permissive default would.
    pub allowed_domains: Vec<String>,
    pub public_url: String,
    secret: Vec<u8>,
}

/// Who may change anything.
///
/// Read from the environment on every call, and deliberately NOT part of the
/// Google configuration. Two reasons, and the second is why it moved: removing
/// someone in Helm has to take effect on their next request rather than in
/// eight hours, and the local bypass has no Google client at all — with this
/// buried in `AuthConfig`, a laptop would report every developer as a viewer
/// while cheerfully accepting their writes.
pub fn is_admin(email: &str) -> bool {
    let email = email.trim().to_lowercase();
    std::env::var("GATE_ADMIN_EMAILS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .any(|a| !a.is_empty() && a == email)
}

impl AuthConfig {
    /// `None` when the public listener is not configured: a deployment that only
    /// serves the cluster needs none of this, and inventing a client id it does
    /// not have would fail at the first redirect instead of at boot.
    pub fn from_env() -> Option<Result<Self, String>> {
        let client_id = std::env::var("GOOGLE_CLIENT_ID").ok()?;
        Some((|| {
            let client_secret = std::env::var("GOOGLE_CLIENT_SECRET")
                .map_err(|_| "GOOGLE_CLIENT_SECRET is required when GOOGLE_CLIENT_ID is set")?;
            let allowed_domains: Vec<String> = std::env::var("GOOGLE_ALLOWED_DOMAINS")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect();
            if allowed_domains.is_empty() {
                return Err(
                    "GOOGLE_ALLOWED_DOMAINS is required: an empty list would admit every Google \
                     account on earth, and a console that retunes production ceilings should not \
                     have that as a default"
                        .to_string(),
                );
            }
            let secret = std::env::var("GATE_SESSION_SECRET")
                .map_err(|_| "GATE_SESSION_SECRET is required: it signs the session cookie")?;
            if secret.len() < 32 {
                return Err("GATE_SESSION_SECRET must be at least 32 bytes".to_string());
            }
            Ok(AuthConfig {
                client_id,
                client_secret,
                allowed_domains,
                public_url: std::env::var("GATE_PUBLIC_URL")
                    .unwrap_or_else(|_| "http://localhost:8789".to_string()),
                secret: secret.into_bytes(),
            })
        })())
    }

    fn redirect_uri(&self) -> String {
        // Byte-identical to what is registered with Google: no wildcards, no
        // tolerance for a trailing slash. Derived from the declared public URL
        // rather than from the request's Host header, because behind an ingress
        // that header is whatever the caller sent — and it decides where Google
        // sends the authorisation code.
        format!(
            "{}/api/auth/google/callback",
            self.public_url.trim_end_matches('/')
        )
    }
}

/// What the session cookie carries. Deliberately thin: an identity and an
/// expiry, nothing an operator could be surprised to find in a browser.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Session {
    pub sub: String,
    pub email: String,
    pub exp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateClaims {
    nonce: String,
    /// Where to land after the round trip, so a deep link survives sign-in.
    next: String,
    exp: i64,
}

#[derive(Debug, Deserialize)]
struct GoogleClaims {
    sub: Option<String>,
    email: Option<String>,
    email_verified: Option<Value>,
    hd: Option<String>,
    nonce: Option<String>,
}

pub enum ClaimErr {
    NonceMismatch,
    MissingSub,
    MissingEmail,
    EmailUnverified,
    DomainNotAllowed,
}

/// Mirrors `queen-proxy`'s check of the same name, including the `hd` OR
/// email-domain form.
fn validate_google_claims(
    claims: &GoogleClaims,
    expected_nonce: &str,
    allowed_domains: &[String],
) -> Result<Session, ClaimErr> {
    match claims.nonce.as_deref() {
        Some(n) if n == expected_nonce => {}
        _ => return Err(ClaimErr::NonceMismatch),
    }
    let sub = claims
        .sub
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(sub) = sub else {
        return Err(ClaimErr::MissingSub);
    };
    let email = claims
        .email
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(email) = email else {
        return Err(ClaimErr::MissingEmail);
    };
    if !truthy(claims.email_verified.as_ref()) {
        return Err(ClaimErr::EmailUnverified);
    }
    let email = email.to_lowercase();
    if !allowed_domains.is_empty() {
        let hd = claims.hd.as_deref().unwrap_or("").trim().to_lowercase();
        let domain = email.rsplit_once('@').map(|(_, d)| d).unwrap_or("");
        let ok =
            allowed_domains.iter().any(|d| d == &hd) || allowed_domains.iter().any(|d| d == domain);
        if !ok {
            return Err(ClaimErr::DomainNotAllowed);
        }
    }
    Ok(Session {
        sub: sub.to_string(),
        email,
        exp: 0,
    })
}

fn truthy(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    }
}

// ------------------------------------------------------------------- JWKS

static JWKS: RwLock<Option<(Value, Instant)>> = RwLock::new(None);

/// Google's keys, cached. On a refresh failure a stale copy is preferable to
/// locking everyone out: the keys rotate slowly and an outage at Google should
/// not become an outage here.
async fn jwks(http: &reqwest::Client) -> Result<Value, String> {
    if let Some((v, at)) = JWKS.read().as_ref() {
        if at.elapsed() < JWKS_TTL {
            return Ok(v.clone());
        }
    }
    match http.get(GOOGLE_JWKS_URL).timeout(HTTP_TIMEOUT).send().await {
        Ok(r) => match r.json::<Value>().await {
            Ok(v) => {
                *JWKS.write() = Some((v.clone(), Instant::now()));
                Ok(v)
            }
            Err(e) => stale_or(format!("jwks decode: {e}")),
        },
        Err(e) => stale_or(format!("jwks fetch: {e}")),
    }
}

fn stale_or(err: String) -> Result<Value, String> {
    match JWKS.read().as_ref() {
        Some((v, _)) => {
            tracing::warn!(error = %err, "google jwks refresh failed; using stale");
            Ok(v.clone())
        }
        None => Err(err),
    }
}

async fn verify_id_token(
    http: &reqwest::Client,
    cfg: &AuthConfig,
    id_token: &str,
) -> Result<GoogleClaims, String> {
    let header = decode_header(id_token).map_err(|e| format!("id_token header: {e}"))?;
    let kid = header.kid.ok_or("id_token has no kid")?;
    let keys = jwks(http).await?;
    let (n, e) = keys["keys"]
        .as_array()
        .and_then(|ks| {
            ks.iter()
                .find(|k| k["kid"].as_str() == Some(&kid))
                .map(|k| {
                    (
                        k["n"].as_str().unwrap_or("").to_string(),
                        k["e"].as_str().unwrap_or("").to_string(),
                    )
                })
        })
        .ok_or("no matching jwks key")?;
    let key = DecodingKey::from_rsa_components(&n, &e).map_err(|e| format!("jwks key: {e}"))?;
    let mut v = Validation::new(Algorithm::RS256);
    v.set_audience(std::slice::from_ref(&cfg.client_id));
    v.set_issuer(&GOOGLE_ISSUERS);
    decode::<GoogleClaims>(id_token, &key, &v)
        .map(|d| d.claims)
        .map_err(|e| format!("id_token: {e}"))
}

// ------------------------------------------------------------------ routes

pub struct Auth {
    pub cfg: AuthConfig,
    http: reqwest::Client,
}

impl Auth {
    pub fn new(cfg: AuthConfig) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
        }
    }

    fn sign<T: Serialize>(&self, claims: &T) -> Result<String, String> {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(&self.cfg.secret),
        )
        .map_err(|e| e.to_string())
    }

    pub fn verify_session(&self, token: &str) -> Option<Session> {
        decode::<Session>(
            token,
            &DecodingKey::from_secret(&self.cfg.secret),
            &Validation::new(Algorithm::HS256),
        )
        .ok()
        .map(|d| d.claims)
    }
}

fn now() -> i64 {
    crate::now_ms() / 1000
}

/// A nonce with no dependency on a random crate: the session secret is already
/// the thing whose secrecy everything here rests on, so a keyed hash of it plus
/// a monotonic instant is as unguessable as the secret itself.
fn nonce(secret: &[u8]) -> String {
    let t = crate::now_ms();
    let mut h: u64 = 1469598103934665603;
    for b in secret.iter().chain(t.to_le_bytes().iter()) {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    format!("{h:016x}{:x}", t)
}

pub async fn login(
    State(app): State<crate::api::Shared>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Some(auth) = app.auth.as_ref() else {
        return (StatusCode::NOT_FOUND, "sign-in is not configured").into_response();
    };
    let n = nonce(&auth.cfg.secret);
    let state = match auth.sign(&StateClaims {
        nonce: n.clone(),
        next: q.get("next").cloned().unwrap_or_else(|| "/".into()),
        exp: now() + STATE_TTL_S,
    }) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    };
    let url = format!(
        "{GOOGLE_AUTH_URL}?client_id={}&redirect_uri={}&response_type=code&scope=openid%20email%20profile&state={}&nonce={}&hd={}&prompt=select_account",
        urlencode(&auth.cfg.client_id),
        urlencode(&auth.cfg.redirect_uri()),
        urlencode(&state),
        urlencode(&n),
        // A HINT to Google about which account to offer. It is not a control:
        // the caller can pick any account, which is why the `hd` claim on the
        // returned token is checked and this is not.
        urlencode(auth.cfg.allowed_domains.first().map(String::as_str).unwrap_or("")),
    );
    Redirect::to(&url).into_response()
}

pub async fn callback(
    State(app): State<crate::api::Shared>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Some(auth) = app.auth.as_ref() else {
        return (StatusCode::NOT_FOUND, "sign-in is not configured").into_response();
    };
    let (Some(code), Some(state)) = (q.get("code"), q.get("state")) else {
        return (StatusCode::BAD_REQUEST, "missing code or state").into_response();
    };

    let mut v = Validation::new(Algorithm::HS256);
    v.set_required_spec_claims(&["exp"]);
    let st = match decode::<StateClaims>(state, &DecodingKey::from_secret(&auth.cfg.secret), &v) {
        Ok(d) => d.claims,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid or expired state").into_response(),
    };

    let form = [
        ("code", code.as_str()),
        ("client_id", auth.cfg.client_id.as_str()),
        ("client_secret", auth.cfg.client_secret.as_str()),
        ("redirect_uri", &auth.cfg.redirect_uri()),
        ("grant_type", "authorization_code"),
    ];
    let tok: Value = match auth
        .http
        .post(GOOGLE_TOKEN_URL)
        .timeout(HTTP_TIMEOUT)
        .form(&form)
        .send()
        .await
    {
        Ok(r) => match r.json().await {
            Ok(v) => v,
            Err(e) => {
                return (StatusCode::BAD_GATEWAY, format!("token decode: {e}")).into_response()
            }
        },
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("token exchange: {e}")).into_response(),
    };
    let Some(id_token) = tok.get("id_token").and_then(|v| v.as_str()) else {
        // Google's own words, not ours. A refused exchange answers 4xx with
        // {error, error_description} and no id_token, so reporting only the
        // missing field describes the symptom and hides the cause — which on
        // stage was "The provided client secret is invalid" showing up as a
        // blank page saying `no id_token in the token response`.
        let why = tok
            .get("error_description")
            .or_else(|| tok.get("error"))
            .and_then(|v| v.as_str())
            .unwrap_or("no id_token and no error in the token response");
        tracing::warn!(error = %why, "google token exchange refused");
        return (
            StatusCode::BAD_GATEWAY,
            format!("google refused the token exchange: {why}"),
        )
            .into_response();
    };

    let claims = match verify_id_token(&auth.http, &auth.cfg, id_token).await {
        Ok(c) => c,
        Err(e) => return (StatusCode::UNAUTHORIZED, e).into_response(),
    };
    let mut session = match validate_google_claims(&claims, &st.nonce, &auth.cfg.allowed_domains) {
        Ok(s) => s,
        // Deliberately does not echo the rejected domain: the caller knows which
        // account they used, and this reply is reachable by anyone.
        Err(ClaimErr::DomainNotAllowed) => {
            return (
                StatusCode::FORBIDDEN,
                "google account domain is not allowed",
            )
                .into_response()
        }
        Err(ClaimErr::EmailUnverified) => {
            return (StatusCode::FORBIDDEN, "provider email is not verified").into_response()
        }
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid id_token").into_response(),
    };

    // Eight hours and no refresh. A signed cookie cannot be revoked one holder
    // at a time — rotating the secret signs everyone out — so the lifetime IS
    // the revocation window, and it is kept to a working day on purpose.
    session.exp = now() + 8 * 3600;
    let token = match auth.sign(&session) {
        Ok(t) => t,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    };

    let secure = auth.cfg.public_url.starts_with("https://");
    let cookie = format!(
        "{COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{}",
        8 * 3600,
        if secure { "; Secure" } else { "" }
    );
    (
        StatusCode::SEE_OTHER,
        [(header::SET_COOKIE, cookie), (header::LOCATION, st.next)],
    )
        .into_response()
}

pub async fn logout() -> Response {
    (
        StatusCode::SEE_OTHER,
        [
            (
                header::SET_COOKIE,
                format!("{COOKIE}=; Path=/; HttpOnly; Max-Age=0"),
            ),
            (header::LOCATION, "/".to_string()),
        ],
    )
        .into_response()
}

pub fn session_of(headers: &axum::http::HeaderMap, auth: &Auth) -> Option<Session> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    let token = raw
        .split(';')
        .filter_map(|c| c.trim().split_once('='))
        .find(|(k, _)| *k == COOKIE)
        .map(|(_, v)| v)?;
    auth.verify_session(token)
}

/// A signed-in identity conjured without Google, for running the console on a
/// laptop.
///
/// Every bypass like this one eventually ships enabled, so it is fenced twice
/// and neither fence is a comment. It has to be asked for by name, AND the
/// public URL must not be `https` — which is what a real deployment behind an
/// ingress always is. Setting it in a Helm chart therefore does not weaken
/// anything: the process refuses to boot instead.
///
/// The identity it mints is a real session that goes through the real cookie
/// and the real gate, so local development exercises the same code path rather
/// than a second one that only works on a laptop.
pub fn dev_identity(cfg: Option<&AuthConfig>) -> Option<Session> {
    let email = std::env::var("GATE_DEV_EMAIL").ok()?;
    if let Some(c) = cfg {
        if c.public_url.starts_with("https://") {
            return None;
        }
    }
    Some(Session {
        sub: format!("dev:{email}"),
        email,
        exp: 0,
    })
}

/// `true` when the console is running without a Google client at all.
pub fn is_dev() -> bool {
    std::env::var("GATE_DEV_EMAIL").is_ok()
}

/// The public listener's gate. Everything needs a session except the two things
/// that structurally cannot: the sign-in routes, and the static shell that
/// renders the page holding the sign-in button.
pub async fn require_session(
    State(app): State<crate::api::Shared>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path().to_string();
    let exempt = path.starts_with("/api/auth/") || path == "/health" || is_shell(&path);
    if exempt {
        return next.run(req).await;
    }
    // The laptop case: no Google client at all, or one that is configured but
    // deliberately stood down for local work.
    if let Some(dev) = dev_identity(app.auth.as_ref().map(|a| &a.cfg)) {
        if writes(req.method()) && !is_admin(&dev.email) {
            return (
                StatusCode::FORBIDDEN,
                "read-only: this account is not in GATE_ADMIN_EMAILS",
            )
                .into_response();
        }
        let mut req = req;
        req.extensions_mut().insert(dev);
        return next.run(req).await;
    }
    let Some(auth) = app.auth.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "sign-in is not configured").into_response();
    };
    match session_of(req.headers(), auth) {
        Some(s) => {
            let identity = s.clone();
            // Read is anyone who got through Google; write is the admin list.
            // Keyed on the METHOD rather than on a list of paths, so a route
            // added tomorrow cannot be born unprotected because someone forgot
            // to enumerate it.
            if writes(req.method()) && !is_admin(&s.email) {
                return (
                    StatusCode::FORBIDDEN,
                    "read-only: this account is not in GATE_ADMIN_EMAILS",
                )
                    .into_response();
            }
            // Carried on the request rather than re-parsed downstream: a
            // handler that has to look at a cookie to know who it is talking to
            // cannot tell which listener it is on, and the two answer
            // differently.
            let mut req = req;
            req.extensions_mut().insert(identity);
            next.run(req).await
        }
        // An API caller gets a status it can act on; a browser gets sent to
        // Google. Distinguished by what the caller said it accepts, because a
        // redirect rendered inside a fetch() is a confusing way to say 401.
        None => {
            let wants_html = req
                .headers()
                .get(header::ACCEPT)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|a| a.contains("text/html"));
            if wants_html {
                Redirect::to(&format!("/api/auth/google/login?next={}", urlencode(&path)))
                    .into_response()
            } else {
                (StatusCode::UNAUTHORIZED, "sign in required").into_response()
            }
        }
    }
}

/// Read is anyone who got through the door; write is the admin list. Keyed on
/// the METHOD rather than on a list of paths, so a route added tomorrow cannot
/// be born unprotected because nobody remembered to enumerate it.
fn writes(m: &axum::http::Method) -> bool {
    !matches!(*m, axum::http::Method::GET | axum::http::Method::HEAD)
}

fn is_shell(path: &str) -> bool {
    path == "/" || path.starts_with("/assets/") || path == "/favicon.ico"
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
