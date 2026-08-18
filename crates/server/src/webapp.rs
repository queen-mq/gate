//! The console, embedded in the binary.
//!
//! Same trick the broker and the relay use: `rust-embed` over the built Vue SPA,
//! so the deployment is one artefact and an operator never has to serve a static
//! bundle alongside it. `ui/dist` is kept in the repo with a placeholder for
//! exactly this reason — a missing folder is a macro error, not a build warning.

use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../../ui/dist/"]
struct Assets;

pub fn router() -> Router<crate::api::Shared> {
    Router::new()
        .route("/", get(index))
        .route("/assets/*path", get(asset))
        .route("/favicon.ico", get(|| async { StatusCode::NO_CONTENT }))
}

async fn index(headers: HeaderMap) -> Response {
    serve("index.html", &headers)
}

async fn asset(Path(path): Path<String>, headers: HeaderMap) -> Response {
    serve(&format!("assets/{path}"), &headers)
}

/// The asset names are fixed — `console.js`, not `console.4f3a1b.js` — because
/// they are compiled into the binary and a hash would make every build a
/// different set of files for no gain. Fixed names and a cache do not mix: a
/// browser holding yesterday's `console.js` renders yesterday's console against
/// today's API and looks perfectly healthy doing it.
///
/// So: `no-cache`, which means revalidate every time and NOT "do not store",
/// plus an ETag off the embedded content hash so the revalidation is a 304 and
/// the bytes only cross the wire when they actually changed.
fn serve(path: &str, req: &HeaderMap) -> Response {
    match Assets::get(path) {
        Some(f) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            let etag = format!("\"{}\"", hex16(&f.metadata.sha256_hash()));
            if req
                .get(header::IF_NONE_MATCH)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.split(',').any(|c| c.trim() == etag))
            {
                return (
                    StatusCode::NOT_MODIFIED,
                    [(header::ETAG, etag.as_str()), (header::CACHE_CONTROL, "no-cache")],
                )
                    .into_response();
            }
            (
                [
                    (header::CONTENT_TYPE, mime.as_ref()),
                    (header::CACHE_CONTROL, "no-cache"),
                    (header::ETAG, etag.as_str()),
                ],
                Body::from(f.data.into_owned()),
            )
                .into_response()
        }
        // The console is a hash-router SPA, so an unknown path is a route it
        // will resolve itself once it boots — not a 404.
        None if !path.starts_with("assets/") => serve("index.html", req),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

/// Sixteen bytes of the hash is plenty for an ETag: it is a cache key, not a
/// signature.
fn hex16(hash: &[u8; 32]) -> String {
    hash[..8].iter().map(|b| format!("{b:02x}")).collect()
}
