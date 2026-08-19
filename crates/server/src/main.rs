//! The binary. Everything it does lives in the library next to it — see
//! `lib.rs` — so a test can boot the same server in-process.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    gate_server::run().await
}
