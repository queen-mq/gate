//! The console is embedded into the binary at compile time, and cargo has no
//! way to know that: `ui/dist` is not a Rust source file, so a rebuilt console
//! with an unchanged server produces a binary that still serves the OLD one.
//!
//! That failure is silent and convincing — the page loads, it is simply the
//! previous version — so the dependency is declared here rather than
//! remembered.
fn main() {
    println!("cargo:rerun-if-changed=../../ui/dist");
}
