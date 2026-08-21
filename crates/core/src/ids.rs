//! Transaction ids, derived.
//!
//! RFC 4122 §4.3 — a version 5 (SHA-1, name-based) UUID — written out here
//! rather than taken from a uuid crate, for the same reason v1 wrote FNV-1a out
//! by hand: **the number has to be stable across releases and reproducible by an
//! operator with a shell.** A hasher chosen by a dependency's default is
//! neither. `client-rust/src/uuid.rs` offers v7 only, so there is nothing in the
//! queen client to borrow.
//!
//! Determinism is the whole point, in both directions:
//!
//! * **deterministic**, so a redelivered relay computes the same id and the
//!   broker's dedup refuses the second push. That is the exactly-once mechanism,
//!   and it is the only one — nothing in Gate keeps a table of what it has
//!   forwarded.
//! * **branch-unique**, so a fan-out's two branches do not carry the same id.
//!   It matters when they later converge on one queue, where dedup would
//!   silently collapse one of them.
//!
//! Reproducing one by hand:
//!
//! ```text
//! printf '%s' "$(printf 6ba7b8149dad11d180b400c04fd430c8 | xxd -r -p)PARENT\x1flabel" \
//!   | shasum -a 1
//! ```
//!
//! — the first sixteen bytes of that digest, with the version and variant bits
//! set, formatted 8-4-4-4-12.

use sha1::{Digest, Sha1};

/// The namespace every Gate-derived id hangs off. A fixed constant, baked in:
/// changing it changes every id this build computes, which is a dedup reset for
/// every graph at once.
///
/// This is RFC 4122's own `NameSpace_X500` (`…b814`; `NameSpace_DNS` is
/// `…b810`). Using a well-known constant rather than a private one is
/// deliberate — an operator checking a value by hand can find the sixteen bytes
/// written down somewhere other than this file.
pub const NS_GATE: [u8; 16] = [
    0x6b, 0xa7, 0xb8, 0x14, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
];

/// The separator between the parent id and the branch label.
///
/// ASCII 31, unit separator: it cannot appear in a uuid and cannot appear in a
/// node or path name (both are `[a-z0-9-]`), so `derive(a, "b/c")` and
/// `derive("a\u{1f}b", "c")` cannot collide by concatenation.
const SEP: u8 = 0x1f;

/// `uuid_v5(NS_GATE, parent + US + label)`, formatted.
pub fn derive(parent: &str, label: &str) -> String {
    let mut h = Sha1::new();
    h.update(NS_GATE);
    h.update(parent.as_bytes());
    h.update([SEP]);
    h.update(label.as_bytes());
    let d = h.finalize();

    let mut b = [0u8; 16];
    b.copy_from_slice(&d[..16]);
    // Version 5 in the high nibble of byte 6, RFC 4122 variant in the top two
    // bits of byte 8. Everything else is digest.
    b[6] = (b[6] & 0x0f) | 0x50;
    b[8] = (b[8] & 0x3f) | 0x80;
    format(&b)
}

fn format(b: &[u8; 16]) -> String {
    let hex = |r: &[u8]| -> String { r.iter().map(|x| format!("{x:02x}")).collect() };
    format!(
        "{}-{}-{}-{}-{}",
        hex(&b[0..4]),
        hex(&b[4..6]),
        hex(&b[6..8]),
        hex(&b[8..10]),
        hex(&b[10..16])
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The value is the API. If this changes, every graph in the fleet re-founds
    /// its dedup and a redelivered relay forwards its batch a second time.
    #[test]
    fn derivation_is_stable() {
        assert_eq!(
            derive("9a1f0f1c-0000-4000-8000-000000000001", "photos/ip"),
            derive("9a1f0f1c-0000-4000-8000-000000000001", "photos/ip"),
        );
    }

    #[test]
    fn a_derived_id_is_a_v5_uuid() {
        let id = derive("parent", "label");
        assert_eq!(id.len(), 36);
        // Version nibble and variant bits, at the offsets the format puts them.
        assert_eq!(&id[14..15], "5", "version 5: {id}");
        assert!(
            matches!(&id[19..20], "8" | "9" | "a" | "b"),
            "RFC 4122 variant: {id}"
        );
    }

    /// The reason the label exists at all: two branches of one fan-out must not
    /// carry one id, or a later fan-in dedups one of them away.
    #[test]
    fn branches_of_one_parent_differ() {
        let a = derive("p", "photos/ip");
        let b = derive("p", "photos/audit");
        assert_ne!(a, b);
    }

    /// And the separator earns its place: without it, `("a", "bc")` and
    /// `("ab", "c")` would be one id.
    #[test]
    fn the_separator_stops_concatenation_collisions() {
        assert_ne!(derive("a", "bc"), derive("ab", "c"));
    }
}
