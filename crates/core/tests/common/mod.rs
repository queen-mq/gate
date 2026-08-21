#![allow(dead_code)]

//! The flagship fixture, in the v2 vocabulary.
//!
//! Ported from v1's `airbnb()` and it is still the most valuable fixture in the
//! repository: three traffic classes, each isolated, merging into the one node
//! that holds the egress-IP ceiling; prices first, photos last; the photo node
//! limited per listing at a cardinality no single state document could hold.
//!
//! It must **validate clean**. The one warning it earns is `fanout-multiplies`,
//! which is the design's own mandated notice that a fan-out doubles what the
//! vendor sees — a fixture with a fan-out that warned about nothing would mean
//! the rule was not wired.

use gate_core::GraphDoc;

pub fn airbnb() -> GraphDoc {
    serde_json::from_str(AIRBNB).expect("the flagship fixture must parse")
}

/// Cost ceilings differ from the design document's §3.7 listing, and
/// deliberately: that listing gives `messages` and `photos` an item cost ceiling
/// of 50 against sub-windows that admit 10 and 5, which `cost-fits` refuses —
/// an item that cannot fit a window can never be admitted and would park the
/// head of its partition for ever. The numbers here are the largest that fit.
pub const AIRBNB: &str = r#"
{
  "application": "channel",
  "graph": "airbnb",
  "version": 1,

  "nodes": {
    "prices": {
      "ingress": true,
      "cost": { "path": "payload.rooms", "default": 1, "max": 50 },
      "budgets": [
        { "id": "prices-1s", "count": 100, "timeMs": 1000,
          "confidence": "documented", "source": "https://airbnb.dev/limits", "asOf": "2026-08-01" }
      ]
    },

    "messages": {
      "ingress": { "queue": "channel.airbnb.messages.in", "http": false },
      "cost": { "path": "payload.rooms", "default": 1, "max": 10 },
      "budgets": [
        { "id": "messages-1m", "count": 600, "timeMs": 60000, "subWindows": 60 }
      ]
    },

    "photos": {
      "ingress": true,
      "cost": { "path": "payload.rooms", "default": 1, "max": 5 },
      "budgets": [
        { "id": "photos-1m",   "count": 300, "timeMs": 60000, "subWindows": 60 },
        { "id": "per-listing", "count": 100, "timeMs": 604800000, "subWindows": 1,
          "scopeBy": "payload.listingId", "whenOp": ["photo.delete"] }
      ]
    },

    "ip": {
      "cost": { "path": "payload.rooms", "default": 1, "max": 50 },
      "budgets": [
        { "id": "ip-10s", "count": 1500,   "timeMs": 10000,   "subWindows": 10,
          "sharedKey": "egress-ip" },
        { "id": "ip-1h",  "count": 300000, "timeMs": 3600000, "subWindows": 60,
          "sharedKey": "egress-ip-hour" }
      ],
      "egress": { "queue": "channel.airbnb.out", "group": "channel-workers" }
    },

    "audit": {
      "budgets": [ { "id": "audit-1s", "count": 2000, "timeMs": 1000 } ],
      "egress": "channel.airbnb.audit"
    }
  },

  "paths": [
    { "name": "prices",   "priority": 0, "share": 1.0,  "nodes": ["prices",   "ip"] },
    { "name": "messages", "priority": 1, "share": 0.75, "nodes": ["messages", "ip"] },
    { "name": "photos",   "priority": 2, "share": 0.5,  "nodes": ["photos", ["ip", "audit"]] }
  ]
}
"#;

/// The rrl.js shape, declared: one node, one budget, an ingress queue the
/// application already owns. `examples/rrl.js` in the queen repo is this
/// document's runtime.
pub const RRL: &str = r#"
{
  "application": "rrl",
  "graph": "price-airbnb",
  "version": 1,
  "nodes": {
    "providerx": {
      "ingress": { "queue": "rrl.ingress.price-airbnb" },
      "cost": { "path": "payload.cost", "default": 1, "max": 10 },
      "budgets": [ { "id": "providerx", "count": 100, "timeMs": 1000 } ],
      "egress": "rrl.egress.price-airbnb"
    }
  },
  "paths": [ { "name": "main", "nodes": ["providerx"] } ]
}
"#;

pub fn rrl() -> GraphDoc {
    serde_json::from_str(RRL).expect("the rrl fixture must parse")
}

pub fn rules(problems: &[gate_core::Problem]) -> Vec<&'static str> {
    let mut v: Vec<&'static str> = problems.iter().map(|p| p.rule).collect();
    v.sort_unstable();
    v.dedup();
    v
}
