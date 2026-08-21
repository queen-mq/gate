//! The v1 declaration, kept only so it can be read and mapped.
//!
//! Nothing here has behaviour. It is the shape of the documents already sitting
//! in the store and already being `PUT` by callers who wrote them last year, and
//! it exists so `migrate::from_v1` can answer **200 with warnings naming every
//! field that was mapped or ignored** — never a silent success, and never a 422
//! for having been written before the rewrite.
//!
//! Every type is `#[deprecated]`. That is the point: a warning at every use site
//! is exactly the reminder wanted, and the two modules that legitimately use
//! them say so with a local `allow`.

#![allow(deprecated)]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::doc::{default_application, Confidence};

#[deprecated(note = "v1 declaration; read and mapped by migrate::from_v1, never written")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TargetSpec {
    #[serde(default = "default_application")]
    pub application: String,
    #[serde(default)]
    pub name: String,
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<String>,
    pub budgets: Vec<Budget>,
    #[serde(default)]
    pub lanes: Vec<Lane>,
    pub cost: Cost,
    #[serde(default)]
    pub pacing: Pacing,
    #[serde(default)]
    pub admitted: Admitted,
    #[serde(default, rename = "shardBy", skip_serializing_if = "Option::is_none")]
    pub shard_by: Option<Dim>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<u32>,
}

#[deprecated(note = "v1 declaration; read and mapped by migrate::from_v1, never written")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    pub id: String,
    pub cap: f64,
    #[serde(rename = "periodSeconds")]
    pub period_seconds: i64,
    pub alignment: Alignment,
    #[serde(default, rename = "match", skip_serializing_if = "Option::is_none")]
    pub matcher: Option<Match>,
    #[serde(default)]
    pub scope: Vec<Dim>,
    #[serde(default, rename = "maxKeys", skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<u64>,
    #[serde(default)]
    pub store: Store,
    pub confidence: Confidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, rename = "asOf", skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Alignment {
    Rolling,
    Calendar,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Store {
    #[default]
    Gate,
    Kv,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dim {
    Host,
    Entity,
    Account,
    Connection,
    Tenant,
}

impl Dim {
    pub fn as_str(&self) -> &'static str {
        match self {
            Dim::Host => "host",
            Dim::Entity => "entity",
            Dim::Account => "account",
            Dim::Connection => "connection",
            Dim::Tenant => "tenant",
        }
    }
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Match {
    pub op: Vec<String>,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Lane {
    pub name: String,
    pub cap: CapPolicy,
    #[serde(default = "eight")]
    pub concurrency: u32,
    #[serde(default)]
    pub floor: f64,
    #[serde(default)]
    pub default: bool,
}

fn eight() -> u32 {
    8
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, PartialEq)]
pub enum CapPolicy {
    Ceiling,
    CeilingMinusMeasured,
    Absolute(f64),
    Share(f64),
}

impl Serialize for CapPolicy {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&match self {
            CapPolicy::Ceiling => "ceiling".into(),
            CapPolicy::CeilingMinusMeasured => "ceiling-minus-measured".into(),
            CapPolicy::Absolute(n) => format!("absolute:{n}"),
            CapPolicy::Share(f) => format!("share:{f}"),
        })
    }
}

impl<'de> Deserialize<'de> for CapPolicy {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let s = String::deserialize(d)?;
        match s.as_str() {
            "ceiling" => Ok(CapPolicy::Ceiling),
            "ceiling-minus-measured" => Ok(CapPolicy::CeilingMinusMeasured),
            other => {
                let (kind, val) = other
                    .split_once(':')
                    .ok_or_else(|| D::Error::custom(format!("bad cap policy: {other}")))?;
                let n: f64 = val
                    .parse()
                    .map_err(|_| D::Error::custom(format!("bad cap value: {val}")))?;
                match kind {
                    "absolute" => Ok(CapPolicy::Absolute(n)),
                    "share" => Ok(CapPolicy::Share(n)),
                    _ => Err(D::Error::custom(format!("bad cap policy: {other}"))),
                }
            }
        }
    }
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Cost {
    pub field: String,
    pub default: f64,
    pub max: f64,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Pacing {
    #[serde(rename = "leaseSeconds")]
    pub lease_seconds: i64,
    pub batch: u32,
}

impl Default for Pacing {
    fn default() -> Self {
        Self {
            lease_seconds: 1,
            batch: 200,
        }
    }
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Admitted {
    #[serde(rename = "partitionBy")]
    pub partition_by: PartitionBy,
    pub partitions: u32,
}

impl Default for Admitted {
    fn default() -> Self {
        Self {
            partition_by: PartitionBy::Connection,
            partitions: 64,
        }
    }
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionBy {
    Connection,
    Entity,
    None,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphSpec {
    #[serde(default = "default_application")]
    pub application: String,
    #[serde(default)]
    pub name: String,
    pub version: u32,
    pub nodes: BTreeMap<String, Node>,
    #[serde(default)]
    pub edges: Vec<Edge>,
    #[serde(default)]
    pub consume: Vec<String>,
    #[serde(default)]
    pub breach: Vec<BreachRule>,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Node {
    #[serde(default)]
    pub entry: bool,
    #[serde(default)]
    pub budgets: Vec<Budget>,
    pub cost: Cost,
    #[serde(default)]
    pub lanes: Vec<Lane>,
    #[serde(default)]
    pub pacing: Pacing,
    #[serde(default)]
    pub admitted: Admitted,
    #[serde(default, rename = "shardBy", skip_serializing_if = "Option::is_none")]
    pub shard_by: Option<Dim>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<String>,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub priority: u32,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BreachRule {
    pub when: BreachWhen,
    #[serde(rename = "retryTo")]
    pub retry_to: String,
    #[serde(rename = "maxAttempts")]
    pub max_attempts: u32,
}

#[deprecated(note = "v1 declaration")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BreachWhen {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}
