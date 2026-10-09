//! Bounded, payload-free Watch samples. Database writes are off the relay path.
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default)]
pub struct Sample {
    pub items: i64,
    pub cost: i64,
}

#[derive(Debug)]
pub struct Observation {
    pub id: String,
    pub application: String,
    pub graph: String,
    pub node: String,
    pub session: i64,
    pub started_at: i64,
    pub until: i64,
    pub closed_at: AtomicI64,
    pub lost: AtomicU64,
    samples: Mutex<BTreeMap<i64, Sample>>,
}
impl Observation {
    pub fn new(app: &str, graph: &str, node: &str, watch: &gate_core::Watch) -> Self {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).expect("OS randomness for observation identity");
        Self {
            id: random.iter().map(|b| format!("{b:02x}")).collect(),
            application: app.into(),
            graph: graph.into(),
            node: node.into(),
            session: watch.started_at.unwrap_or(crate::now_ms()),
            started_at: crate::now_ms(),
            until: watch.ends_at().unwrap_or(crate::now_ms()),
            closed_at: AtomicI64::new(0),
            lost: AtomicU64::new(0),
            samples: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn record(&self, now: i64, items: u64, cost: u64) {
        if items == 0 || now >= self.until || self.closed_at.load(Ordering::Relaxed) > 0 {
            return;
        }
        let mut samples = self.samples.lock();
        let bucket = samples.entry(now / 1000 * 1000).or_default();
        bucket.items = bucket
            .items
            .saturating_add(items.min(i64::MAX as u64) as i64);
        bucket.cost = bucket.cost.saturating_add(cost.min(i64::MAX as u64) as i64);
        // At most an hour of distinct seconds, including during a DB outage.
        while samples.len() > 3600 {
            if let Some((_, lost)) = samples.pop_first() {
                self.lost.fetch_add(lost.items as u64, Ordering::Relaxed);
            }
        }
    }
    pub fn through(&self, now: i64) -> i64 {
        let closed = self.closed_at.load(Ordering::Relaxed);
        now.min(self.until)
            .min(if closed > 0 { closed } else { i64::MAX })
            / 1000
            * 1000
    }
    pub fn pending(&self, through: i64) -> Vec<(i64, Sample)> {
        self.samples
            .lock()
            .range(..through)
            .map(|(t, v)| (*t, *v))
            .collect()
    }
    pub fn acknowledge(&self, rows: &[(i64, Sample)]) {
        let mut samples = self.samples.lock();
        for (t, _) in rows {
            samples.remove(t);
        }
    }
    pub fn close(&self) {
        self.closed_at.store(crate::now_ms(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recording_is_bounded_sealed_and_stops_at_watch_end() {
        let watch = gate_core::Watch {
            started_at: Some(0),
            duration_seconds: 7200,
        };
        let rec = Observation::new("a", "g", "n", &watch);
        rec.record(500, 2, 4);
        assert!(rec.pending(0).is_empty());
        let rows = rec.pending(1000);
        assert_eq!(rows[0].1.items, 2);
        rec.acknowledge(&rows);
        for t in 1..=3601 {
            rec.record(t * 1000, 1, 1);
        }
        assert_eq!(rec.pending(7_200_000).len(), 3600);
        assert_eq!(rec.lost.load(Ordering::Relaxed), 1);
        rec.record(7_200_000, 50, 50);
        assert_eq!(rec.pending(7_201_000).len(), 3600);
    }
}
