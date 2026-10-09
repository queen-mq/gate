use crate::history::History;
use crate::observation::{Observation, Sample};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;

impl History {
    pub async fn flush_observation(&self, rec: &Observation) -> Result<(), String> {
        let now = crate::now_ms();
        let through = rec.through(now);
        let rows = rec.pending(now / 1000 * 1000);
        let mut client = self.pool.get().await.map_err(|e| e.to_string())?;
        let tx = client.transaction().await.map_err(|e| e.to_string())?;
        // Replacement, not addition: retrying an ambiguous commit is idempotent.
        for (second, sample) in &rows {
            tx.execute("INSERT INTO gate.watch_samples VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (recorder,second) DO UPDATE SET items=EXCLUDED.items,cost=EXCLUDED.cost",
                &[&rec.application,&rec.graph,&rec.node,&rec.session,&rec.id,second,&sample.items,&sample.cost])
                .await.map_err(|e| e.to_string())?;
        }
        tx.execute(
            "INSERT INTO gate.watch_recorders VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (recorder) DO UPDATE SET through=EXCLUDED.through,lost=EXCLUDED.lost",
            &[
                &rec.id,
                &rec.application,
                &rec.graph,
                &rec.node,
                &rec.session,
                &rec.started_at,
                &through,
                &(rec.lost.load(Ordering::Relaxed).min(i64::MAX as u64) as i64),
            ],
        )
        .await
        .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        rec.acknowledge(&rows);
        Ok(())
    }

    pub async fn observation_data(
        &self,
        app: &str,
        graph: &str,
        node: &str,
        session: i64,
        since: i64,
        until: i64,
    ) -> Result<(Vec<(i64, Sample)>, Value), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        let coverage = client.query("SELECT started,through,lost FROM gate.watch_recorders WHERE application=$1 AND graph=$2 AND node=$3 AND session=$4 AND through >= started ORDER BY started", &[&app,&graph,&node,&session]).await.map_err(|e| e.to_string())?;
        let spans: Vec<(i64, i64)> = coverage.iter().map(|r| (r.get(0), r.get(1))).collect();
        let start = spans
            .iter()
            .map(|(a, _)| *a)
            .min()
            .unwrap_or(until)
            .max(since)
            .saturating_add(999)
            / 1000
            * 1000;
        let end = spans
            .iter()
            .map(|(_, b)| *b)
            .max()
            .unwrap_or(start)
            .min(until)
            .max(start);
        // Union coverage: any known missing interval is reported, never filled as idle.
        let mut covered_until = start;
        let mut gaps = 0i64;
        for (a, b) in spans {
            let a = a.saturating_add(999) / 1000 * 1000;
            if a > covered_until {
                gaps += (a.min(end) - covered_until).max(0);
            }
            covered_until = covered_until.max(b.min(end));
        }
        gaps += (end - covered_until).max(0);
        let rows = client.query("SELECT second,SUM(items)::bigint,SUM(cost)::bigint FROM gate.watch_samples WHERE application=$1 AND graph=$2 AND node=$3 AND session=$4 AND second >= $5 AND second < $6 GROUP BY second ORDER BY second", &[&app,&graph,&node,&session,&start,&end]).await.map_err(|e| e.to_string())?;
        let lost: i64 = coverage.iter().map(|r| r.get::<_, i64>(2)).sum();
        Ok((
            rows.iter()
                .map(|r| {
                    (
                        r.get(0),
                        Sample {
                            items: r.get(1),
                            cost: r.get(2),
                        },
                    )
                })
                .collect(),
            json!({"since":start,"until":end,"gapSeconds":gaps/1000,"lostItems":lost,"resolutionSeconds":1}),
        ))
    }

    pub async fn snapshot(
        &self,
        app: &str,
        graph: &str,
        node: &str,
        at: i64,
        body: &Value,
    ) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        client.execute("INSERT INTO gate.snapshots VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (application,graph,node,minute) DO UPDATE SET at=EXCLUDED.at,body=EXCLUDED.body WHERE gate.snapshots.at < EXCLUDED.at", &[&app,&graph,&node,&(at/60_000*60_000),&at,body]).await.map_err(|e| e.to_string())?;
        Ok(())
    }
    pub async fn config_event(
        &self,
        doc: &gate_core::GraphDoc,
        previous: Option<&gate_core::GraphDoc>,
    ) -> Result<(), String> {
        if previous == Some(doc) {
            return Ok(());
        }
        let label = match previous {
            None => "Created",
            Some(old) if old.watch.is_some() != doc.watch.is_some() => {
                if doc.watch.is_some() {
                    "Watch started"
                } else {
                    "Limits activated"
                }
            }
            _ => "Configuration updated",
        };
        let body = json!({"label":label,"version":doc.version,"mode":if doc.watch.is_some() {"watch"} else {"enforce"}});
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        client
            .execute(
                "INSERT INTO gate.config_events(application,graph,at,body) VALUES ($1,$2,$3,$4)",
                &[&doc.application, &doc.graph, &crate::now_ms(), &body],
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub async fn insights(&self, app: &str, graph: &str, minutes: i64) -> Result<Value, String> {
        let since = crate::now_ms() - minutes * 60_000;
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        let rows = client.query("SELECT node,minute,at,body FROM gate.snapshots WHERE application=$1 AND graph=$2 AND minute >= $3 ORDER BY minute,node", &[&app,&graph,&since]).await.map_err(|e| e.to_string())?;
        let events = client.query("SELECT at,body FROM gate.config_events WHERE application=$1 AND graph=$2 AND at >= $3 ORDER BY at DESC LIMIT 200", &[&app,&graph,&since]).await.map_err(|e| e.to_string())?;
        Ok(
            json!({"samples":rows.iter().map(|r| json!({"node":r.get::<_,String>(0),"t":r.get::<_,i64>(1),"at":r.get::<_,i64>(2),"value":r.get::<_,Value>(3)})).collect::<Vec<_>>(),"events":events.iter().map(|r| json!({"at":r.get::<_,i64>(0),"change":r.get::<_,Value>(1)})).collect::<Vec<_>>()}),
        )
    }
    pub async fn prune_insights(&self) {
        if let Ok(client) = self.pool.get().await {
            let since = crate::now_ms() - 31 * 86_400_000;
            for table in [
                "watch_samples",
                "watch_recorders",
                "snapshots",
                "config_events",
            ] {
                let column = match table {
                    "watch_samples" => "second",
                    "watch_recorders" => "through",
                    _ => "at",
                };
                let _ = client
                    .execute(
                        &format!("DELETE FROM gate.{table} WHERE {column} < $1"),
                        &[&since],
                    )
                    .await;
            }
        }
    }
}
