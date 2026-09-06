use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::{CoordinatorError, WorkerClient};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerHealth {
    Healthy,
    Unhealthy,
}

#[derive(Debug, Clone)]
pub struct WorkerRecord {
    pub address: String,
    pub worker_id: Option<String>,
    pub last_seen: Option<Instant>,
    pub health: WorkerHealth,
}

impl WorkerRecord {
    fn new(address: String) -> Self {
        Self {
            address,
            worker_id: None,
            last_seen: None,
            health: WorkerHealth::Unhealthy,
        }
    }

    pub fn is_healthy(&self, now: Instant, timeout: Duration) -> bool {
        matches!(self.health, WorkerHealth::Healthy)
            && self
                .last_seen
                .is_some_and(|last_seen| now.duration_since(last_seen) <= timeout)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerRegistry {
    workers: BTreeMap<String, WorkerRecord>,
    heartbeat_timeout: Duration,
}

impl WorkerRegistry {
    pub fn new<I, S>(addresses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let workers = addresses
            .into_iter()
            .map(Into::into)
            .map(|address| {
                let key = address.clone();
                (key, WorkerRecord::new(address))
            })
            .collect();

        Self {
            workers,
            heartbeat_timeout: Duration::from_secs(15),
        }
    }

    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn record(&self, address: &str) -> Option<&WorkerRecord> {
        self.workers.get(address)
    }

    pub fn healthy_workers(&self) -> Vec<WorkerClient> {
        let now = Instant::now();
        self.workers
            .values()
            .filter(|record| record.is_healthy(now, self.heartbeat_timeout))
            .map(|record| WorkerClient::new(record.address.clone()))
            .collect()
    }

    pub fn probe(&mut self) -> Vec<(String, Result<String, CoordinatorError>)> {
        let addresses = self.workers.keys().cloned().collect::<Vec<_>>();
        let mut results = Vec::with_capacity(addresses.len());

        for address in addresses {
            let client = WorkerClient::new(&address);
            let result = client.heartbeat().map(|heartbeat| heartbeat.worker_id);
            let now = Instant::now();

            if let Some(record) = self.workers.get_mut(&address) {
                match &result {
                    Ok(worker_id) => {
                        record.worker_id = Some(worker_id.clone());
                        record.last_seen = Some(now);
                        record.health = WorkerHealth::Healthy;
                    }
                    Err(_) => {
                        record.health = WorkerHealth::Unhealthy;
                    }
                }
            }

            results.push((address, result));
        }

        results
    }

    pub fn refresh_health(&mut self) {
        let now = Instant::now();
        for record in self.workers.values_mut() {
            if !record.is_healthy(now, self.heartbeat_timeout) {
                record.health = WorkerHealth::Unhealthy;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_worker::{handle_connection, Worker};
    use std::net::TcpListener;
    use std::thread;

    fn spawn_worker(worker: Worker) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            handle_connection(&mut stream, &worker).unwrap();
        });
        (address, thread)
    }

    #[test]
    fn probe_marks_live_worker_healthy() {
        let (address, worker_thread) = spawn_worker(Worker::new("registry-worker", 1));
        let mut registry = WorkerRegistry::new([address.clone()]);

        let results = registry.probe();
        assert!(results[0].1.is_ok());
        assert_eq!(registry.healthy_workers().len(), 1);
        assert_eq!(registry.record(&address).unwrap().worker_id.as_deref(), Some("registry-worker"));

        worker_thread.join().unwrap();
    }

    #[test]
    fn dead_worker_is_not_healthy() {
        let registry = WorkerRegistry::new(["127.0.0.1:1"]);
        assert!(registry.healthy_workers().is_empty());
    }

    #[test]
    fn stale_worker_becomes_unhealthy() {
        let mut registry = WorkerRegistry::new(["127.0.0.1:1"])
            .with_heartbeat_timeout(Duration::from_millis(1));
        let record = registry.workers.get_mut("127.0.0.1:1").unwrap();
        record.health = WorkerHealth::Healthy;
        record.last_seen = Some(Instant::now() - Duration::from_secs(1));

        registry.refresh_health();
        assert!(registry.healthy_workers().is_empty());
    }
}
