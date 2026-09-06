use std::io;
use std::path::Path;
use std::time::Duration;

use forge_coordinator::{CoordinatorError, DistributedExecutor};
use forge_core::{TaskGraph, TaskId, TaskState};
use forge_store::{sha256_hex, ArtifactStore, CacheStore};

#[derive(Debug)]
pub struct CachedDistributedExecutor {
    executor: DistributedExecutor,
    artifacts: ArtifactStore,
    cache: CacheStore,
    task_timeout: Option<Duration>,
}

impl CachedDistributedExecutor {
    pub fn open<I, S>(addresses: I, root: impl AsRef<Path>) -> io::Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let root = root.as_ref();
        let artifacts = ArtifactStore::open(root.join("artifacts"))?;
        let cache = CacheStore::open(root.join("cache.db"))?;
        Ok(Self {
            executor: DistributedExecutor::new(addresses),
            artifacts,
            cache,
            task_timeout: None,
        })
    }

    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.executor = self.executor.with_max_attempts(max_attempts);
        self
    }

    pub fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.task_timeout = Some(timeout);
        self.executor = self.executor.with_task_timeout(timeout);
        self
    }

    pub fn worker_count(&self) -> usize {
        self.executor.worker_count()
    }

    pub fn healthy_worker_count(&self) -> usize {
        self.executor.healthy_worker_count()
    }

    pub fn execute(&mut self, graph: &mut TaskGraph) -> Result<Vec<TaskId>, CoordinatorError> {
        let pending_before = graph.pending();
        let mut cached = Vec::new();

        for task_id in &pending_before {
            let task = graph.task(*task_id).expect("pending task must exist");
            let key = cache_key(&task.command, self.task_timeout);
            let hit = self
                .cache
                .lookup(&key)
                .and_then(|entry| self.artifacts.get(&entry.artifact_hash).ok().flatten())
                .is_some();

            if hit {
                graph.task_mut(*task_id).expect("pending task must exist").state = TaskState::Succeeded;
                cached.push(*task_id);
            }
        }

        if graph.pending().is_empty() {
            return Ok(cached);
        }

        let completed = self.executor.execute(graph)?;

        for task_id in pending_before {
            let Some(task) = graph.task(task_id) else { continue };
            if task.state != TaskState::Succeeded || cached.contains(&task_id) {
                continue;
            }

            let key = cache_key(&task.command, self.task_timeout);
            let artifact = self
                .artifacts
                .put(task.command.as_bytes())
                .map_err(CoordinatorError::Io)?;
            self.cache
                .insert(key, artifact.hash)
                .map_err(CoordinatorError::Io)?;
        }

        let mut all_completed = cached;
        all_completed.extend(completed);
        all_completed.sort_unstable();
        all_completed.dedup();
        Ok(all_completed)
    }
}

pub fn cache_key(command: &str, timeout: Option<Duration>) -> String {
    let timeout_ms = timeout
        .map(|value| value.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0);
    let material = format!("v1\n{timeout_ms}\n{command}");
    format!("task-v1:{}", sha256_hex(material.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_worker::{handle_connection, Worker};
    use std::net::TcpListener;
    use std::thread;

    fn spawn_worker(worker: Worker, connections: usize) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let thread = thread::spawn(move || {
            for _ in 0..connections {
                let (mut stream, _) = listener.accept().unwrap();
                handle_connection(&mut stream, &worker).unwrap();
            }
        });
        (address, thread)
    }

    #[test]
    fn cache_key_changes_when_command_changes() {
        assert_ne!(cache_key("echo one", None), cache_key("echo two", None));
    }

    #[test]
    fn cache_key_changes_when_timeout_changes() {
        assert_ne!(
            cache_key("echo one", Some(Duration::from_millis(100))),
            cache_key("echo one", Some(Duration::from_millis(200)))
        );
    }

    #[test]
    fn cached_execution_skips_worker_on_second_run() {
        let root = std::env::temp_dir().join(format!("forge-cache-exec-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let (address, worker_thread) = spawn_worker(Worker::new("cache-worker", 1), 1);

        let mut first_graph = TaskGraph::default();
        first_graph.add_task(1, "echo cache-me", Vec::new()).unwrap();
        let mut first = CachedDistributedExecutor::open([address], &root).unwrap();
        assert_eq!(first.execute(&mut first_graph).unwrap(), vec![1]);
        assert_eq!(first_graph.task(1).unwrap().state, TaskState::Succeeded);
        worker_thread.join().unwrap();

        let mut second_graph = TaskGraph::default();
        second_graph.add_task(1, "echo cache-me", Vec::new()).unwrap();
        let mut second = CachedDistributedExecutor::open(["127.0.0.1:1"], &root).unwrap();
        assert_eq!(second.execute(&mut second_graph).unwrap(), vec![1]);
        assert_eq!(second_graph.task(1).unwrap().state, TaskState::Succeeded);

        let _ = std::fs::remove_dir_all(root);
    }
}
