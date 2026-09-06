pub mod worker_registry;

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::Duration;

use forge_core::{TaskGraph, TaskId, TaskState};
use forge_protocol::{Frame, Heartbeat, MessageKind, TaskRequest, TaskResult};
use worker_registry::WorkerRegistry;

#[derive(Debug)]
pub enum CoordinatorError {
    Io(std::io::Error),
    Protocol(String),
    UnexpectedMessage(MessageKind),
    NoWorkers,
    NoHealthyWorkers,
    SchedulerStalled,
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Protocol(error) => write!(formatter, "protocol error: {error}"),
            Self::UnexpectedMessage(kind) => write!(formatter, "unexpected worker message: {kind:?}"),
            Self::NoWorkers => write!(formatter, "distributed executor has no workers"),
            Self::NoHealthyWorkers => write!(formatter, "distributed executor has no healthy workers"),
            Self::SchedulerStalled => write!(formatter, "distributed scheduler stalled with pending tasks"),
        }
    }
}

impl std::error::Error for CoordinatorError {}

impl From<std::io::Error> for CoordinatorError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerClient {
    address: String,
    connect_timeout: Duration,
}

impl WorkerClient {
    pub fn new(address: impl Into<String>) -> Self {
        Self { address: address.into(), connect_timeout: Duration::from_secs(5) }
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    fn connect(&self) -> Result<TcpStream, CoordinatorError> {
        let mut addresses = self.address.to_socket_addrs()?;
        let address = addresses.next().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "worker address resolved to no endpoints")
        })?;
        let stream = TcpStream::connect_timeout(&address, self.connect_timeout)?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    pub fn execute(&self, task_id: u64, command: impl Into<String>) -> Result<TaskResult, CoordinatorError> {
        self.execute_with_timeout(task_id, command, None)
    }

    pub fn execute_with_timeout(
        &self,
        task_id: u64,
        command: impl Into<String>,
        timeout: Option<Duration>,
    ) -> Result<TaskResult, CoordinatorError> {
        let mut stream = self.connect()?;
        let request = TaskRequest {
            task_id,
            command: command.into(),
            timeout_ms: timeout.map(|value| value.as_millis().min(u64::MAX as u128) as u64),
        };
        let payload = request
            .encode()
            .map_err(|error| CoordinatorError::Protocol(error.to_string()))?;
        Frame { kind: MessageKind::TaskRequest, payload }.encode(&mut stream)?;
        stream.flush()?;

        let response = Frame::decode(&mut stream)
            .map_err(|error| CoordinatorError::Protocol(error.to_string()))?;
        if response.kind != MessageKind::TaskResult {
            return Err(CoordinatorError::UnexpectedMessage(response.kind));
        }
        TaskResult::decode(&response.payload)
            .map_err(|error| CoordinatorError::Protocol(error.to_string()))
    }

    pub fn heartbeat(&self) -> Result<Heartbeat, CoordinatorError> {
        let mut stream = self.connect()?;
        let payload = Heartbeat { worker_id: String::new(), unix_seconds: 0 }
            .encode()
            .map_err(|error| CoordinatorError::Protocol(error.to_string()))?;
        Frame { kind: MessageKind::Heartbeat, payload }.encode(&mut stream)?;
        stream.flush()?;

        let response = Frame::decode(&mut stream)
            .map_err(|error| CoordinatorError::Protocol(error.to_string()))?;
        if response.kind != MessageKind::Heartbeat {
            return Err(CoordinatorError::UnexpectedMessage(response.kind));
        }
        Heartbeat::decode(&response.payload)
            .map_err(|error| CoordinatorError::Protocol(error.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct DistributedExecutor {
    workers: Vec<WorkerClient>,
    registry: WorkerRegistry,
    max_attempts: usize,
    task_timeout: Option<Duration>,
}

impl DistributedExecutor {
    pub fn new<I, S>(addresses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let workers = addresses.into_iter().map(WorkerClient::new).collect::<Vec<_>>();
        let registry = WorkerRegistry::new(workers.iter().map(|worker| worker.address.clone()));
        Self { workers, registry, max_attempts: 1, task_timeout: None }
    }

    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    pub fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.task_timeout = Some(timeout);
        self
    }

    pub fn without_task_timeout(mut self) -> Self {
        self.task_timeout = None;
        self
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn healthy_worker_count(&self) -> usize {
        self.registry.healthy_workers().len()
    }

    pub fn execute(&mut self, graph: &mut TaskGraph) -> Result<Vec<TaskId>, CoordinatorError> {
        if self.workers.is_empty() {
            return Err(CoordinatorError::NoWorkers);
        }

        let mut completed = Vec::new();
        let mut attempts = BTreeMap::<TaskId, usize>::new();
        let mut worker_index = 0usize;

        self.registry.probe();

        loop {
            self.registry.refresh_health();
            let healthy_workers = self.registry.healthy_workers();
            if healthy_workers.is_empty() {
                return Err(CoordinatorError::NoHealthyWorkers);
            }

            graph.reconcile_blocked();
            let runnable = graph.runnable();

            if runnable.is_empty() {
                if graph.pending().is_empty() {
                    return Ok(completed);
                }
                return Err(CoordinatorError::SchedulerStalled);
            }

            let assignments = runnable
                .into_iter()
                .map(|task_id| {
                    let worker = healthy_workers[worker_index % healthy_workers.len()].clone();
                    worker_index += 1;
                    let command = graph.task(task_id).expect("runnable task must exist").command.clone();
                    graph.task_mut(task_id).expect("runnable task must exist").state = TaskState::Running;
                    let attempt = attempts.entry(task_id).or_insert(0);
                    *attempt += 1;
                    (task_id, worker, command)
                })
                .collect::<Vec<_>>();

            let timeout = self.task_timeout;
            let handles = assignments
                .into_iter()
                .map(|(task_id, worker, command)| {
                    let address = worker.address().to_string();
                    thread::spawn(move || {
                        (task_id, address, worker.execute_with_timeout(task_id, command, timeout))
                    })
                })
                .collect::<Vec<_>>();

            for handle in handles {
                let (task_id, address, result) = handle
                    .join()
                    .map_err(|_| CoordinatorError::SchedulerStalled)?;
                match result {
                    Ok(result) if result.success => {
                        graph.task_mut(task_id).expect("running task must exist").state = TaskState::Succeeded;
                        completed.push(task_id);
                    }
                    Ok(result) if result.timed_out => {
                        let attempt = attempts.get(&task_id).copied().unwrap_or(1);
                        if attempt < self.max_attempts {
                            graph.task_mut(task_id).expect("running task must exist").state = TaskState::Pending;
                        } else {
                            graph.task_mut(task_id).expect("running task must exist").state = TaskState::Failed;
                        }
                    }
                    Ok(_) => {
                        let attempt = attempts.get(&task_id).copied().unwrap_or(1);
                        if attempt < self.max_attempts {
                            graph.task_mut(task_id).expect("running task must exist").state = TaskState::Pending;
                        } else {
                            graph.task_mut(task_id).expect("running task must exist").state = TaskState::Failed;
                        }
                    }
                    Err(_) => {
                        self.registry.mark_unhealthy(&address);
                        let attempt = attempts.get(&task_id).copied().unwrap_or(1);
                        if attempt < self.max_attempts {
                            graph.task_mut(task_id).expect("running task must exist").state = TaskState::Pending;
                        } else {
                            graph.task_mut(task_id).expect("running task must exist").state = TaskState::Failed;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_worker::{handle_connection, Worker};
    use std::io::Read;
    use std::net::TcpListener;

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

    fn spawn_dropping_worker(listener: TcpListener) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            handle_connection(&mut stream, &Worker::new("dropping-worker", 1)).unwrap();
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 1024];
            let _ = stream.read(&mut buffer);
            drop(stream);
        })
    }

    fn long_running_command() -> &'static str {
        if cfg!(target_os = "windows") { "ping 127.0.0.1 -n 4 > nul" } else { "sleep 2" }
    }

    #[test]
    fn client_executes_task_on_remote_worker() {
        let (address, worker_thread) = spawn_worker(Worker::new("test-worker", 1), 1);
        let result = WorkerClient::new(address).execute(42, "echo distributed-forge").unwrap();
        assert_eq!(result.task_id, 42);
        assert!(result.success);
        assert!(result.stdout.to_ascii_lowercase().contains("distributed-forge"));
        worker_thread.join().unwrap();
    }

    #[test]
    fn client_reads_worker_heartbeat() {
        let (address, worker_thread) = spawn_worker(Worker::new("heartbeat-worker", 1), 1);
        let heartbeat = WorkerClient::new(address).heartbeat().unwrap();
        assert_eq!(heartbeat.worker_id, "heartbeat-worker");
        assert!(heartbeat.unix_seconds > 0);
        worker_thread.join().unwrap();
    }

    #[test]
    fn distributed_executor_runs_dependency_order() {
        let (address, worker_thread) = spawn_worker(Worker::new("graph-worker", 2), 3);
        let mut graph = TaskGraph::default();
        graph.add_task(1, "echo build", Vec::new()).unwrap();
        graph.add_task(2, "echo test", vec![1]).unwrap();
        let mut executor = DistributedExecutor::new([address]);
        let completed = executor.execute(&mut graph).unwrap();
        assert_eq!(completed, vec![1, 2]);
        assert_eq!(executor.healthy_worker_count(), 1);
        assert_eq!(graph.task(1).unwrap().state, TaskState::Succeeded);
        assert_eq!(graph.task(2).unwrap().state, TaskState::Succeeded);
        worker_thread.join().unwrap();
    }

    #[test]
    fn distributed_executor_rejects_only_unhealthy_workers() {
        let mut graph = TaskGraph::default();
        graph.add_task(1, "echo never-runs", Vec::new()).unwrap();
        let mut executor = DistributedExecutor::new(["127.0.0.1:1"]);
        let error = executor.execute(&mut graph).unwrap_err();
        assert!(matches!(error, CoordinatorError::NoHealthyWorkers));
        assert_eq!(graph.task(1).unwrap().state, TaskState::Pending);
    }

    #[test]
    fn command_failure_does_not_mark_worker_unhealthy() {
        let (address, worker_thread) = spawn_worker(Worker::new("command-failure-worker", 1), 2);
        let mut graph = TaskGraph::default();
        graph.add_task(1, "exit /B 1", Vec::new()).unwrap();
        let mut executor = DistributedExecutor::new([address.clone()]);
        let completed = executor.execute(&mut graph).unwrap();
        assert!(completed.is_empty());
        assert_eq!(graph.task(1).unwrap().state, TaskState::Failed);
        assert_eq!(executor.healthy_worker_count(), 1);
        worker_thread.join().unwrap();
    }

    #[test]
    fn worker_failure_retries_task_on_another_healthy_worker() {
        let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
        let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
        let address_a = listener_a.local_addr().unwrap().to_string();
        let address_b = listener_b.local_addr().unwrap().to_string();
        let (dead_listener, healthy_listener, dead_address, healthy_address) = if address_a < address_b {
            (listener_a, listener_b, address_a, address_b)
        } else {
            (listener_b, listener_a, address_b, address_a)
        };
        let dead_thread = spawn_dropping_worker(dead_listener);
        let healthy_thread = thread::spawn(move || {
            let (mut stream, _) = healthy_listener.accept().unwrap();
            handle_connection(&mut stream, &Worker::new("healthy-worker", 1)).unwrap();
            let (mut stream, _) = healthy_listener.accept().unwrap();
            handle_connection(&mut stream, &Worker::new("healthy-worker", 1)).unwrap();
        });
        let mut graph = TaskGraph::default();
        graph.add_task(1, "echo recovered", Vec::new()).unwrap();
        let mut executor = DistributedExecutor::new([dead_address, healthy_address]).with_max_attempts(2);
        let completed = executor.execute(&mut graph).unwrap();
        assert_eq!(completed, vec![1]);
        assert_eq!(graph.task(1).unwrap().state, TaskState::Succeeded);
        assert_eq!(executor.healthy_worker_count(), 1);
        dead_thread.join().unwrap();
        healthy_thread.join().unwrap();
    }

    #[test]
    fn task_timeout_marks_task_failed_but_keeps_worker_healthy() {
        let (address, worker_thread) = spawn_worker(Worker::new("timeout-worker", 1), 2);
        let mut graph = TaskGraph::default();
        graph.add_task(1, long_running_command(), Vec::new()).unwrap();
        let mut executor = DistributedExecutor::new([address]).with_task_timeout(Duration::from_millis(100));
        let completed = executor.execute(&mut graph).unwrap();
        assert!(completed.is_empty());
        assert_eq!(graph.task(1).unwrap().state, TaskState::Failed);
        assert_eq!(executor.healthy_worker_count(), 1);
        worker_thread.join().unwrap();
    }

    #[test]
    fn task_timeout_retries_on_another_healthy_worker() {
        let marker = format!("forge-timeout-retry-{}.tmp", std::process::id());
        let marker_for_command = marker.clone();
        let command = if cfg!(target_os = "windows") {
            format!("if exist {marker_for_command} (echo recovered) else (echo marker>{marker_for_command} & ping 127.0.0.1 -n 4 > nul)")
        } else {
            format!("if [ -f {marker_for_command} ]; then echo recovered; else touch {marker_for_command}; sleep 2; fi")
        };

        let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
        let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
        let address_a = listener_a.local_addr().unwrap().to_string();
        let address_b = listener_b.local_addr().unwrap().to_string();
        let (first_listener, second_listener, first_address, second_address) = if address_a < address_b {
            (listener_a, listener_b, address_a, address_b)
        } else {
            (listener_b, listener_a, address_b, address_a)
        };

        let first_thread = thread::spawn(move || {
            let (mut stream, _) = first_listener.accept().unwrap();
            handle_connection(&mut stream, &Worker::new("timeout-first", 1)).unwrap();
            let (mut stream, _) = first_listener.accept().unwrap();
            handle_connection(&mut stream, &Worker::new("timeout-first", 1)).unwrap();
        });
        let second_thread = thread::spawn(move || {
            let (mut stream, _) = second_listener.accept().unwrap();
            handle_connection(&mut stream, &Worker::new("timeout-second", 1)).unwrap();
        });

        let mut graph = TaskGraph::default();
        graph.add_task(1, command, Vec::new()).unwrap();
        let mut executor = DistributedExecutor::new([first_address, second_address])
            .with_max_attempts(2)
            .with_task_timeout(Duration::from_millis(100));
        let completed = executor.execute(&mut graph).unwrap();

        assert_eq!(completed, vec![1]);
        assert_eq!(graph.task(1).unwrap().state, TaskState::Succeeded);
        assert_eq!(executor.healthy_worker_count(), 2);
        first_thread.join().unwrap();
        second_thread.join().unwrap();
        let _ = std::fs::remove_file(marker);
    }
}
