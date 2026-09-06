pub mod worker_registry;

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::Duration;

use forge_core::{TaskGraph, TaskId, TaskState};
use forge_protocol::{Frame, Heartbeat, MessageKind, TaskRequest, TaskResult};

#[derive(Debug)]
pub enum CoordinatorError {
    Io(std::io::Error),
    Protocol(String),
    UnexpectedMessage(MessageKind),
    NoWorkers,
    SchedulerStalled,
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Protocol(error) => write!(formatter, "protocol error: {error}"),
            Self::UnexpectedMessage(kind) => write!(formatter, "unexpected worker message: {kind:?}"),
            Self::NoWorkers => write!(formatter, "distributed executor has no workers"),
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
        Self {
            address: address.into(),
            connect_timeout: Duration::from_secs(5),
        }
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    fn connect(&self) -> Result<TcpStream, CoordinatorError> {
        let mut addresses = self.address.to_socket_addrs()?;
        let address = addresses
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "worker address resolved to no endpoints"))?;
        let stream = TcpStream::connect_timeout(&address, self.connect_timeout)?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    pub fn execute(&self, task_id: u64, command: impl Into<String>) -> Result<TaskResult, CoordinatorError> {
        let mut stream = self.connect()?;
        let request = TaskRequest {
            task_id,
            command: command.into(),
        };
        let payload = request
            .encode()
            .map_err(|error| CoordinatorError::Protocol(error.to_string()))?;
        Frame {
            kind: MessageKind::TaskRequest,
            payload,
        }
        .encode(&mut stream)?;
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
        let payload = Heartbeat {
            worker_id: String::new(),
            unix_seconds: 0,
        }
        .encode()
        .map_err(|error| CoordinatorError::Protocol(error.to_string()))?;
        Frame {
            kind: MessageKind::Heartbeat,
            payload,
        }
        .encode(&mut stream)?;
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
    max_attempts: usize,
}

impl DistributedExecutor {
    pub fn new<I, S>(addresses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            workers: addresses.into_iter().map(WorkerClient::new).collect(),
            max_attempts: 1,
        }
    }

    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn execute(&self, graph: &mut TaskGraph) -> Result<Vec<TaskId>, CoordinatorError> {
        if self.workers.is_empty() {
            return Err(CoordinatorError::NoWorkers);
        }

        let mut completed = Vec::new();
        let mut attempts = BTreeMap::<TaskId, usize>::new();
        let mut worker_index = 0usize;

        loop {
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
                    let worker = self.workers[worker_index % self.workers.len()].clone();
                    worker_index += 1;
                    let command = graph
                        .task(task_id)
                        .expect("runnable task must exist")
                        .command
                        .clone();
                    graph.task_mut(task_id).expect("runnable task must exist").state = TaskState::Running;
                    let attempt = attempts.entry(task_id).or_insert(0);
                    *attempt += 1;
                    (task_id, worker, command)
                })
                .collect::<Vec<_>>();

            let handles = assignments
                .into_iter()
                .map(|(task_id, worker, command)| {
                    thread::spawn(move || (task_id, worker.execute(task_id, command)))
                })
                .collect::<Vec<_>>();

            for handle in handles {
                let (task_id, result) = handle
                    .join()
                    .map_err(|_| CoordinatorError::SchedulerStalled)?;
                match result {
                    Ok(result) if result.success => {
                        graph.task_mut(task_id).expect("running task must exist").state = TaskState::Succeeded;
                        completed.push(task_id);
                    }
                    Ok(_) | Err(_) => {
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

    #[test]
    fn client_executes_task_on_remote_worker() {
        let (address, worker_thread) = spawn_worker(Worker::new("test-worker", 1), 1);
        let result = WorkerClient::new(address)
            .execute(42, "echo distributed-forge")
            .unwrap();
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
        let (address, worker_thread) = spawn_worker(Worker::new("graph-worker", 2), 2);
        let mut graph = TaskGraph::default();
        graph.add_task(1, "echo build", Vec::new()).unwrap();
        graph.add_task(2, "echo test", vec![1]).unwrap();

        let executor = DistributedExecutor::new([address]);
        let completed = executor.execute(&mut graph).unwrap();

        assert_eq!(completed, vec![1, 2]);
        assert_eq!(graph.task(1).unwrap().state, TaskState::Succeeded);
        assert_eq!(graph.task(2).unwrap().state, TaskState::Succeeded);
        worker_thread.join().unwrap();
    }
}
