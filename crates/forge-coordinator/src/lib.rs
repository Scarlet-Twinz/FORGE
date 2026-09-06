use std::fmt;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use forge_protocol::{Frame, MessageKind, TaskRequest, TaskResult};

#[derive(Debug)]
pub enum CoordinatorError {
    Io(std::io::Error),
    Protocol(Box<dyn std::error::Error>),
    UnexpectedMessage(MessageKind),
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Protocol(error) => write!(formatter, "protocol error: {error}"),
            Self::UnexpectedMessage(kind) => write!(formatter, "unexpected worker message: {kind:?}"),
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

    pub fn execute(&self, task_id: u64, command: impl Into<String>) -> Result<TaskResult, CoordinatorError> {
        let mut addresses = self.address.to_socket_addrs()?;
        let address = addresses
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "worker address resolved to no endpoints"))?;
        let mut stream = TcpStream::connect_timeout(&address, self.connect_timeout)?;
        stream.set_nodelay(true)?;

        let request = TaskRequest {
            task_id,
            command: command.into(),
        };
        let payload = request
            .encode()
            .map_err(|error| CoordinatorError::Protocol(Box::new(error)))?;
        Frame {
            kind: MessageKind::TaskRequest,
            payload,
        }
        .encode(&mut stream)?;
        stream.flush()?;

        let response = Frame::decode(&mut stream)
            .map_err(CoordinatorError::Protocol)?;
        if response.kind != MessageKind::TaskResult {
            return Err(CoordinatorError::UnexpectedMessage(response.kind));
        }

        TaskResult::decode(&response.payload)
            .map_err(|error| CoordinatorError::Protocol(Box::new(error)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_worker::{handle_connection, Worker};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn client_executes_task_on_remote_worker() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let worker_thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            handle_connection(&mut stream, &Worker::new("test-worker", 1)).unwrap();
        });

        let result = WorkerClient::new(address.to_string())
            .execute(42, "echo distributed-forge")
            .unwrap();

        assert_eq!(result.task_id, 42);
        assert!(result.success);
        assert!(result.stdout.to_ascii_lowercase().contains("distributed-forge"));
        worker_thread.join().unwrap();
    }
}
