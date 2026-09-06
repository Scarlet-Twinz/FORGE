use std::io::Write;
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use forge_protocol::{Frame, Heartbeat, MessageKind, TaskRequest as WireTaskRequest, TaskResult as WireTaskResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResult {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration: Duration,
}

#[derive(Debug, Clone)]
pub struct Worker {
    pub id: String,
    pub max_concurrency: usize,
}

impl Worker {
    pub fn new(id: impl Into<String>, max_concurrency: usize) -> Self {
        Self {
            id: id.into(),
            max_concurrency: max_concurrency.max(1),
        }
    }

    pub fn execute(&self, command: &str) -> std::io::Result<TaskResult> {
        let started = Instant::now();
        let output = Command::new(default_shell())
            .args(default_shell_args(command))
            .stdin(Stdio::null())
            .output()?;

        Ok(TaskResult {
            success: output.status.success(),
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            duration: started.elapsed(),
        })
    }

    pub fn heartbeat(&self) -> Heartbeat {
        Heartbeat {
            worker_id: self.id.clone(),
            unix_seconds: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

pub fn handle_connection(
    stream: &mut TcpStream,
    worker: &Worker,
) -> Result<(), Box<dyn std::error::Error>> {
    let frame = Frame::decode(stream)?;

    match frame.kind {
        MessageKind::TaskRequest => {
            let request = WireTaskRequest::decode(&frame.payload)?;
            let result = worker.execute(&request.command)?;
            let response = WireTaskResult {
                task_id: request.task_id,
                success: result.success,
                exit_code: result.exit_code,
                stdout: result.stdout,
                stderr: result.stderr,
            };
            let payload = response.encode()?;
            Frame {
                kind: MessageKind::TaskResult,
                payload,
            }
            .encode(stream)?;
            stream.flush()?;
        }
        MessageKind::Heartbeat => {
            let _ = forge_protocol::Heartbeat::decode(&frame.payload)?;
            let payload = worker.heartbeat().encode()?;
            Frame {
                kind: MessageKind::Heartbeat,
                payload,
            }
            .encode(stream)?;
            stream.flush()?;
        }
        _ => return Err("worker received unsupported message kind".into()),
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn default_shell() -> &'static str { "cmd" }

#[cfg(target_os = "windows")]
fn default_shell_args(command: &str) -> [&str; 2] { ["/C", command] }

#[cfg(not(target_os = "windows"))]
fn default_shell() -> &'static str { "sh" }

#[cfg(not(target_os = "windows"))]
fn default_shell_args(command: &str) -> [&str; 2] { ["-c", command] }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_executes_a_task() {
        let worker = Worker::new("local", 2);
        let result = worker.execute("echo forge").unwrap();
        assert!(result.success);
        assert!(result.stdout.to_ascii_lowercase().contains("forge"));
    }

    #[test]
    fn worker_concurrency_is_never_zero() {
        assert_eq!(Worker::new("local", 0).max_concurrency, 1);
    }
}
