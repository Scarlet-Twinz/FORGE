use std::io::Write;
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use forge_protocol::{Frame, Heartbeat, MessageKind, TaskRequest as WireTaskRequest, TaskResult as WireTaskResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResult {
    pub success: bool,
    pub timed_out: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration: Duration,
}

#[derive(Debug)]
struct ConcurrencyGate {
    state: Mutex<usize>,
    changed: Condvar,
    limit: usize,
}

impl ConcurrencyGate {
    fn new(limit: usize) -> Self {
        Self { state: Mutex::new(0), changed: Condvar::new(), limit }
    }

    fn acquire(self: &Arc<Self>) -> ConcurrencyPermit {
        let mut active = self.state.lock().expect("worker concurrency mutex poisoned");
        while *active >= self.limit {
            active = self.changed.wait(active).expect("worker concurrency mutex poisoned");
        }
        *active += 1;
        ConcurrencyPermit { gate: Arc::clone(self) }
    }
}

#[derive(Debug)]
struct ConcurrencyPermit {
    gate: Arc<ConcurrencyGate>,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        let mut active = self.gate.state.lock().expect("worker concurrency mutex poisoned");
        *active -= 1;
        self.gate.changed.notify_one();
    }
}

#[derive(Debug, Clone)]
pub struct Worker {
    pub id: String,
    pub max_concurrency: usize,
    gate: Arc<ConcurrencyGate>,
}

impl Worker {
    pub fn new(id: impl Into<String>, max_concurrency: usize) -> Self {
        let max_concurrency = max_concurrency.max(1);
        Self { id: id.into(), max_concurrency, gate: Arc::new(ConcurrencyGate::new(max_concurrency)) }
    }

    pub fn execute(&self, command: &str) -> std::io::Result<TaskResult> {
        self.execute_with_timeout(command, None)
    }

    pub fn execute_with_timeout(&self, command: &str, timeout: Option<Duration>) -> std::io::Result<TaskResult> {
        let _permit = self.gate.acquire();
        let started = Instant::now();
        let mut child = Command::new(default_shell())
            .args(default_shell_args(command))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let timed_out = if let Some(timeout) = timeout {
            loop {
                if child.try_wait()?.is_some() {
                    break false;
                }
                if started.elapsed() >= timeout {
                    child.kill()?;
                    child.wait()?;
                    break true;
                }
                thread::sleep(Duration::from_millis(10));
            }
        } else {
            false
        };

        let output = child.wait_with_output()?;
        let success = !timed_out && output.status.success();
        let stderr = if timed_out {
            let existing = String::from_utf8_lossy(&output.stderr);
            if existing.is_empty() { "task timed out".to_string() } else { format!("task timed out: {existing}") }
        } else {
            String::from_utf8_lossy(&output.stderr).into_owned()
        };

        Ok(TaskResult {
            success,
            timed_out,
            exit_code: if timed_out { None } else { output.status.code() },
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr,
            duration: started.elapsed(),
        })
    }

    pub fn heartbeat(&self) -> Heartbeat {
        Heartbeat {
            worker_id: self.id.clone(),
            unix_seconds: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
        }
    }
}

pub fn handle_connection(stream: &mut TcpStream, worker: &Worker) -> Result<(), Box<dyn std::error::Error>> {
    let frame = Frame::decode(stream)?;

    match frame.kind {
        MessageKind::TaskRequest => {
            let request = WireTaskRequest::decode(&frame.payload)?;
            let timeout = request.timeout_ms.map(Duration::from_millis);
            let result = worker.execute_with_timeout(&request.command, timeout)?;
            let response = WireTaskResult {
                task_id: request.task_id,
                success: result.success,
                timed_out: result.timed_out,
                exit_code: result.exit_code,
                stdout: result.stdout,
                stderr: result.stderr,
            };
            let payload = response.encode()?;
            Frame { kind: MessageKind::TaskResult, payload }.encode(stream)?;
            stream.flush()?;
        }
        MessageKind::Heartbeat => {
            let _ = forge_protocol::Heartbeat::decode(&frame.payload)?;
            let payload = worker.heartbeat().encode()?;
            Frame { kind: MessageKind::Heartbeat, payload }.encode(stream)?;
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
        assert!(!result.timed_out);
        assert!(result.stdout.to_ascii_lowercase().contains("forge"));
    }

    #[test]
    fn worker_concurrency_is_never_zero() {
        assert_eq!(Worker::new("local", 0).max_concurrency, 1);
    }

    #[test]
    fn cloned_workers_share_the_same_concurrency_gate() {
        let worker = Worker::new("local", 3);
        let clone = worker.clone();
        assert!(Arc::ptr_eq(&worker.gate, &clone.gate));
        assert_eq!(worker.gate.limit, 3);
    }

    #[test]
    fn worker_times_out_and_terminates_task() {
        let worker = Worker::new("local", 1);
        let command = if cfg!(target_os = "windows") {
            "ping 127.0.0.1 -n 4 > nul"
        } else {
            "sleep 2"
        };
        let result = worker.execute_with_timeout(command, Some(Duration::from_millis(100))).unwrap();
        assert!(!result.success);
        assert!(result.timed_out);
        assert!(result.duration < Duration::from_secs(1));
        assert!(result.stderr.to_ascii_lowercase().contains("timed out"));
    }

    #[test]
    fn worker_completes_fast_task_before_timeout() {
        let worker = Worker::new("local", 1);
        let result = worker.execute_with_timeout("echo fast", Some(Duration::from_secs(2))).unwrap();
        assert!(result.success);
        assert!(!result.timed_out);
    }
}
