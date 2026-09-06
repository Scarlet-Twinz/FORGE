use std::fmt::Write as _;
use std::time::Instant;

use forge_coordinator::{CoordinatorError, DistributedExecutor};
use forge_core::{TaskGraph, TaskId, TaskState};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub executions_total: u64,
    pub tasks_succeeded_total: u64,
    pub tasks_failed_total: u64,
    pub tasks_blocked_total: u64,
    pub execution_duration_ms_total: u64,
    pub execution_duration_ms_last: u64,
    pub workers_configured: usize,
    pub healthy_workers_last: usize,
}

impl MetricsSnapshot {
    pub fn render_prometheus(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "forge_executions_total {}", self.executions_total);
        let _ = writeln!(output, "forge_tasks_succeeded_total {}", self.tasks_succeeded_total);
        let _ = writeln!(output, "forge_tasks_failed_total {}", self.tasks_failed_total);
        let _ = writeln!(output, "forge_tasks_blocked_total {}", self.tasks_blocked_total);
        let _ = writeln!(
            output,
            "forge_execution_duration_ms_total {}",
            self.execution_duration_ms_total
        );
        let _ = writeln!(
            output,
            "forge_execution_duration_ms_last {}",
            self.execution_duration_ms_last
        );
        let _ = writeln!(output, "forge_workers_configured {}", self.workers_configured);
        let _ = writeln!(output, "forge_workers_healthy {}", self.healthy_workers_last);
        output
    }
}

#[derive(Debug)]
pub struct MeteredDistributedExecutor {
    executor: DistributedExecutor,
    metrics: MetricsSnapshot,
}

impl MeteredDistributedExecutor {
    pub fn new<I, S>(addresses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let executor = DistributedExecutor::new(addresses);
        let workers_configured = executor.worker_count();
        Self {
            executor,
            metrics: MetricsSnapshot {
                workers_configured,
                ..MetricsSnapshot::default()
            },
        }
    }

    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.executor = self.executor.with_max_attempts(max_attempts);
        self
    }

    pub fn with_task_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.executor = self.executor.with_task_timeout(timeout);
        self
    }

    pub fn worker_count(&self) -> usize {
        self.executor.worker_count()
    }

    pub fn healthy_worker_count(&self) -> usize {
        self.executor.healthy_worker_count()
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics
    }

    pub fn execute(&mut self, graph: &mut TaskGraph) -> Result<Vec<TaskId>, CoordinatorError> {
        let before = graph
            .task_ids()
            .filter_map(|task_id| graph.task(task_id).map(|task| (task_id, task.state)))
            .collect::<Vec<_>>();
        let started = Instant::now();
        let result = self.executor.execute(graph);
        let elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;

        self.metrics.executions_total = self.metrics.executions_total.saturating_add(1);
        self.metrics.execution_duration_ms_total = self
            .metrics
            .execution_duration_ms_total
            .saturating_add(elapsed_ms);
        self.metrics.execution_duration_ms_last = elapsed_ms;
        self.metrics.healthy_workers_last = self.executor.healthy_worker_count();

        for (task_id, previous) in before {
            let Some(current) = graph.task(task_id).map(|task| task.state) else {
                continue;
            };
            if previous == current {
                continue;
            }
            match current {
                TaskState::Succeeded => {
                    self.metrics.tasks_succeeded_total = self.metrics.tasks_succeeded_total.saturating_add(1);
                }
                TaskState::Failed => {
                    self.metrics.tasks_failed_total = self.metrics.tasks_failed_total.saturating_add(1);
                }
                TaskState::Blocked => {
                    self.metrics.tasks_blocked_total = self.metrics.tasks_blocked_total.saturating_add(1);
                }
                _ => {}
            }
        }

        result
    }
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
    fn metrics_record_successful_execution() {
        let (address, worker_thread) = spawn_worker(Worker::new("metrics-worker", 1), 2);
        let mut graph = TaskGraph::default();
        graph.add_task(1, "echo metrics", Vec::new()).unwrap();

        let mut executor = MeteredDistributedExecutor::new([address]);
        assert_eq!(executor.execute(&mut graph).unwrap(), vec![1]);
        worker_thread.join().unwrap();

        let metrics = executor.metrics();
        assert_eq!(metrics.executions_total, 1);
        assert_eq!(metrics.tasks_succeeded_total, 1);
        assert_eq!(metrics.tasks_failed_total, 0);
        assert_eq!(metrics.tasks_blocked_total, 0);
        assert_eq!(metrics.workers_configured, 1);
        assert_eq!(metrics.healthy_workers_last, 1);
        assert!(metrics.execution_duration_ms_last < 10_000);
    }

    #[test]
    fn metrics_record_failure_without_marking_worker_unhealthy() {
        let (address, worker_thread) = spawn_worker(Worker::new("metrics-failure", 1), 2);
        let mut graph = TaskGraph::default();
        graph.add_task(1, "exit /B 1", Vec::new()).unwrap();

        let mut executor = MeteredDistributedExecutor::new([address]);
        assert!(executor.execute(&mut graph).unwrap().is_empty());
        worker_thread.join().unwrap();

        let metrics = executor.metrics();
        assert_eq!(metrics.tasks_succeeded_total, 0);
        assert_eq!(metrics.tasks_failed_total, 1);
        assert_eq!(metrics.healthy_workers_last, 1);
    }

    #[test]
    fn prometheus_render_is_stable_and_machine_readable() {
        let metrics = MetricsSnapshot {
            executions_total: 2,
            tasks_succeeded_total: 3,
            tasks_failed_total: 1,
            tasks_blocked_total: 1,
            execution_duration_ms_total: 42,
            execution_duration_ms_last: 17,
            workers_configured: 2,
            healthy_workers_last: 1,
        };
        let output = metrics.render_prometheus();
        assert!(output.contains("forge_executions_total 2\n"));
        assert!(output.contains("forge_tasks_succeeded_total 3\n"));
        assert!(output.contains("forge_workers_healthy 1\n"));
    }
}
