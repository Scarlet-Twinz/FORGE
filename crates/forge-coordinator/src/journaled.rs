use std::io;
use std::path::Path;

use forge_core::{TaskGraph, TaskId, TaskState};
use forge_store::journal::{ExecutionEvent, ExecutionJournal};

use crate::{CoordinatorError, DistributedExecutor};

#[derive(Debug)]
pub struct JournaledDistributedExecutor {
    executor: DistributedExecutor,
    journal: ExecutionJournal,
}

impl JournaledDistributedExecutor {
    pub fn open<I, S>(addresses: I, journal_path: impl AsRef<Path>) -> io::Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Ok(Self {
            executor: DistributedExecutor::new(addresses),
            journal: ExecutionJournal::open(journal_path)?,
        })
    }

    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.executor = self.executor.with_max_attempts(max_attempts);
        self
    }

    pub fn worker_count(&self) -> usize {
        self.executor.worker_count()
    }

    pub fn healthy_worker_count(&self) -> usize {
        self.executor.healthy_worker_count()
    }

    pub fn execute(&mut self, graph: &mut TaskGraph) -> Result<Vec<TaskId>, CoordinatorError> {
        let pending = graph.pending();

        for task_id in &pending {
            self.journal
                .append(&ExecutionEvent::TaskStarted { task_id: *task_id, attempt: 1 })
                .map_err(CoordinatorError::Io)?;
        }

        let result = self.executor.execute(graph);

        for task_id in pending {
            let Some(task) = graph.task(task_id) else { continue };
            match task.state {
                TaskState::Succeeded => self
                    .journal
                    .append(&ExecutionEvent::TaskSucceeded { task_id })
                    .map_err(CoordinatorError::Io)?,
                TaskState::Failed | TaskState::Blocked => self
                    .journal
                    .append(&ExecutionEvent::TaskFailed { task_id })
                    .map_err(CoordinatorError::Io)?,
                _ => {}
            }
        }

        result
    }

    pub fn replay(&self) -> io::Result<Vec<ExecutionEvent>> {
        self.journal.replay()
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
    fn execution_journal_records_start_and_terminal_state() {
        let root = std::env::temp_dir().join(format!("forge-journaled-{}", std::process::id()));
        let _ = std::fs::remove_file(&root);
        let (address, worker_thread) = spawn_worker(Worker::new("journal-worker", 1), 2);

        let mut graph = TaskGraph::default();
        graph.add_task(1, "echo journal", Vec::new()).unwrap();

        let mut executor = JournaledDistributedExecutor::open([address], &root).unwrap();
        assert_eq!(executor.execute(&mut graph).unwrap(), vec![1]);
        worker_thread.join().unwrap();

        let events = executor.replay().unwrap();
        assert_eq!(events[0], ExecutionEvent::TaskStarted { task_id: 1, attempt: 1 });
        assert_eq!(events[1], ExecutionEvent::TaskSucceeded { task_id: 1 });

        let _ = std::fs::remove_file(root);
    }
}
