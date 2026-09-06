use std::collections::{HashMap, VecDeque};

pub type TaskId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: TaskId,
    pub command: String,
    pub dependencies: Vec<TaskId>,
    pub state: TaskState,
}

#[derive(Debug, Default)]
pub struct TaskGraph {
    tasks: HashMap<TaskId, Task>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphError {
    DuplicateTask(TaskId),
    MissingDependency { task: TaskId, dependency: TaskId },
    CycleDetected,
}

impl TaskGraph {
    pub fn add_task(
        &mut self,
        id: TaskId,
        command: impl Into<String>,
        dependencies: Vec<TaskId>,
    ) -> Result<(), GraphError> {
        if self.tasks.contains_key(&id) {
            return Err(GraphError::DuplicateTask(id));
        }

        for &dependency in &dependencies {
            if dependency == id || !self.tasks.contains_key(&dependency) {
                return Err(GraphError::MissingDependency {
                    task: id,
                    dependency,
                });
            }
        }

        self.tasks.insert(
            id,
            Task {
                id,
                command: command.into(),
                dependencies,
                state: TaskState::Pending,
            },
        );

        if self.has_cycle() {
            self.tasks.remove(&id);
            return Err(GraphError::CycleDetected);
        }

        Ok(())
    }

    pub fn task(&self, id: TaskId) -> Option<&Task> {
        self.tasks.get(&id)
    }

    pub fn task_mut(&mut self, id: TaskId) -> Option<&mut Task> {
        self.tasks.get_mut(&id)
    }

    pub fn runnable(&self) -> Vec<TaskId> {
        self.tasks
            .values()
            .filter(|task| {
                task.state == TaskState::Pending
                    && task.dependencies.iter().all(|dependency| {
                        self.tasks
                            .get(dependency)
                            .is_some_and(|task| task.state == TaskState::Succeeded)
                    })
            })
            .map(|task| task.id)
            .collect()
    }

    fn has_cycle(&self) -> bool {
        fn visit(
            id: TaskId,
            graph: &TaskGraph,
            visiting: &mut Vec<TaskId>,
            visited: &mut Vec<TaskId>,
        ) -> bool {
            if visiting.contains(&id) {
                return true;
            }
            if visited.contains(&id) {
                return false;
            }

            visiting.push(id);
            let cycle = graph
                .tasks
                .get(&id)
                .map(|task| {
                    task.dependencies
                        .iter()
                        .copied()
                        .any(|dependency| visit(dependency, graph, visiting, visited))
                })
                .unwrap_or(false);
            visiting.pop();

            if !cycle {
                visited.push(id);
            }
            cycle
        }

        let mut visiting = Vec::new();
        let mut visited = Vec::new();
        self.tasks
            .keys()
            .copied()
            .any(|id| visit(id, self, &mut visiting, &mut visited))
    }
}

#[derive(Debug, Default)]
pub struct Scheduler {
    queue: VecDeque<TaskId>,
}

impl Scheduler {
    pub fn refresh(&mut self, graph: &TaskGraph) {
        self.queue.clear();
        self.queue.extend(graph.runnable());
    }

    pub fn next(&mut self) -> Option<TaskId> {
        self.queue.pop_front()
    }

    pub fn queued(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependencies_control_runnable_tasks() {
        let mut graph = TaskGraph::default();
        graph.add_task(1, "compile", Vec::new()).unwrap();
        graph.add_task(2, "test", vec![1]).unwrap();

        assert_eq!(graph.runnable(), vec![1]);

        graph.task_mut(1).unwrap().state = TaskState::Succeeded;
        assert_eq!(graph.runnable(), vec![2]);
    }

    #[test]
    fn duplicate_tasks_are_rejected() {
        let mut graph = TaskGraph::default();
        graph.add_task(1, "compile", Vec::new()).unwrap();
        assert_eq!(
            graph.add_task(1, "test", Vec::new()),
            Err(GraphError::DuplicateTask(1))
        );
    }

    #[test]
    fn scheduler_dispatches_ready_work() {
        let mut graph = TaskGraph::default();
        graph.add_task(1, "compile", Vec::new()).unwrap();
        graph.add_task(2, "test", vec![1]).unwrap();

        let mut scheduler = Scheduler::default();
        scheduler.refresh(&graph);
        assert_eq!(scheduler.next(), Some(1));
        assert_eq!(scheduler.next(), None);
    }
}
