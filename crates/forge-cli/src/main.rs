use forge_core::{Scheduler, TaskGraph, TaskState};
use forge_store::{JobRecord, JobStore};
use forge_worker::Worker;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = TaskGraph::default();
    graph.add_task(1, "echo forge-build", Vec::new())?;
    graph.add_task(2, "echo forge-test", vec![1])?;

    let mut scheduler = Scheduler::default();
    let worker = Worker::new("local-1", 1);
    let store_path = std::env::temp_dir().join("forge-jobs.db");
    let mut store = JobStore::open(store_path)?;

    scheduler.refresh(&graph);
    while let Some(task_id) = scheduler.next() {
        let command = graph.task(task_id).expect("scheduled task disappeared").command.clone();
        graph.task_mut(task_id).expect("task disappeared").state = TaskState::Running;
        let result = worker.execute(&command)?;
        let state = if result.success { TaskState::Succeeded } else { TaskState::Failed };
        graph.task_mut(task_id).expect("task disappeared").state = state;
        store.upsert(JobRecord {
            job_id: task_id,
            status: format!("{:?}", state).to_lowercase(),
        })?;
        println!("task={task_id} worker={} state={state:?} duration={:?}", worker.id, result.duration);
    }

    scheduler.refresh(&graph);
    if let Some(task_id) = scheduler.next() {
        eprintln!("unexpected runnable task: {task_id}");
        std::process::exit(1);
    }

    println!("FORGE local execution complete");
    Ok(())
}
