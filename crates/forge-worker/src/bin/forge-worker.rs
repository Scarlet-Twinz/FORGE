use std::env;
use std::net::{TcpListener, TcpStream};
use std::thread;

use forge_worker::{handle_connection, Worker};

fn handle_stream(mut stream: TcpStream, worker: Worker) {
    if let Err(error) = handle_connection(&mut stream, &worker) {
        eprintln!("worker connection failed: {error}");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9100".to_string());
    let listener = TcpListener::bind(&address)?;
    let worker = Worker::new("worker-1", 4);

    println!("FORGE worker listening on {address}");
    println!("worker id={} max_concurrency={}", worker.id, worker.max_concurrency);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let worker = worker.clone();
                thread::spawn(move || handle_stream(stream, worker));
            }
            Err(error) => eprintln!("worker accept failed: {error}"),
        }
    }

    Ok(())
}
