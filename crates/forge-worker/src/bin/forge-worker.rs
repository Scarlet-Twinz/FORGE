use std::env;
use std::net::TcpListener;

use forge_worker::{handle_connection, Worker};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9100".to_string());
    let listener = TcpListener::bind(&address)?;
    let worker = Worker::new("worker-1", 1);

    println!("FORGE worker listening on {address}");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                if let Err(error) = handle_connection(&mut stream, &worker) {
                    eprintln!("worker connection failed: {error}");
                }
            }
            Err(error) => eprintln!("worker accept failed: {error}"),
        }
    }

    Ok(())
}
