use std::env;

use forge_coordinator::WorkerClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let address = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:9100".to_string());
    let command = args.collect::<Vec<_>>().join(" ");
    let command = if command.is_empty() {
        default_command().to_string()
    } else {
        command
    };

    let client = WorkerClient::new(&address);
    let result = client.execute(1, command)?;

    println!("remote task=1 state={}", if result.success { "Succeeded" } else { "Failed" });
    println!("exit_code={:?}", result.exit_code);
    println!("stdout={}", result.stdout.trim_end());
    if !result.stderr.is_empty() {
        println!("stderr={}", result.stderr.trim_end());
    }

    if result.success {
        Ok(())
    } else {
        std::process::exit(1);
    }
}

#[cfg(target_os = "windows")]
fn default_command() -> &'static str {
    "echo forge-remote"
}

#[cfg(not(target_os = "windows"))]
fn default_command() -> &'static str {
    "printf 'forge-remote\\n'"
}
