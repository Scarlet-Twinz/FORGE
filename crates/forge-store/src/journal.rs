use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionEvent {
    TaskStarted { task_id: u64, attempt: usize },
    TaskSucceeded { task_id: u64 },
    TaskFailed { task_id: u64 },
    TaskRetry { task_id: u64, attempt: usize },
    WorkerUnhealthy { address: String },
}

impl ExecutionEvent {
    fn encode(&self) -> String {
        match self {
            Self::TaskStarted { task_id, attempt } => format!("task_started\t{task_id}\t{attempt}"),
            Self::TaskSucceeded { task_id } => format!("task_succeeded\t{task_id}"),
            Self::TaskFailed { task_id } => format!("task_failed\t{task_id}"),
            Self::TaskRetry { task_id, attempt } => format!("task_retry\t{task_id}\t{attempt}"),
            Self::WorkerUnhealthy { address } => format!("worker_unhealthy\t{}", escape(address)),
        }
    }

    fn decode(line: &str) -> io::Result<Self> {
        let mut fields = line.split('\t');
        let kind = fields.next().unwrap_or_default();
        let parse_u64 = |value: Option<&str>| {
            value
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing event field"))?
                .parse::<u64>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid event integer"))
        };
        let parse_usize = |value: Option<&str>| {
            value
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing event field"))?
                .parse::<usize>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid event integer"))
        };

        let event = match kind {
            "task_started" => Self::TaskStarted {
                task_id: parse_u64(fields.next())?,
                attempt: parse_usize(fields.next())?,
            },
            "task_succeeded" => Self::TaskSucceeded { task_id: parse_u64(fields.next())? },
            "task_failed" => Self::TaskFailed { task_id: parse_u64(fields.next())? },
            "task_retry" => Self::TaskRetry {
                task_id: parse_u64(fields.next())?,
                attempt: parse_usize(fields.next())?,
            },
            "worker_unhealthy" => Self::WorkerUnhealthy {
                address: unescape(fields.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing worker address")
                })?)?,
            },
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown execution event")),
        };

        if fields.next().is_some() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "trailing execution event fields"));
        }
        Ok(event)
    }
}

#[derive(Debug)]
pub struct ExecutionJournal {
    path: PathBuf,
}

impl ExecutionJournal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { path })
    }

    pub fn append(&self, event: &ExecutionEvent) -> io::Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(&self.path)?;
        writeln!(file, "{}", event.encode())?;
        file.sync_data()
    }

    pub fn replay(&self) -> io::Result<Vec<ExecutionEvent>> {
        let file = File::open(&self.path)?;
        let mut events = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            events.push(ExecutionEvent::decode(&line)?);
        }
        Ok(events)
    }
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\t', "\\t").replace('\n', "\\n")
}

fn unescape(value: &str) -> io::Result<String> {
    let mut output = String::with_capacity(value.len());
    let mut escaped = false;
    for byte in value.bytes() {
        if escaped {
            match byte {
                b'\\' => output.push('\\'),
                b't' => output.push('\t'),
                b'n' => output.push('\n'),
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid event escape")),
            }
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else {
            output.push(byte as char);
        }
    }
    if escaped {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unterminated event escape"));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_survives_reopen_and_replays_events() {
        let path = std::env::temp_dir().join(format!("forge-journal-{}.log", std::process::id()));
        let _ = fs::remove_file(&path);

        {
            let journal = ExecutionJournal::open(&path).unwrap();
            journal.append(&ExecutionEvent::TaskStarted { task_id: 7, attempt: 1 }).unwrap();
            journal.append(&ExecutionEvent::TaskRetry { task_id: 7, attempt: 2 }).unwrap();
            journal.append(&ExecutionEvent::WorkerUnhealthy { address: "127.0.0.1:9100".into() }).unwrap();
            journal.append(&ExecutionEvent::TaskSucceeded { task_id: 7 }).unwrap();
        }

        let journal = ExecutionJournal::open(&path).unwrap();
        assert_eq!(journal.replay().unwrap(), vec![
            ExecutionEvent::TaskStarted { task_id: 7, attempt: 1 },
            ExecutionEvent::TaskRetry { task_id: 7, attempt: 2 },
            ExecutionEvent::WorkerUnhealthy { address: "127.0.0.1:9100".into() },
            ExecutionEvent::TaskSucceeded { task_id: 7 },
        ]);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn journal_preserves_escaped_worker_addresses() {
        let path = std::env::temp_dir().join(format!("forge-journal-escape-{}.log", std::process::id()));
        let _ = fs::remove_file(&path);
        let journal = ExecutionJournal::open(&path).unwrap();
        let address = "worker\\node\n01\t9100";
        journal.append(&ExecutionEvent::WorkerUnhealthy { address: address.into() }).unwrap();
        assert_eq!(journal.replay().unwrap(), vec![ExecutionEvent::WorkerUnhealthy { address: address.into() }]);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn malformed_events_are_rejected() {
        let path = std::env::temp_dir().join(format!("forge-journal-invalid-{}.log", std::process::id()));
        let _ = fs::remove_file(&path);
        fs::write(&path, "task_started\tbad\t1\n").unwrap();
        let journal = ExecutionJournal::open(&path).unwrap();
        assert!(journal.replay().is_err());
        let _ = fs::remove_file(path);
    }
}
