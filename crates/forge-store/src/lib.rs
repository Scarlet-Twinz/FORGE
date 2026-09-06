use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    pub job_id: u64,
    pub status: String,
}

#[derive(Debug)]
pub struct JobStore {
    path: PathBuf,
    jobs: HashMap<u64, JobRecord>,
}

impl JobStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut jobs = HashMap::new();

        if path.exists() {
            let file = File::open(&path)?;
            for line in BufReader::new(file).lines() {
                let line = line?;
                let mut fields = line.splitn(2, '\t');
                let Some(id) = fields.next() else { continue };
                let Some(status) = fields.next() else { continue };
                if let Ok(job_id) = id.parse::<u64>() {
                    jobs.insert(job_id, JobRecord { job_id, status: status.to_owned() });
                }
            }
        }

        Ok(Self { path, jobs })
    }

    pub fn upsert(&mut self, record: JobRecord) -> io::Result<()> {
        self.jobs.insert(record.job_id, record);
        self.persist()
    }

    pub fn get(&self, job_id: u64) -> Option<&JobRecord> {
        self.jobs.get(&job_id)
    }

    fn persist(&self) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let temp = self.path.with_extension("tmp");
        {
            let mut file = File::create(&temp)?;
            for record in self.jobs.values() {
                writeln!(file, "{}\t{}", record.job_id, record.status)?;
            }
            file.sync_all()?;
        }

        fs::rename(temp, &self.path)
    }

    pub fn append_audit(path: impl AsRef<Path>, message: &str) -> io::Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        writeln!(file, "{message}")?;
        file.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_survive_reopen() {
        let path = std::env::temp_dir().join(format!("forge-store-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        {
            let mut store = JobStore::open(&path).unwrap();
            store.upsert(JobRecord { job_id: 7, status: "succeeded".into() }).unwrap();
        }
        let store = JobStore::open(&path).unwrap();
        assert_eq!(store.get(7).unwrap().status, "succeeded");
        let _ = fs::remove_file(path);
    }
}
