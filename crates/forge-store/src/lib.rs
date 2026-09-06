pub mod journal;

use sha2::{Digest, Sha256};
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

        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        fs::rename(temp, &self.path)
    }

    pub fn append_audit(path: impl AsRef<Path>, message: &str) -> io::Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        writeln!(file, "{message}")?;
        file.sync_data()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    pub hash: String,
    pub size: u64,
}

#[derive(Debug)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects"))?;
        Ok(Self { root })
    }

    pub fn put(&self, data: &[u8]) -> io::Result<Artifact> {
        let hash = sha256_hex(data);
        let path = self.object_path(&hash);

        if !path.exists() {
            let temp = path.with_extension("tmp");
            {
                let mut file = File::create(&temp)?;
                file.write_all(data)?;
                file.sync_all()?;
            }
            if let Err(error) = fs::rename(&temp, &path) {
                let _ = fs::remove_file(&temp);
                if !path.exists() {
                    return Err(error);
                }
            }
        }

        Ok(Artifact { hash, size: data.len() as u64 })
    }

    pub fn get(&self, hash: &str) -> io::Result<Option<Vec<u8>>> {
        if !is_valid_hash(hash) {
            return Ok(None);
        }

        let path = self.object_path(hash);
        match fs::read(path) {
            Ok(data) => Ok(Some(data)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn contains(&self, hash: &str) -> bool {
        is_valid_hash(hash) && self.object_path(hash).is_file()
    }

    fn object_path(&self, hash: &str) -> PathBuf {
        self.root.join("objects").join(hash)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    pub key: String,
    pub artifact_hash: String,
}

#[derive(Debug)]
pub struct CacheStore {
    path: PathBuf,
    entries: HashMap<String, CacheEntry>,
}

impl CacheStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut entries = HashMap::new();

        if path.exists() {
            let file = File::open(&path)?;
            for line in BufReader::new(file).lines() {
                let line = line?;
                let mut fields = line.splitn(2, '\t');
                let Some(key) = fields.next() else { continue };
                let Some(artifact_hash) = fields.next() else { continue };
                if !key.is_empty() && is_valid_hash(artifact_hash) {
                    entries.insert(
                        key.to_owned(),
                        CacheEntry { key: key.to_owned(), artifact_hash: artifact_hash.to_owned() },
                    );
                }
            }
        }

        Ok(Self { path, entries })
    }

    pub fn lookup(&self, key: &str) -> Option<&CacheEntry> {
        self.entries.get(key)
    }

    pub fn insert(&mut self, key: impl Into<String>, artifact_hash: impl Into<String>) -> io::Result<()> {
        let key = key.into();
        let artifact_hash = artifact_hash.into();
        if key.is_empty() || !is_valid_hash(&artifact_hash) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid cache entry"));
        }

        self.entries.insert(key.clone(), CacheEntry { key, artifact_hash });
        self.persist()
    }

    pub fn remove(&mut self, key: &str) -> io::Result<bool> {
        let removed = self.entries.remove(key).is_some();
        if removed {
            self.persist()?;
        }
        Ok(removed)
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
            let mut entries: Vec<_> = self.entries.values().collect();
            entries.sort_by(|left, right| left.key.cmp(&right.key));
            for entry in entries {
                writeln!(file, "{}\t{}", entry.key, entry.artifact_hash)?;
            }
            file.sync_all()?;
        }

        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        fs::rename(temp, &self.path)
    }
}

pub fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_survive_reopen_and_upsert() {
        let path = std::env::temp_dir().join(format!("forge-store-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        {
            let mut store = JobStore::open(&path).unwrap();
            store.upsert(JobRecord { job_id: 7, status: "running".into() }).unwrap();
            store.upsert(JobRecord { job_id: 7, status: "succeeded".into() }).unwrap();
        }
        let store = JobStore::open(&path).unwrap();
        assert_eq!(store.get(7).unwrap().status, "succeeded");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn artifact_store_round_trips_content_by_hash() {
        let root = std::env::temp_dir().join(format!("forge-artifacts-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let store = ArtifactStore::open(&root).unwrap();
        let artifact = store.put(b"forge artifact").unwrap();

        assert_eq!(artifact.size, 14);
        assert_eq!(artifact.hash.len(), 64);
        assert!(store.contains(&artifact.hash));
        assert_eq!(store.get(&artifact.hash).unwrap().unwrap(), b"forge artifact");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn identical_artifacts_are_deduplicated() {
        let root = std::env::temp_dir().join(format!("forge-artifacts-dedup-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let store = ArtifactStore::open(&root).unwrap();

        let first = store.put(b"same bytes").unwrap();
        let second = store.put(b"same bytes").unwrap();

        assert_eq!(first, second);
        assert_eq!(fs::read_dir(root.join("objects")).unwrap().count(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cache_entries_survive_reopen_and_replace_values() {
        let path = std::env::temp_dir().join(format!("forge-cache-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        let first_hash = sha256_hex(b"first");
        let second_hash = sha256_hex(b"second");

        {
            let mut cache = CacheStore::open(&path).unwrap();
            cache.insert("task:v1", &first_hash).unwrap();
            cache.insert("task:v1", &second_hash).unwrap();
        }

        let cache = CacheStore::open(&path).unwrap();
        assert_eq!(cache.lookup("task:v1").unwrap().artifact_hash, second_hash);
        assert!(cache.lookup("missing").is_none());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_cache_entries_are_rejected() {
        let path = std::env::temp_dir().join(format!("forge-cache-invalid-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        let mut cache = CacheStore::open(&path).unwrap();

        assert!(cache.insert("", sha256_hex(b"x")).is_err());
        assert!(cache.insert("task", "not-a-hash").is_err());

        let _ = fs::remove_file(path);
    }
}
