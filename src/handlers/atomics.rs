use std::{sync::{atomic::Ordering, Arc}, time::SystemTime};

use axum::{body::Bytes, extract::{Path, State}, http::StatusCode, Json};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

use crate::db_state::{AtomicFd, DbAtomic, DbState};

const SPACES: u8 = 0x20;

#[derive(Debug, Deserialize)]
pub struct CreateAtomicPayload {
    pub id: i32,
    pub min_value: i32,
    pub log_size: usize
}

pub async fn create_atomic(
    State(db_state): State<Arc<DbState>>,
    Json(payload): Json<CreateAtomicPayload>
) -> (StatusCode, String) {
    let data_path = &db_state.data_path;
    {
        let mut atomics = db_state.atomics.write().await;
        _ = atomics.insert(payload.id, DbAtomic::new(payload.min_value, payload.log_size).await);
    }
    db_state.log_files.insert_async(payload.id, tokio::io::split(
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(format!("{data_path}/{}.log", payload.id)).await.unwrap()
    ).1).await.unwrap();
    _ = db_state.atomic_fd.insert_async(payload.id, AtomicFd::new(payload.id, payload.log_size).await).await;
    println!("New atomic created: {}", payload.id);
    (StatusCode::CREATED, String::new())
}

pub async fn get_atomic(
    State(db_state): State<Arc<DbState>>,
    Path(atomic_id): Path<i32>,
) -> (StatusCode, String) {
    let atomics = db_state.atomics.read().await;
    let atomic = atomics.get(&atomic_id).unwrap();
    (StatusCode::OK, atomic.value.load(Ordering::SeqCst).to_string())
}

pub async fn get_atomic_logs(
    State(db_state): State<Arc<DbState>>,
    Path(atomic_id): Path<i32>,
) -> (StatusCode, Vec<u8>) {
    let mut atomic_fd = db_state.atomic_fd.get_async(&atomic_id).await.unwrap();
    let atomic_fd = atomic_fd.get_mut();
    let logs_file = atomic_fd.get_logs_file(&db_state).await;
    (StatusCode::OK, atomic_fd.get_logs(logs_file, 10).await)
}

pub async fn mutate_atomic(
    State(db_state): State<Arc<DbState>>,
    Path((atomic_id, value)): Path<(i32, i32)>,
    payload: Bytes
) -> (StatusCode, String) {
    let atomics = db_state.atomics.read().await;
    let atomic = atomics.get(&atomic_id).unwrap();
    let stored_value = atomic.value.load(Ordering::Acquire);
    let updated_value = stored_value + value;
    if updated_value < atomic.min_value {
        return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    }
    let datetime = SystemTime::now();
    let tx_id = db_state.tx_id.fetch_add(1, Ordering::AcqRel);
    atomic.value.store(updated_value, Ordering::Release);
    let datetime_rfc3339 = parse_sys_time_as_string(datetime);
    let mut log_bytes = Vec::new();
    let log_info = &[format!("{tx_id},{value},{updated_value},{datetime_rfc3339},").as_bytes(), &payload].concat();
    let _ = log_bytes.write_all(&log_info).await;
    let log_bytes_len = log_bytes.len();
    let spaces = vec![SPACES; atomic.log_size - log_bytes_len - 1];
    let log = &[log_bytes, spaces, vec![0x0A]].concat();
    _ = db_state.fs_channel.send((log.to_vec(), atomic_id)).await;
    (StatusCode::OK, updated_value.to_string())
}

fn parse_sys_time_as_string(system_time: SystemTime) -> String {
    DateTime::<Utc>::from(system_time).format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, fs::create_dir_all, str::FromStr};

    use crate::{db_state::{AtomicLog, FsChannelMsg}, fs_channel};

    use super::*;

    async fn create_test_db_state(test_name: &str) -> (Arc<DbState>, tokio::sync::mpsc::Receiver<FsChannelMsg>) {
        let (fs_channel_tx, fs_channel_rx) = tokio::sync::mpsc::channel(100);
        let tmp = &temp_dir();
        let tmp = tmp.to_str().unwrap();
        let data_path = &format!("{tmp}/{test_name}");
        create_dir_all(data_path).unwrap();
        let db_state = Arc::new(DbState {
            data_path: data_path.to_string(),
            atomics: Default::default(),
            tx_id: Default::default(),
            log_files: Default::default(),
            atomic_fd: Default::default(),
            fs_channel: fs_channel_tx,
        });
        (db_state, fs_channel_rx)
    }

    impl Drop for DbState {
        fn drop(&mut self) {
            _ = std::fs::remove_dir_all(&self.data_path);
        }
    }

    #[tokio::test]
    async fn test_create_atomic() {

        let (db_state, _) = create_test_db_state("test_create_atomic").await;
        let payload = CreateAtomicPayload {
            id: 1,
            min_value: 0,
            log_size: 100
        };
        let (status, _) = create_atomic(State(db_state), Json(payload)).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    fn parse_logs(logs: Vec<u8>) -> Vec<AtomicLog> {
        let lines = String::from_utf8(logs).unwrap();
        let lines = lines.trim_matches(char::from(0x0A)).split("\n");
        let mut r = Vec::new();
        for line in lines {
            let split = line.split(",").collect::<Vec<&str>>();
            let txid = match split.get(0).unwrap().trim_matches(char::from(0)).parse::<i32>() {
                Ok(i) => i,
                Err(_) => {
                    continue;
                }
            };
            r.push(AtomicLog {
                tx_id: txid,
                value: match split.get(1).unwrap().trim_matches(char::from(0)).parse::<i32>() {
                    Ok(i) => i,
                    Err(_) => {
                        continue;
                    }
                },
                updated_value: match split.get(2).unwrap().trim_matches(char::from(0)).parse::<i32>() {
                    Ok(i) => i,
                    Err(_) => {
                        continue;
                    }
                },
                datetime: split.get(3).unwrap().trim_matches(char::from(0)).to_string(),
                payload: split.get(4).unwrap().trim_matches(char::from(0)).to_string(),
            })
        }
        r
    }

    #[tokio::test]
    async fn test_mutate_atomic() {

        let (db_state, mut fs_channel_rx) = create_test_db_state("test_mutate_atomic").await;
        let payload = CreateAtomicPayload {
            id: 1,
            min_value: 0,
            log_size: 72
        };
        let db_state = State(db_state);
        let (status, _) = create_atomic(db_state.clone(), Json(payload)).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, value) = mutate_atomic(db_state.clone(), Path((1, 1)), Bytes::from("test")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, "1");
        let (status, value) = get_atomic(db_state.clone(), Path(1)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, "1");

        fs_channel::recv(&mut fs_channel_rx, &db_state).await;

        let (_, lines) = get_atomic_logs(db_state.clone(), Path(1)).await;
        
        let r = parse_logs(lines);

        assert_eq!(r.len(), 1);
        // rewrite assertions below
        assert_eq!(r[0].tx_id, 0);
        assert_eq!(r[0].value, 1);
        assert_eq!(r[0].updated_value, 1);
        assert!(DateTime::<Utc>::from_str(&r[0].datetime).is_ok());
        assert_eq!(r[0].payload.trim_end(), "test");
    }

    #[tokio::test]
    async fn test_mutate_atomic_concurrent() {

        let (db_state, mut fs_channel_rx) = create_test_db_state("test_mutate_atomic_concurrent").await;
        let payload = CreateAtomicPayload {
            id: 1,
            min_value: 0,
            log_size: 72
        };
        let db_state = State(db_state);
        let (status, _) = create_atomic(db_state.clone(), Json(payload)).await;
        assert_eq!(status, StatusCode::CREATED);

        let handle1 = tokio::spawn(mutate_atomic(db_state.clone(), Path((1, 1)), Bytes::from("test")));
        let handle2 = tokio::spawn(mutate_atomic(db_state.clone(), Path((1, 2)), Bytes::from("test2")));
        let (status, value) = handle1.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, "1");
        let (status, value) = handle2.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, "3");

        fs_channel::recv(&mut fs_channel_rx, &db_state).await;
        fs_channel::recv(&mut fs_channel_rx, &db_state).await;

        let (_, lines) = get_atomic_logs(db_state.clone(), Path(1)).await;
        
        let r = parse_logs(lines);

        assert_eq!(r.len(), 2);

        assert_eq!(r[0].tx_id, 0);
        assert_eq!(r[0].value, 1);
        assert_eq!(r[0].updated_value, 1);
        assert!(DateTime::<Utc>::from_str(&r[0].datetime).is_ok());
        assert_eq!(r[0].payload.trim_end(), "test");

        assert_eq!(r[1].tx_id, 1);
        assert_eq!(r[1].value, 2);
        assert_eq!(r[1].updated_value, 3);
        assert!(DateTime::<Utc>::from_str(&r[1].datetime).is_ok());
        assert_eq!(r[1].payload.trim_end(), "test2");
    }
}