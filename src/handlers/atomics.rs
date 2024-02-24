use std::{env, sync::{atomic::Ordering, Arc}, time::SystemTime};

use axum::{body::Bytes, extract::{Path, State}, http::StatusCode, response::IntoResponse, Json};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

use crate::db_state::{DbAtomic, DbState};

const SPACES: u8 = 0x20;

#[derive(Deserialize)]
pub struct CreateAtomicPayload {
    id: i32,
    min_value: i32,
    log_size: usize
}

pub async fn create_atomic(
    State(db_state): State<Arc<DbState>>,
    Json(payload): Json<CreateAtomicPayload>
) -> impl IntoResponse {
    let data_path = env::var("DATA_PATH").expect("no DATA_PATH env var found");
    {
        let mut atomics = db_state.atomics.write().await;
        let _ = atomics.insert(payload.id, DbAtomic::new(payload.id, payload.min_value, payload.log_size).await);
    }
    db_state.log_files.insert_async(payload.id, tokio::io::split(
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(format!("{data_path}/{}.log", payload.id)).await.unwrap()
    ).1).await.unwrap();
    db_state.atomic_files.insert_async(payload.id, 
        OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .append(false)
            .open(format!("{data_path}/{}.a", payload.id)).await.unwrap()
    ).await.unwrap();
    println!("New atomic created: {}", payload.id);
    (StatusCode::CREATED, "")
}

pub async fn get_atomic(
    State(db_state): State<Arc<DbState>>,
    Path(atomic_id): Path<i32>,
) -> impl IntoResponse {
    let atomics = db_state.atomics.read().await;
    let atomic = atomics.get(&atomic_id).unwrap();
    (StatusCode::OK, format!("{},{}", atomic.value.load(Ordering::SeqCst), atomic.min_value))
}

pub async fn get_atomic_logs(
    State(db_state): State<Arc<DbState>>,
    Path(atomic_id): Path<i32>,
) -> impl IntoResponse {
    let atomics = &db_state.atomics.read().await;
    let _atomic = atomics.get(&atomic_id).unwrap();
}

pub async fn mutate_atomic(
    State(db_state): State<Arc<DbState>>,
    Path((atomic_id, value)): Path<(i32, i32)>,
    payload: Bytes
) -> impl IntoResponse {
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
    let log_info = &[format!("{tx_id},{updated_value},{datetime_rfc3339},").as_bytes(), &payload].concat();
    let _ = log_bytes.write_all(&log_info).await;
    let log_bytes_len = log_bytes.len();
    if log_bytes_len > atomic.log_size - 1 {
        return (StatusCode::BAD_REQUEST, String::new())
    }
    let spaces = vec![SPACES; atomic.log_size - log_bytes_len - 1];
    _ = log_bytes.write_all(&spaces).await;
    let log = &[log_bytes, spaces, vec![0x0A]].concat();
    _ = db_state.fs_channel.send((log.to_vec(), atomic_id, updated_value));
    (StatusCode::OK, updated_value.to_string())
}

fn parse_sys_time_as_string(system_time: SystemTime) -> String {
    DateTime::<Utc>::from(system_time).format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}