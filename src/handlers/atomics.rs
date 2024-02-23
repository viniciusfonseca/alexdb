use std::{env, sync::{atomic::Ordering, Arc}, time::SystemTime};

use axum::{body::Bytes, extract::{Path, State}, http::StatusCode, response::IntoResponse, Json};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

use crate::db_state::{DbAtomic, DbState};

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
    let _ = db_state.atomics.insert_async(payload.id, DbAtomic::new(payload.id, payload.min_value, payload.log_size).await).await;
    db_state.log_files.insert_async(payload.id, tokio::io::split(
        tokio::fs::File::open(format!("{data_path}/{}.log", payload.id)).await.unwrap()
    ).1,).await.unwrap();
    (StatusCode::CREATED, "")
}

pub async fn get_atomic(
    State(db_state): State<Arc<DbState>>,
    Path(atomic_id): Path<i32>,
) -> impl IntoResponse {
    let atomic = db_state.atomics.get(&atomic_id).unwrap();
    let atomic = atomic.get();
    (StatusCode::OK, format!("{},{}", atomic.value.load(Ordering::SeqCst), atomic.min_value))
}

pub async fn get_atomic_logs(
    State(db_state): State<Arc<DbState>>,
    Path(atomic_id): Path<i32>,
) -> impl IntoResponse {
    let atomics = &db_state.atomics;
    let _atomic = atomics.get(&atomic_id).unwrap();
}

pub async fn mutate_atomic(
    State(db_state): State<Arc<DbState>>,
    Path((atomic_id, value)): Path<(i32, i32)>,
    payload: Bytes
) -> impl IntoResponse {
    let atomic = db_state.atomics.get(&atomic_id).unwrap();
    let atomic = atomic.get();
    let stored_value = atomic.value.load(Ordering::Acquire);
    let updated_value = stored_value + value;
    if updated_value < atomic.min_value {
        return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    }
    let datetime = SystemTime::now();
    atomic.value.store(stored_value, Ordering::Release);
    let tx_id = db_state.tx_id.fetch_add(1, Ordering::AcqRel);
    let datetime_rfc3339 = parse_sys_time_as_string(datetime);
    let log_bytes = &[format!("{tx_id},{updated_value},{datetime_rfc3339}").as_bytes(), &payload].concat();
    let _ = db_state.log_files.get(&atomic_id).unwrap().get_mut().write_all(log_bytes).await;
    let _ = db_state.atomics.get(&atomic_id).unwrap().get_mut().file.write_all(value.to_string().as_bytes()).await;
    (StatusCode::OK, format!("{},{}", updated_value, atomic.min_value))
}

fn parse_sys_time_as_string(system_time: SystemTime) -> String {
    DateTime::<Utc>::from(system_time).format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}