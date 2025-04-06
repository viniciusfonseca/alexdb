use std::sync::{atomic::AtomicI32, Arc};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, WriteHalf},
    sync::{mpsc::Sender, RwLock},
};

pub type FsChannelMsg = (Vec<u8>, i32);

pub struct DbAtomic {
    pub value: Arc<AtomicI32>,
    pub min_value: i32,
    pub log_size: usize,
}

pub struct AtomicFd {
    id: i32,
    log_size: usize,
}

pub struct DbState {
    pub data_path: String,
    pub atomics: RwLock<std::collections::HashMap<i32, DbAtomic>>,
    pub atomic_fd: scc::HashMap<i32, AtomicFd>,
    pub tx_id: AtomicI32,
    pub log_files: scc::HashMap<i32, WriteHalf<File>>,
    pub fs_channel: Sender<FsChannelMsg>,
}

impl DbAtomic {
    pub async fn new(min_value: i32, log_size: usize) -> DbAtomic {
        DbAtomic {
            value: Arc::new(AtomicI32::new(0)),
            min_value,
            log_size,
        }
    }
}

pub struct AtomicLog {
    pub tx_id: i32,
    pub value: i32,
    pub updated_value: i32,
    pub datetime: String,
    pub payload: String,
}

impl AtomicFd {
    pub async fn new(id: i32, log_size: usize) -> AtomicFd {
        AtomicFd { id, log_size }
    }

    pub async fn get_logs_file(&mut self, db_state: &Arc<DbState>) -> File {
        let data_path = &db_state.data_path;
        OpenOptions::new()
            .read(true)
            .write(false)
            .open(format!("{data_path}/{}.log", self.id))
            .await
            .unwrap()
    }

    pub async fn get_logs(&mut self, mut logs: File, max: usize) -> Vec<u8> {
        let buffer_size = self.log_size * max;
        _ = logs.seek(std::io::SeekFrom::End(0)).await;
        let cursor_target = -(TryInto::<i64>::try_into(buffer_size)).unwrap();
        if logs
            .seek(std::io::SeekFrom::Current(cursor_target))
            .await
            .is_err()
        {
            _ = logs.seek(std::io::SeekFrom::Start(0)).await;
        }
        let mut buf = vec![0u8; buffer_size - 1];
        let bytes_read = logs.read(&mut buf).await.unwrap();
        if bytes_read == 0 {
            return Vec::new();
        }
        buf
    }
}