use std::{env, sync::atomic::AtomicI32};
use tokio::{fs::{File, OpenOptions}, io::WriteHalf};

pub struct DbAtomic {
    pub id: i32,
    pub file: File,
    pub value: AtomicI32,
    pub min_value: i32,
    pub log_size: usize
}

pub struct DbState {
    pub atomics: scc::HashMap<i32, DbAtomic>,
    pub tx_id: AtomicI32,
    pub log_files: scc::HashMap<i32, WriteHalf<File>>
}

impl DbAtomic {
    pub async fn new(id: i32, min_value: i32, log_size: usize) -> DbAtomic {
        let data_path = env::var("DATA_PATH").expect("no DATA_PATH env var found");
        DbAtomic {
            id,
            file: OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(format!("{data_path}/{id}")).await.unwrap(),
            value: AtomicI32::new(0),
            min_value,
            log_size,
        }
    }
}