use std::sync::{atomic::AtomicI32, Arc};
use tokio::{fs::File, io::WriteHalf, sync::RwLock};

pub struct DbAtomic {
    pub id: i32,
    pub value: Arc<AtomicI32>,
    pub min_value: i32,
    pub log_size: usize
}

pub struct DbState {
    pub atomics: RwLock<std::collections::HashMap<i32, DbAtomic>>,
    pub tx_id: AtomicI32,
    pub log_files: scc::HashMap<i32, WriteHalf<File>>,
    pub atomic_files: scc::HashMap<i32, WriteHalf<File>>
}

impl DbAtomic {
    pub async fn new(id: i32, min_value: i32, log_size: usize) -> DbAtomic {
        DbAtomic {
            id,
            value: Arc::new(AtomicI32::new(0)),
            min_value,
            log_size,
        }
    }
}