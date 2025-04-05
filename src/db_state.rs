use std::sync::{atomic::AtomicI32, Arc};
use tokio::{fs::File, io::WriteHalf, sync::{mpsc::Sender, RwLock}};

pub type FsChannelMsg = (Vec<u8>, i32);

pub struct DbAtomic {
    pub value: Arc<AtomicI32>,
    pub min_value: i32,
    pub log_size: usize
}

pub struct DbState {
    pub atomics: RwLock<std::collections::HashMap<i32, DbAtomic>>,
    pub tx_id: AtomicI32,
    pub log_files: scc::HashMap<i32, WriteHalf<File>>,
    pub fs_channel: Sender<FsChannelMsg>
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