use std::sync::Arc;

use tokio::{io::AsyncWriteExt, sync::mpsc::Receiver};

use crate::db_state::{DbState, FsChannelMsg};

pub fn setup(mut fs_channel_rx: Receiver<FsChannelMsg>, db_state: Arc<DbState>) {
    tokio::spawn(async move {
        loop {
            let (log, atomic_id) = fs_channel_rx.recv().await.unwrap();
            _ = db_state
                .log_files
                .get(&atomic_id)
                .unwrap()
                .get_mut()
                .write_all(&log)
                .await;
        }
    });
}
