use std::sync::{mpsc::Receiver, Arc};

use tokio::io::AsyncWriteExt;

use crate::db_state::{DbState, FsChannelMsg};

pub fn setup(fs_channel_rx: Receiver<FsChannelMsg>, db_state: Arc<DbState>) {
    tokio::spawn(async move {
        loop {
            let (log, atomic_id, updated_value) = fs_channel_rx.recv().unwrap();
            _ = db_state
                .log_files
                .get(&atomic_id)
                .unwrap()
                .get_mut()
                .write_all(&log)
                .await;
            let mut atomic_file = db_state.atomic_files.get(&atomic_id).unwrap();
            let atomic_file = atomic_file.get_mut();
            _ = atomic_file.set_len(0).await;
            _ = atomic_file
                .write_all(updated_value.to_string().as_bytes())
                .await;
        }
    });
}
