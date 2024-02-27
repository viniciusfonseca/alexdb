use std::{str::FromStr, sync::Arc};

use axum::{body::Bytes, extract::{Path, State}, Json};
use tokio::net::UdpSocket;

use crate::{db_state::DbState, handlers::{self, atomics::CreateAtomicPayload}};

pub async fn net_loop(socket: UdpSocket, db_state: Arc<DbState>) {
    loop {
        let mut buf = [0; 256];
        let (_, addr) = socket.recv_from(&mut buf).await.unwrap();

        let cmd = match String::from_utf8(buf[0..9].to_vec()) {
            Ok(s) => s,
            Err(_) => continue
        };

        let data = &buf[9..256];

        let (_, response) = match cmd.trim() {
            "CREATE" => {
                let id = parse_num::<i32>(&data[0..32]).unwrap();
                let min_value = parse_num::<i32>(&data[32..42]).unwrap();
                let log_size = parse_num::<usize>(&data[42..52]).unwrap();
                handlers::atomics::create_atomic(State(db_state.clone()), Json(CreateAtomicPayload {
                    id,
                    min_value,
                    log_size
                })).await
            },
            "MUTATE" => {
                let atomic_id = parse_num::<i32>(&data[0..32]).unwrap();
                let value = parse_num::<i32>(&data[32..42]).unwrap();
                let payload = Bytes::copy_from_slice(&data[32..245]);
                handlers::atomics::mutate_atomic(State(db_state.clone()), Path((atomic_id, value)), payload).await
            },
            "GET" => {
                let atomic_id = parse_num::<i32>(&data[0..32]).unwrap();
                handlers::atomics::get_atomic(State(db_state.clone()), Path(atomic_id)).await
            },
            _ => continue
        };
        
        socket.send_to(response.as_bytes(), addr).await.unwrap();
    }
}

fn parse_num<T: FromStr>(slice: &[u8]) -> Result<T, <T as FromStr>::Err> {
    String::from_utf8(slice.to_vec()).unwrap().trim().parse::<T>()
}