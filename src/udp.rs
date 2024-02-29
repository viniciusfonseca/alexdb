use std::{str::FromStr, sync::Arc};

use axum::{body::Bytes, extract::{Path, State}, http::StatusCode, Json};
use tokio::net::UdpSocket;

use crate::{db_state::DbState, handlers::{self, atomics::CreateAtomicPayload}};

pub async fn net_loop(socket: Arc<UdpSocket>, db_state: Arc<DbState>) {
    loop {
        let mut buf = [0; 256];
        let socket = socket.clone();
        let (_, addr) = socket.recv_from(&mut buf).await.unwrap();
        let db_state_async = db_state.clone();

        tokio::spawn(async move {

            let cmd = match String::from_utf8(buf[0..9].to_vec()) {
                Ok(s) => s,
                Err(_) => return
            };
            let callback_key = &buf[9..18];
            let data = &buf[18..256];
    
            let (_, response) = match cmd.trim() {
                "CREATE" => {
                    let id = parse_num::<i32>(&data[0..8]).unwrap();
                    let min_value = parse_num::<i32>(&data[8..18]).unwrap();
                    let log_size = parse_num::<usize>(&data[18..28]).unwrap();
                    handlers::atomics::create_atomic(State(db_state_async.clone()), Json(CreateAtomicPayload {
                        id,
                        min_value,
                        log_size
                    })).await
                },
                "MUTATE" => {
                    let atomic_id = parse_num::<i32>(&data[0..8]).unwrap();
                    let value = parse_num::<i32>(&data[8..18]).unwrap();
                    let p = &data[18..238];
                    let mut payload = vec![];
                    for c in p {
                        if *c == 0 { break }
                        payload.push(*c);
                    }
                    let payload = Bytes::copy_from_slice(&payload);
                    handlers::atomics::mutate_atomic(State(db_state_async.clone()), Path((atomic_id, value)), payload).await
                },
                "GET" => {
                    let atomic_id = parse_num::<i32>(&data[0..8]).unwrap();
                    handlers::atomics::get_atomic(State(db_state_async.clone()), Path(atomic_id)).await
                },
                _ => (StatusCode::INTERNAL_SERVER_ERROR, String::new())
            };
            
            socket.send_to(&[callback_key, response.as_bytes()].concat(), addr).await.unwrap();
        });

    }
}

fn parse_num<T: FromStr>(slice: &[u8]) -> Result<T, <T as FromStr>::Err> {
    String::from_utf8(slice.to_vec()).unwrap().trim().parse::<T>()
}