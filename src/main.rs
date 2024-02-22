use std::{env, sync::{atomic::AtomicI32, Arc}};

use axum::{routing::{get, post}, Router};
use db_state::DbState;

mod handlers;
mod db_state;

#[tokio::main]
async fn main() {

    let atomics = scc::HashMap::new();
    let log_files = scc::HashMap::new();

    let db_state = Arc::new(DbState {
        atomics,
        tx_id: AtomicI32::new(1),
        log_files
    });
    
    let app = Router::new()
        .route("/atomics", post(handlers::atomics::create_atomic))
        .route("/atomics/:atomic_id", get(handlers::atomics::get_atomic))
        .route("/atomics/:atomic_id", get(handlers::atomics::get_atomic_logs))
        .route("/atomics/:atomic_id/:value", post(handlers::atomics::mutate_atomic))
        .with_state::<()>(db_state);

    let socket_path = env::var("SOCKET_PATH").
        expect("no SOCKET_PATH env var found");

    match tokio::fs::remove_file(&socket_path).await {
        Err(e) => println!("warn: unable to unlink path {socket_path}: {e}"),
        _ => ()
    };

    let listener = std::os::unix::net::UnixListener::bind(&socket_path)
        .expect(format!("error listening to socket {socket_path}").as_str());
    listener.set_nonblocking(true).unwrap();

    let listener = tokio::net::UnixListener::from_std(listener)
        .expect("error parsing std listener");

    axum::serve(listener, app.into_make_service()).await
        .expect("error serving app");
}
