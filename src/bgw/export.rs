use crate::{
    bgw::{posgres::PostgresWorker, run::run_worker},
    error, log,
    shared::WORKER_MESSAGE_QUEUE,
};

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_subscriber(_arg: pgrx::pg_sys::Datum) {
    log!("Starting background worker: subscriber");

    let worker = match PostgresWorker::new() {
        Ok(worker) => worker,
        Err(err) => {
            error!("Failed to initialize subscriber worker: {}", err);
            return;
        }
    };

    log!(
        "Subscriber worker connected to database '{}'",
        worker.connected_db_name()
    );

    if let Err(err) = run_worker(worker, &WORKER_MESSAGE_QUEUE) {
        error!("Error while running subscriber worker: {}", err);
        return;
    }

    log!("Subscriber worker stopped gracefully");
}
