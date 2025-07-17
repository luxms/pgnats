use std::sync::{mpsc::channel, Arc};

use crate::{
    bgw::{
        context::{InternalWorkerMessage, WorkerContext},
        SharedQueue, Worker, WorkerState,
    },
    config::fetch_connection_options,
    log,
    shared::WorkerMessage,
};

pub fn run_worker<const N: usize, W: Worker, L: SharedQueue<N>>(
    worker: W,
    shared_queue: &L,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    log!("Tokio runtime initialized");

    let (msg_sender, msg_receiver) = channel();
    let mut ctx = WorkerContext::new(rt, msg_sender.clone(), worker.fetch_state());

    if ctx.is_master() {
        let opt = worker.transaction(fetch_connection_options);
        if let Err(error) = ctx.restore_state(opt, &worker) {
            log!("Got error while resotring state: {}", error);
        }
    }

    while worker.wait(std::time::Duration::from_secs(1)) {
        let state = worker.fetch_state();

        match (state, ctx.state()) {
            (WorkerState::Master, WorkerState::Slave) => {
                ctx.clear_state();
            }
            (WorkerState::Slave, WorkerState::Master) => {
                let opt = worker.transaction(fetch_connection_options);
                if let Err(error) = ctx.restore_state(opt, &worker) {
                    log!("Got error while resotring state: {}", error);
                }
            }
            _ => {}
        }

        {
            let mut shared_queue = shared_queue.unique();

            while let Some(buf) = shared_queue.try_recv() {
                log!(
                    "Got msg from shared queue: {:?}",
                    String::from_utf8_lossy(&buf)
                );

                let parse_result: Result<(WorkerMessage, _), _> =
                    bincode::decode_from_slice(&buf[..], bincode::config::standard());
                let msg = match parse_result {
                    Ok((msg, _)) => msg,
                    Err(_) => {
                        continue;
                    }
                };

                match msg {
                    WorkerMessage::Subscribe { subject, fn_name } => {
                        msg_sender.send(InternalWorkerMessage::Subscribe {
                            register: true,
                            subject: subject.to_string(),
                            fn_name: fn_name.to_string(),
                        })?;
                    }
                    WorkerMessage::Unsubscribe { subject, fn_name } => {
                        msg_sender.send(InternalWorkerMessage::Unsubscribe {
                            subject: Arc::from(subject.as_str()),
                            fn_name: Arc::from(fn_name.as_str()),
                        })?;
                    }
                    WorkerMessage::NewConnectionConfig(opt) => {
                        if let Err(err) = ctx.restore_state(opt, &worker) {
                            log!("Got reconnection error: {}", err);
                        }
                    }
                }
            }
        }

        while let Ok(message) = msg_receiver.try_recv() {
            if state == WorkerState::Slave {
                continue;
            }

            match message {
                InternalWorkerMessage::Subscribe {
                    register,
                    subject,
                    fn_name,
                } => {
                    log!(
                        "Received subscription: subject='{}', fn='{}'",
                        subject,
                        fn_name
                    );

                    if register {
                        if let Err(error) = worker.insert_subject_callback(&subject, &fn_name) {
                            log!(
                                "Got an error while subscribing from subject '{}' and callback '{}': {}",
                                subject,
                                fn_name,
                                error,
                            );
                        }
                    }

                    ctx.handle_subscribe(Arc::from(subject), Arc::from(fn_name));
                }
                InternalWorkerMessage::Unsubscribe { subject, fn_name } => {
                    log!(
                        "Received unsubscription: subject='{}', fn='{}'",
                        subject,
                        fn_name
                    );

                    if let Err(error) = worker.delete_subject_callback(&subject, &fn_name) {
                        log!(
                            "Got an error while unsubscribing from subject '{}' and callback '{}': {}",
                            subject,
                            fn_name,
                            error,
                        );
                    }

                    ctx.handle_unsubscribe(subject.clone(), &fn_name);
                }
                InternalWorkerMessage::CallbackCall { subject, data } => {
                    log!("Received callback for subject '{}", subject,);
                    ctx.handle_callback(&subject, data, |callback, data| {
                        if let Err(err) = worker.call_function(callback, data) {
                            log!("Error in SPI call '{}': {:?}", callback, err);
                        }
                    });
                }
            }
        }
    }

    Ok(())
}
