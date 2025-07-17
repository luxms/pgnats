use std::sync::{
    mpsc::{channel, Sender},
    Arc,
};

use crate::{
    bgw::{
        context::{InternalWorkerMessage, WorkerContext},
        SharedQueue, Worker,
    },
    config::fetch_connection_options,
    debug, log,
    shared::WorkerMessage,
    warn,
};

pub fn run_worker<const N: usize, W: Worker, L: SharedQueue<N>>(
    worker: W,
    shared_queue: &L,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    log!("Tokio runtime initialized.");

    let (msg_sender, msg_receiver) = channel();
    let mut ctx = WorkerContext::new(rt, msg_sender.clone(), worker);

    if ctx.is_master() {
        let opt = ctx.worker().transaction(fetch_connection_options);
        if let Err(error) = ctx.restore_state(opt) {
            warn!("Error restoring state: {}", error);
        }
    }

    while ctx.worker().wait(std::time::Duration::from_secs(1)) {
        ctx.check_migration();
        process_shared_queue(shared_queue, &msg_sender, &mut ctx)?;

        while let Ok(message) = msg_receiver.try_recv() {
            if ctx.is_slave() {
                debug!("Received internal message on replica. Ignoring.");
                continue;
            }

            ctx.handle_internal_message(message);
        }
    }

    Ok(())
}

fn process_shared_queue<const N: usize, Q: SharedQueue<N>, T: Worker>(
    queue: &Q,
    sender: &Sender<InternalWorkerMessage>,
    ctx: &mut WorkerContext<T>,
) -> anyhow::Result<()> {
    let mut queue = queue.unique();

    while let Some(buf) = queue.try_recv() {
        debug!(
            "Received message from shared queue: {:?}",
            String::from_utf8_lossy(&buf)
        );

        let parse_result: Result<(WorkerMessage, _), _> =
            bincode::decode_from_slice(&buf[..], bincode::config::standard());
        let msg = match parse_result {
            Ok((msg, _)) => msg,
            Err(err) => {
                warn!("Failed to decode worker message: {}", err);
                continue;
            }
        };

        match msg {
            WorkerMessage::Subscribe { subject, fn_name } => {
                debug!(
                    "Handling Subscribe for subject '{}', fn '{}'",
                    subject, fn_name
                );

                sender.send(InternalWorkerMessage::Subscribe {
                    register: true,
                    subject: subject.to_string(),
                    fn_name: fn_name.to_string(),
                })?;
            }
            WorkerMessage::Unsubscribe { subject, fn_name } => {
                log!(
                    "Handling Unsubscribe for subject '{}', fn '{}'",
                    subject,
                    fn_name
                );

                sender.send(InternalWorkerMessage::Unsubscribe {
                    reason: None,
                    subject: Arc::from(subject.as_str()),
                    fn_name: Arc::from(fn_name.as_str()),
                })?;
            }
            WorkerMessage::NewConnectionConfig(opt) => {
                log!("Handling NewConnectionConfig update");

                if let Err(err) = ctx.restore_state(opt) {
                    log!("Error during restoring state: {}", err);
                }
            }
        }
    }

    Ok(())
}
