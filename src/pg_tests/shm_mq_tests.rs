#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
mod tests {
    use pgrx::{
        IntoDatum,
        bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
        datum::FromDatum,
        pg_test,
    };

    use crate::bgw::pgrx_wrappers::{
        dsm::DynamicSharedMemory,
        shm_mq::{ShmMqReceiver, ShmMqSender},
    };

    #[pgrx::pg_guard]
    #[unsafe(no_mangle)]
    pub extern "C-unwind" fn bgw_mock_shared_mem(arg: pgrx::pg_sys::Datum) {
        BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);

        let packed = unsafe { i64::from_polymorphic_datum(arg, false, pgrx::pg_sys::INT8OID) }
            .unwrap() as u64;
        let dsm_worker_handle = ((packed >> 32) & 0xFFFF_FFFF) as u32;
        let dsm_main_handle = (packed & 0xFFFF_FFFF) as u32;

        let dsm_worker = DynamicSharedMemory::attach(dsm_worker_handle.into()).unwrap();
        let mut sender = ShmMqSender::attach(&dsm_worker).unwrap();
        sender.send(b"Hello, MQ!").unwrap();

        let dsm_main = DynamicSharedMemory::attach(dsm_main_handle.into()).unwrap();
        let mut receiver = ShmMqReceiver::attach(&dsm_main).unwrap();
        let ack = receiver.recv().unwrap();

        assert_eq!(ack, b"ACK");
    }

    #[pg_test]
    fn test_shm_mq_send_recv() {
        const CONTENT: &[u8] = b"Hello, MQ!";

        let dsm = DynamicSharedMemory::new(128).unwrap();

        let mut sender = ShmMqSender::new(&dsm, 128).unwrap();
        let mut recv = ShmMqReceiver::attach(&dsm).unwrap();

        let result = sender.try_send(CONTENT).unwrap();
        assert!(result);

        let content = recv.try_recv().unwrap().unwrap();

        assert_eq!(content, CONTENT);
    }

    #[pg_test]
    fn test_shm_mq_send_recv_full_size() {
        const CONTENT: &[u8] = &[0u8; 128];

        let dsm = DynamicSharedMemory::new(128).unwrap();

        let mut sender = ShmMqSender::new(&dsm, 128).unwrap();
        let mut recv = ShmMqReceiver::attach(&dsm).unwrap();

        let not_send = sender.try_send(CONTENT).unwrap();
        assert_eq!(not_send, false);

        let content = recv.try_recv().unwrap();

        assert!(content.is_none());
    }

    #[pg_test]
    fn test_shm_mq_send_recv_with_two_dsm() {
        const CONTENT: &[u8] = b"Hello, MQ!";
        const ACK: &[u8] = b"ACK";

        let dsm_worker = DynamicSharedMemory::new(128).unwrap();
        let mut receiver = ShmMqReceiver::new(&dsm_worker, 128).unwrap();

        let dsm_main = DynamicSharedMemory::new(64).unwrap();
        let mut ack_sender = ShmMqSender::new(&dsm_main, 64).unwrap();

        let packed_arg =
            (((*dsm_worker.handle() as u64) << 32) | (*dsm_main.handle() as u64)) as i64;

        let worker = BackgroundWorkerBuilder::new("test_shm_mq_send_recv_with_two_dsm")
            .set_library("pgnats")
            .set_function("bgw_mock_shared_mem")
            .enable_spi_access()
            .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
            .set_argument(packed_arg.into_datum())
            .load_dynamic()
            .unwrap();

        assert!(worker.wait_for_startup().is_ok());

        let msg = receiver.recv().unwrap();
        assert_eq!(msg, CONTENT);

        ack_sender.send(ACK).unwrap();

        let worker = worker.terminate();
        worker.wait_for_shutdown().unwrap();
    }
}
