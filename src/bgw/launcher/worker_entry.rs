use pgrx::{
    IntoDatum,
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, DynamicBackgroundWorker},
    pg_sys as sys,
};

use crate::{
    bgw::pgrx_wrappers::{dsm::DynamicSharedMemory, shm_mq::ShmMqSender},
    constants::EXTENSION_NAME,
    utils::{get_database_name, pack_oid_dsmh_to_i64},
};

const APPROX_SHM_HEADER_SIZE: usize = 64;

pub struct WorkerEntry {
    pub oid: sys::Oid,
    pub db_name: String,
    pub worker: DynamicBackgroundWorker,
    pub dsm: DynamicSharedMemory,
    pub sender: ShmMqSender,
}

impl WorkerEntry {
    pub fn start(
        oid: sys::Oid,
        name: &str,
        ty: &str,
        entrypoint: &str,
        shm_size: usize,
    ) -> anyhow::Result<Self> {
        let db_name = BackgroundWorker::transaction(|| get_database_name(oid))
            .ok_or_else(|| anyhow::anyhow!("Failed to get database name for {} oid", oid))?;

        let size = shm_size.min(unsafe { sys::shm_mq_minimum_size });
        let dsm = DynamicSharedMemory::new(size + APPROX_SHM_HEADER_SIZE)?;

        let packed_arg = pack_oid_dsmh_to_i64(oid, dsm.handle());

        let sender = ShmMqSender::new(&dsm, size)?;

        let worker = BackgroundWorkerBuilder::new(name)
            .set_type(ty)
            .enable_spi_access()
            .set_library(EXTENSION_NAME)
            .set_function(entrypoint)
            .set_argument(packed_arg.into_datum())
            .set_notify_pid(unsafe { sys::MyProcPid })
            .load_dynamic()
            .map_err(|err| anyhow::anyhow!("Failed to start worker. Reason: {:?}", err))?;

        Ok(Self {
            oid,
            db_name,
            worker,
            dsm,
            sender,
        })
    }
}
