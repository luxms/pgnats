use pgrx::{
    IntoDatum,
    bgworkers::{
        BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, DynamicBackgroundWorker,
        TerminatingDynamicBackgroundWorker,
    },
    pg_sys as sys,
};

use crate::{
    bgw::pgrx_wrappers::{dsm::DynamicSharedMemory, shm_mq::ShmMqSender},
    constants::EXTENSION_NAME,
    utils::{get_database_name, pack_oid_dsmh_to_i64},
};

const APPROX_SHM_HEADER_SIZE: usize = 64;

pub struct RunningState(DynamicBackgroundWorker);
pub struct TerminatedState(TerminatingDynamicBackgroundWorker);

pub struct WorkerEntry<S> {
    pub db_name: String,
    pub sender: ShmMqSender,
    pub oid: sys::Oid,
    state: S,
    _dsm: DynamicSharedMemory,
}

impl WorkerEntry<RunningState> {
    pub fn start(
        oid: sys::Oid,
        name: &str,
        ty: &str,
        entrypoint: &str,
        shm_size: usize,
    ) -> anyhow::Result<Self> {
        let db_name = BackgroundWorker::transaction(|| get_database_name(oid))
            .ok_or_else(|| anyhow::anyhow!("Failed to resolve database name for OID {oid}."))?;

        let size = shm_size.min(unsafe { sys::shm_mq_minimum_size });
        let dsm = DynamicSharedMemory::new(size + APPROX_SHM_HEADER_SIZE)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to allocate dynamic shared memory segment (requested size: {}). Reason: {e}",
                    size + APPROX_SHM_HEADER_SIZE
                )
            })?;

        let packed_arg = pack_oid_dsmh_to_i64(oid, dsm.handle());

        let sender = ShmMqSender::new(&dsm, size).map_err(|e| {
            anyhow::anyhow!(
                "Failed to initialize shared memory message queue sender (size: {}). Reason: {e}",
                size
            )
        })?;

        let worker = BackgroundWorkerBuilder::new(name)
            .set_type(ty)
            .enable_spi_access()
            .set_library(EXTENSION_NAME)
            .set_function(entrypoint)
            .set_argument(packed_arg.into_datum())
            .set_start_time(BgWorkerStartTime::ConsistentState)
            .set_notify_pid(unsafe { sys::MyProcPid })
            .load_dynamic()
            .map_err(|err| {
                anyhow::anyhow!(
                    "Failed to launch background worker '{}'. Entry point: '{}', database: '{}'. Error: {err:?}",
                    name,
                    entrypoint,
                    db_name
                )
            })?;

        Ok(Self {
            db_name,
            oid,
            state: RunningState(worker),
            sender,
            _dsm: dsm,
        })
    }

    pub fn terminate(self) -> WorkerEntry<TerminatedState> {
        let terminate = self.state.0.terminate();

        WorkerEntry::<TerminatedState> {
            db_name: self.db_name,
            sender: self.sender,
            oid: self.oid,
            state: TerminatedState(terminate),

            _dsm: self._dsm,
        }
    }
}

impl WorkerEntry<TerminatedState> {
    pub fn wait_for_shutdown(self) -> anyhow::Result<()> {
        self.state.0.wait_for_shutdown().map_err(|err| {
            anyhow::anyhow!(
                "Failed to gracefully shutdown background worker for database '{}': {err:?}",
                self.db_name
            )
        })
    }
}
