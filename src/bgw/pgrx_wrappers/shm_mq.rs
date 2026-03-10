use pgrx::pg_sys as sys;

use std::{
    ffi,
    ptr::{null_mut, NonNull},
};

use crate::bgw::pgrx_wrappers::dsm::DynamicSharedMemory;

#[allow(dead_code)]
#[derive(Debug)]
pub struct ShmMqSender {
    mq: NonNull<sys::shm_mq>,
    mqh: NonNull<sys::shm_mq_handle>,
}

impl ShmMqSender {
    pub fn new(dsm: &DynamicSharedMemory, size: usize) -> anyhow::Result<Self> {
        // SAFETY:
        // 1. `dsm.addr()` returns a pointer to a valid Postgres DSM segment.
        // 2. `size` specifies the queue capacity and is controlled by the caller.
        // 3. `shm_mq_create` initializes the message queue entirely inside the DSM segment.
        // 4. The returned pointer (if non-null) refers to memory owned by Postgres and
        //    remains valid until the DSM segment is detached.
        let mq = unsafe { sys::shm_mq_create(dsm.addr(), size) };
        Self::new_internal(mq, dsm)
    }

    pub fn attach(dsm: &DynamicSharedMemory) -> anyhow::Result<Self> {
        let mq = dsm.addr() as *mut sys::shm_mq;
        Self::new_internal(mq, dsm)
    }

    fn new_internal(mq: *mut sys::shm_mq, dsm: &DynamicSharedMemory) -> anyhow::Result<Self> {
        // SAFETY:
        // 1. `mq` points to memory inside a valid DSM segment.
        // 2. `shm_mq_set_sender` is called before attaching the handle.
        // 3. Returned handle is checked for null and tied to DSM lifetime.
        let mqh = unsafe {
            sys::shm_mq_set_sender(mq, sys::MyProc);
            sys::shm_mq_attach(mq, dsm.as_ptr(), null_mut())
        };

        NonNull::new(mq)
            .and_then(|mq| NonNull::new(mqh).map(|mqh| (mq, mqh)))
            .map(|(mq, mqh)| Self { mq, mqh })
            .ok_or_else(|| anyhow::anyhow!("Failed to create Shared Memory Message Queue"))
    }

    pub fn send(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.send_internal(data, false).and_then(|success| {
            if success {
                Ok(())
            } else {
                Err(anyhow::anyhow!("Failed to send"))
            }
        })
    }

    pub fn try_send(&mut self, data: &[u8]) -> anyhow::Result<bool> {
        self.send_internal(data, true)
    }

    fn send_internal(&mut self, data: &[u8], no_wait: bool) -> anyhow::Result<bool> {
        // SAFETY:
        // 1. `self.mqh` is a valid shm_mq_handle obtained via Postgres API.
        // 2. `data.as_ptr()` and `data.len()` describe a valid memory region
        //    that lives for the duration of the call.
        // 3. Postgres does not retain the pointer after returning.
        let res = unsafe {
            #[cfg(any(feature = "pg14"))]
            let res = sys::shm_mq_send(
                self.mqh.as_mut(),
                data.len(),
                data.as_ptr() as *const _,
                no_wait,
            );

            #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17", feature = "pg18"))]
            let res = sys::shm_mq_send(
                self.mqh.as_mut(),
                data.len(),
                data.as_ptr() as *const _,
                no_wait,
                true,
            );

            res
        };

        match res {
            sys::shm_mq_result::SHM_MQ_SUCCESS => Ok(true),
            sys::shm_mq_result::SHM_MQ_WOULD_BLOCK => Ok(false),
            _ => Err(anyhow::anyhow!("Failed to send")),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ShmMqReceiver {
    mq: NonNull<sys::shm_mq>,
    mqh: NonNull<sys::shm_mq_handle>,
}

impl ShmMqReceiver {
    pub fn new(dsm: &DynamicSharedMemory, size: usize) -> anyhow::Result<Self> {
        // SAFETY:
        // 1. `dsm.addr()` returns a pointer to a valid DSM segment.
        // 2. `size` is controlled by the caller and defines the queue capacity.
        // 3. Postgres guarantees that the created shm_mq resides fully inside
        //    the provided DSM segment and remains valid until the segment is detached.
        let mq = unsafe { sys::shm_mq_create(dsm.addr(), size) };
        Self::new_internal(mq, dsm)
    }

    pub fn attach(dsm: &DynamicSharedMemory) -> anyhow::Result<Self> {
        // SAFETY:
        // `dsm.addr()` points to a DSM segment that already contains
        // a properly initialized shm_mq structure.
        let mq = dsm.addr() as *mut sys::shm_mq;
        Self::new_internal(mq, dsm)
    }

    pub fn recv(&mut self) -> anyhow::Result<Vec<u8>> {
        self.recv_internal(false)
            .transpose()
            .ok_or_else(|| anyhow::anyhow!("Failed to recv"))?
    }

    pub fn try_recv(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        self.recv_internal(true)
    }

    fn new_internal(mq: *mut sys::shm_mq, dsm: &DynamicSharedMemory) -> anyhow::Result<Self> {
        // SAFETY:
        // 1. `mq` points to a shm_mq structure located inside a valid DSM segment.
        // 2. `MyProc` is initialized for the current backend process.
        // 3. `shm_mq_set_receiver` must be called before `shm_mq_attach`
        //    according to Postgres API contract.
        // 4. Returned handle is checked for null before use.
        let mqh = unsafe {
            sys::shm_mq_set_receiver(mq, sys::MyProc);
            sys::shm_mq_attach(mq, dsm.as_ptr(), null_mut())
        };

        NonNull::new(mq)
            .and_then(|mq| NonNull::new(mqh).map(|mqh| (mq, mqh)))
            .map(|(mq, mqh)| Self { mq, mqh })
            .ok_or_else(|| anyhow::anyhow!("Failed to create Shared Memory Message Queue"))
    }

    fn recv_internal(&mut self, no_wait: bool) -> anyhow::Result<Option<Vec<u8>>> {
        // SAFETY:
        // 1. `self.mqh` is a valid shm_mq_handle obtained via Postgres API.
        // 2. On success, Postgres returns a pointer to a buffer inside DSM which
        //    remains valid until the next receive call.
        // 3. The returned `(ptr, nbytes)` pair describes a valid byte slice.
        // 4. The data is immediately copied into a Rust-owned buffer.
        unsafe {
            let mut ptr: *mut ffi::c_void = null_mut();
            let mut nbytes: usize = 0;

            let res = sys::shm_mq_receive(self.mqh.as_mut(), &mut nbytes, &mut ptr, no_wait);

            match res {
                sys::shm_mq_result::SHM_MQ_SUCCESS => {
                    if nbytes == 0 && ptr.is_null() {
                        return Ok(None);
                    }

                    let slice = std::slice::from_raw_parts(ptr as *const u8, nbytes);
                    Ok(Some(slice.to_vec()))
                }
                sys::shm_mq_result::SHM_MQ_WOULD_BLOCK => Ok(None),
                _ => Err(anyhow::anyhow!("Failed to receive (non-blocking)")),
            }
        }
    }
}
