use pgrx::pg_sys as sys;

use std::{
    ffi,
    ptr::{NonNull, null_mut},
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
        let mq = unsafe { sys::shm_mq_create(dsm.addr(), size) };
        Self::new_internal(mq, dsm)
    }

    pub fn attach(dsm: &DynamicSharedMemory) -> anyhow::Result<Self> {
        let mq = dsm.addr() as *mut sys::shm_mq;
        Self::new_internal(mq, dsm)
    }

    fn new_internal(mq: *mut sys::shm_mq, dsm: &DynamicSharedMemory) -> anyhow::Result<Self> {
        unsafe { sys::shm_mq_set_sender(mq, sys::MyProc) };
        let mqh = unsafe { sys::shm_mq_attach(mq, dsm.as_ptr(), null_mut()) };

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
        let res = unsafe {
            #[cfg(feature = "pg14")]
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
        let mq = unsafe { sys::shm_mq_create(dsm.addr(), size) };
        Self::new_internal(mq, dsm)
    }

    pub fn attach(dsm: &DynamicSharedMemory) -> anyhow::Result<Self> {
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
        unsafe { sys::shm_mq_set_receiver(mq, sys::MyProc) };
        let mqh = unsafe { sys::shm_mq_attach(mq, dsm.as_ptr(), null_mut()) };

        NonNull::new(mq)
            .and_then(|mq| NonNull::new(mqh).map(|mqh| (mq, mqh)))
            .map(|(mq, mqh)| Self { mq, mqh })
            .ok_or_else(|| anyhow::anyhow!("Failed to create Shared Memory Message Queue"))
    }

    fn recv_internal(&mut self, no_wait: bool) -> anyhow::Result<Option<Vec<u8>>> {
        unsafe {
            let mut ptr: *mut ffi::c_void = null_mut();
            let mut nbytes: usize = 0;

            let res = sys::shm_mq_receive(self.mqh.as_mut(), &mut nbytes, &mut ptr, no_wait);

            match res {
                sys::shm_mq_result::SHM_MQ_SUCCESS => {
                    let slice = std::slice::from_raw_parts(ptr as *const u8, nbytes);
                    Ok(Some(slice.to_vec()))
                }
                sys::shm_mq_result::SHM_MQ_WOULD_BLOCK => Ok(None),
                _ => Err(anyhow::anyhow!("Failed to receive (non-blocking)")),
            }
        }
    }
}
