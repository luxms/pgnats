use pgrx::pg_sys as sys;
use std::ffi::c_void;
use std::ptr::{NonNull, null_mut};

pub struct DsmSegment {
    raw: NonNull<sys::dsm_segment>,
}

impl DsmSegment {
    pub fn create(size: usize) -> anyhow::Result<Self> {
        let seg = unsafe { sys::dsm_create(size, 0) };
        NonNull::new(seg)
            .map(|raw| DsmSegment { raw })
            .ok_or(anyhow::anyhow!("Failed to create DSM"))
    }

    pub fn attach(handle: u32) -> anyhow::Result<Self> {
        let seg = unsafe { sys::dsm_attach(handle) };
        NonNull::new(seg)
            .map(|raw| DsmSegment { raw })
            .ok_or(anyhow::anyhow!("Failed to attach to DSM"))
    }

    pub fn handle(&self) -> u32 {
        unsafe { sys::dsm_segment_handle(self.raw.as_ptr()) }
    }

    pub fn addr(&self) -> *mut c_void {
        unsafe { sys::dsm_segment_address(self.raw.as_ptr()) }
    }

    pub fn as_ptr(&self) -> *mut sys::dsm_segment {
        self.raw.as_ptr()
    }
}

impl Drop for DsmSegment {
    fn drop(&mut self) {
        //unsafe { sys::dsm_detach(self.raw.as_ptr()) };
    }
}

pub struct MessageQueue {
    mq: NonNull<sys::shm_mq>,
    mqh: NonNull<sys::shm_mq_handle>,
}

impl MessageQueue {
    pub fn create_in(dsm: &DsmSegment, size: usize) -> Self {
        let mq = unsafe { sys::shm_mq_create(dsm.addr(), size) };
        unsafe { sys::shm_mq_set_sender(mq, sys::MyProc) };

        let mqh = unsafe { sys::shm_mq_attach(mq, dsm.as_ptr(), null_mut()) };

        MessageQueue {
            mq: NonNull::new(mq).expect("mq is null"),
            mqh: NonNull::new(mqh).expect("mqh is null"),
        }
    }

    pub fn open_in(seg: &DsmSegment) -> Self {
        let mq = seg.addr() as *mut sys::shm_mq;
        unsafe { sys::shm_mq_set_receiver(mq, sys::MyProc) };

        let mqh = unsafe { sys::shm_mq_attach(mq, seg.as_ptr(), null_mut()) };

        MessageQueue {
            mq: NonNull::new(mq).expect("mq is null"),
            mqh: NonNull::new(mqh).expect("mqh is null"),
        }
    }

    pub fn set_receiver(&mut self) {
        unsafe { sys::shm_mq_set_receiver(self.mq.as_ptr(), sys::MyProc) };
    }

    pub fn send(&mut self, data: &[u8]) -> anyhow::Result<()> {
        let res = unsafe {
            sys::shm_mq_send(
                self.mqh.as_mut(),
                data.len(),
                data.as_ptr() as *const _,
                false,
            )
        };
        match res {
            sys::shm_mq_result_SHM_MQ_SUCCESS => Ok(()),
            _ => Err(anyhow::anyhow!("Failed to send")),
        }
    }

    pub fn receive_blocking(&mut self) -> anyhow::Result<Vec<u8>> {
        unsafe {
            let mut ptr: *mut c_void = null_mut();
            let mut nbytes: usize = 0;
            let res = sys::shm_mq_receive(self.mqh.as_mut(), &mut nbytes, &mut ptr, false);
            if res == sys::shm_mq_result_SHM_MQ_SUCCESS {
                Ok(std::slice::from_raw_parts(ptr as *const u8, nbytes).to_vec())
            } else {
                Err(anyhow::anyhow!("Failed to recv"))
            }
        }
    }
}
