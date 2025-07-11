pub struct SharedQueue {
    pub mq: *mut pgrx::pg_sys::shm_mq,
    pub mq_handle: *mut pgrx::pg_sys::shm_mq_handle,
    pub seg: *mut pgrx::pg_sys::dsm_segment,
}

impl SharedQueue {
    pub unsafe fn new(size: usize) -> Result<Self, ()> {
        if size < pgrx::pg_sys::shm_mq_minimum_size {
            return Err(());
        }

        let seg = pgrx::pg_sys::dsm_create(size, 0);
        if seg.is_null() {
            return Err(());
        }
        let address = pgrx::pg_sys::dsm_segment_address(seg);
        let mq = pgrx::pg_sys::shm_mq_create(address, size);
        pgrx::pg_sys::shm_mq_set_sender(mq, pgrx::pg_sys::MyProc);

        let mq_handle = pgrx::pg_sys::shm_mq_attach(mq, seg, std::ptr::null_mut());

        Ok(Self { mq, mq_handle, seg })
    }

    pub unsafe fn from_existing(
        dsm_handle: pgrx::pg_sys::dsm_handle,
        offset: usize,
    ) -> Result<Self, ()> {
        let seg = pgrx::pg_sys::dsm_attach(dsm_handle);
        if seg.is_null() {
            return Err(());
        }

        let base = pgrx::pg_sys::dsm_segment_address(seg);
        let mq = (base as usize + offset) as *mut pgrx::pg_sys::shm_mq;
        let mq_handle = pgrx::pg_sys::shm_mq_attach(mq, seg, std::ptr::null_mut());

        Ok(Self { mq, mq_handle, seg })
    }

    pub unsafe fn send(&self, bytes: &[u8]) -> Result<(), ()> {
        let result =
            pgrx::pg_sys::shm_mq_send(self.mq_handle, bytes.len(), bytes.as_ptr() as _, true);

        if result != pgrx::pg_sys::shm_mq_result::SHM_MQ_SUCCESS {
            return Err(());
        }

        Ok(())
    }

    pub unsafe fn try_recv(&self) -> Result<Option<Vec<u8>>, ()> {
        let mut ptr = std::ptr::null_mut();
        let mut bytes = 0;

        let result = pgrx::pg_sys::shm_mq_receive(self.mq_handle, &mut bytes, &mut ptr, true);

        match result {
            pgrx::pg_sys::shm_mq_result::SHM_MQ_SUCCESS => Ok(Some(
                std::slice::from_raw_parts_mut(ptr as _, bytes).to_vec(),
            )),
            pgrx::pg_sys::shm_mq_result::SHM_MQ_WOULD_BLOCK => Ok(None),
            pgrx::pg_sys::shm_mq_result::SHM_MQ_DETACHED => Err(()),
            _ => {
                todo!()
            }
        }
    }
}

impl Drop for SharedQueue {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::dsm_detach(self.seg);
        }
    }
}
