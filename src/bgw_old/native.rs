use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{bgw::SharedQueue, ring_queue::RingQueue};

impl<const N: usize> SharedQueue<N> for RwLock<RingQueue<N>> {
    type Unqiue<'a> = RwLockWriteGuard<'a, RingQueue<N>>;
    type Shared<'a> = RwLockReadGuard<'a, RingQueue<N>>;

    fn shared(&self) -> Self::Shared<'_> {
        self.read().expect("Failed to lock")
    }

    fn unique(&self) -> Self::Unqiue<'_> {
        self.write().expect("Failed to lock")
    }
}
