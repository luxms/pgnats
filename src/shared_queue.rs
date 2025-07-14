use pgrx::PGRXSharedMemory;

const PTR_SIZE: usize = size_of::<usize>();

#[repr(C)]
pub struct SharedRingQueue<const CAPACITY: usize> {
    head: usize,
    tail: usize,
    buffer: [u8; CAPACITY],
}

impl<const CAPACITY: usize> Default for SharedRingQueue<CAPACITY> {
    fn default() -> Self {
        Self {
            head: 0,
            tail: 0,
            buffer: [0; CAPACITY],
        }
    }
}

impl<const CAPACITY: usize> SharedRingQueue<CAPACITY> {
    fn mask(idx: usize) -> usize {
        idx % CAPACITY
    }

    pub fn try_send(&mut self, msg: &[u8]) -> Result<(), ()> {
        let msg_len = msg.len();
        if msg_len > (CAPACITY - PTR_SIZE) {
            return Err(());
        }

        let total_len = PTR_SIZE + msg_len;
        let head = self.head;
        let tail = self.tail;

        let used = if tail >= head {
            tail - head
        } else {
            CAPACITY - (head - tail)
        };

        let free = CAPACITY - used;
        if free < total_len {
            return Err(());
        }
        let mut pos = tail;

        for b in msg_len.to_le_bytes() {
            self.buffer[Self::mask(pos)] = b;
            pos += 1;
        }

        for b in msg {
            self.buffer[Self::mask(pos)] = *b;
            pos += 1;
        }
        self.tail = Self::mask(pos);
        Ok(())
    }

    pub fn try_recv(&mut self) -> Option<Vec<u8>> {
        let head = self.head;
        let tail = self.tail;
        if head == tail {
            return None;
        }

        let mut len_bytes = [0u8; PTR_SIZE];
        let mut pos = head;
        for b in &mut len_bytes {
            *b = self.buffer[Self::mask(pos)];
            pos += 1;
        }
        let msg_len = usize::from_le_bytes(len_bytes);
        if msg_len > (CAPACITY - PTR_SIZE) {
            return None;
        }

        let used = if tail >= head {
            tail - head
        } else {
            CAPACITY - (head - tail)
        };
        if used < PTR_SIZE + msg_len {
            return None;
        }

        let mut msg = Vec::with_capacity(msg_len);
        for _ in 0..msg_len {
            msg.push(self.buffer[Self::mask(pos)]);
            pos += 1;
        }
        self.head = Self::mask(pos);
        Some(msg)
    }
}

unsafe impl<const CAPACITY: usize> PGRXSharedMemory for SharedRingQueue<CAPACITY> {}
