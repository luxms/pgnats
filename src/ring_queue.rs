use pgrx::PGRXSharedMemory;

const HEADER_SIZE: usize = size_of::<usize>();

#[repr(C)]
pub struct RingQueue<const CAPACITY: usize> {
    read: usize,
    write: usize,
    is_full: bool,
    buffer: [u8; CAPACITY],
}

impl<const CAPACITY: usize> Default for RingQueue<CAPACITY> {
    fn default() -> Self {
        Self {
            read: 0,
            write: 0,
            is_full: false,
            buffer: [0; CAPACITY],
        }
    }
}

impl<const CAPACITY: usize> RingQueue<CAPACITY> {
    pub fn try_send(&mut self, msg: &[u8]) -> Result<(), ()> {
        if self.is_full {
            return Err(());
        }

        let msg_len = msg.len();
        if msg_len > (CAPACITY - HEADER_SIZE) {
            return Err(());
        }

        // | |<--------total len-------->|<----------free---------->|        |
        // │ |<---header--->|<---data--->|<----------free---------->|        │
        // | read                        write                      capacity |
        let total_len = HEADER_SIZE + msg_len;
        let read = self.read;
        let write = self.write;

        if write >= read {
            // │ |<---data--->|<-------free------>|        │
            // | read         write               capacity |

            let free = CAPACITY - (write - read);
            if free < total_len {
                return Err(());
            }

            let header = write + HEADER_SIZE;
            let header_data = msg_len.to_le_bytes();

            let end_space = CAPACITY - write;
            if end_space >= HEADER_SIZE {
                self.buffer[write..header].copy_from_slice(&header_data);
            } else {
                // wrap header
                self.buffer[write..].copy_from_slice(&header_data[..end_space]);
                self.buffer[..HEADER_SIZE - end_space].copy_from_slice(&header_data[end_space..]);
            }

            let header = header % CAPACITY;
            let end_space = CAPACITY - header;
            if end_space >= msg_len {
                self.buffer[header..header + msg_len].copy_from_slice(msg);
            } else {
                // wrap data
                self.buffer[header..].copy_from_slice(&msg[..end_space]);
                self.buffer[..msg_len - end_space].copy_from_slice(&msg[end_space..]);
            }
        } else {
            // │ |<---data--->|<---free--->|<---data--->|        │
            // |              write        read         capacity |

            let free = read - write;
            if free < total_len {
                return Err(());
            }

            // continuous memory
            let header = write + HEADER_SIZE;
            let header_data = msg_len.to_le_bytes();

            self.buffer[write..header].copy_from_slice(&header_data);
            self.buffer[header..header + msg.len()].copy_from_slice(msg);
        }

        self.write = (write + total_len) % CAPACITY;
        self.is_full = self.write == self.read;

        Ok(())
    }

    pub fn try_recv(&mut self) -> Option<Vec<u8>> {
        let read = self.read;
        let write = self.write;
        let header = read + HEADER_SIZE;

        if read == write && !self.is_full {
            return None;
        }

        if write >= read {
            let mut len_bytes = [0u8; HEADER_SIZE];
            len_bytes.copy_from_slice(&self.buffer[read..header]);
            let msg_len = usize::from_le_bytes(len_bytes);

            if !self.is_full && write - read < HEADER_SIZE + msg_len {
                // corrupted data?
                return None;
            }

            // continuous memory
            let mut msg = vec![0u8; msg_len];
            msg.copy_from_slice(&self.buffer[header..header + msg_len]);

            self.read = header + msg_len;
            self.is_full = false;

            Some(msg)
        } else {
            let mut len_bytes = [0u8; HEADER_SIZE];
            let end_space = CAPACITY - read;

            if HEADER_SIZE <= end_space {
                len_bytes.copy_from_slice(&self.buffer[read..header]);
            } else {
                // read wrapped header
                len_bytes[..end_space].copy_from_slice(&self.buffer[read..]);
                len_bytes[end_space..].copy_from_slice(&self.buffer[..HEADER_SIZE - end_space]);
            }

            let msg_len = usize::from_le_bytes(len_bytes);

            if !self.is_full && CAPACITY - (read - write) < HEADER_SIZE + msg_len {
                // courrupted data?
                return None;
            }

            let header = header % CAPACITY;
            let mut msg = vec![0u8; msg_len];
            let end_space = CAPACITY - header;
            if msg_len <= end_space {
                msg.copy_from_slice(&self.buffer[header..header + msg_len]);
            } else {
                // read wrapped data
                msg[..end_space].copy_from_slice(&self.buffer[header..]);
                msg[end_space..].copy_from_slice(&self.buffer[..msg_len - end_space]);
            }

            self.read = (read + HEADER_SIZE + msg_len) % CAPACITY;
            self.is_full = false;

            Some(msg)
        }
    }
}

unsafe impl<const CAPACITY: usize> PGRXSharedMemory for RingQueue<CAPACITY> {}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CAPACITY: usize = 64;

    #[test]
    fn test_send_and_recv_single_message() {
        let mut queue = RingQueue::<TEST_CAPACITY>::default();
        let msg = b"hello world";
        assert!(queue.try_send(msg).is_ok());
        assert_eq!(queue.try_recv().unwrap(), msg.as_slice());
        assert!(queue.try_recv().is_none());
    }

    #[test]
    fn test_queue_full() {
        let mut queue = RingQueue::<TEST_CAPACITY>::default();
        let msg = [1u8; 10];
        let mut count = 0;
        while queue.try_send(&msg).is_ok() {
            count += 1;
        }
        assert_eq!(count, TEST_CAPACITY / (10 + HEADER_SIZE));
        assert!(queue.try_send(&msg).is_err());
        assert!(queue.try_recv().is_some());
        assert!(queue.try_send(&msg).is_ok());
    }

    #[test]
    fn test_empty_queue() {
        let mut queue = RingQueue::<TEST_CAPACITY>::default();
        assert!(queue.try_recv().is_none());
    }

    #[test]
    fn test_wrap_around() {
        let mut queue = RingQueue::<TEST_CAPACITY>::default();
        let msg1 = [1u8; 20];
        let msg2 = [2u8; 20];
        let msg3 = [3u8; 20];

        assert!(queue.try_send(&msg1).is_ok());
        assert!(queue.try_send(&msg2).is_ok());

        assert_eq!(queue.try_recv().unwrap(), msg1);

        assert!(queue.try_send(&msg3).is_ok());

        assert_eq!(queue.try_recv().unwrap(), msg2);
        assert_eq!(queue.try_recv().unwrap(), msg3);
        assert!(queue.try_recv().is_none());
    }

    #[test]
    fn test_variable_message_sizes() {
        let mut queue = RingQueue::<TEST_CAPACITY>::default();
        let msg1 = [1u8; 5];
        let msg2 = [2u8; 10];
        let msg3 = [3u8; 15];
        assert!(queue.try_send(&msg1).is_ok());
        assert!(queue.try_send(&msg2).is_ok());
        assert!(queue.try_send(&msg3).is_ok());
        assert_eq!(queue.try_recv().unwrap(), msg1);
        assert_eq!(queue.try_recv().unwrap(), msg2);
        assert_eq!(queue.try_recv().unwrap(), msg3);
        assert!(queue.try_recv().is_none());
    }

    #[test]
    fn test_capacity_overflow_and_recovery() {
        let mut queue = RingQueue::<TEST_CAPACITY>::default();

        let msg = [42u8; 10];
        let max_msgs = TEST_CAPACITY / (HEADER_SIZE + msg.len());
        let mut sent = 0;
        while queue.try_send(&msg).is_ok() {
            sent += 1;
        }
        assert!(
            sent == max_msgs || sent == max_msgs - 1,
            "Sent: {} (expected ~{})",
            sent,
            max_msgs
        );
        assert!(queue.try_send(&msg).is_err());
        assert!(queue.try_recv().is_some());
        assert!(queue.try_send(&msg).is_ok());
    }

    #[test]
    fn test_wrap_around_capacity() {
        let mut queue = RingQueue::<TEST_CAPACITY>::default();
        let msg = [7u8; 10];
        let max_msgs = TEST_CAPACITY / (HEADER_SIZE + msg.len());

        for _ in 0..3 {
            let mut sent = 0;
            while queue.try_send(&msg).is_ok() {
                sent += 1;
            }
            assert!(sent == max_msgs || sent == max_msgs - 1);
            let mut recvd = 0;
            while queue.try_recv().is_some() {
                recvd += 1;
            }
            assert_eq!(sent, recvd);
        }

        assert!(queue.try_send(&msg).is_ok());
        assert_eq!(queue.try_recv().unwrap(), msg);
    }

    #[test]
    fn test_fill_queue_exactly_and_recv_all() {
        const COUNT: usize = 4;
        let mut queue = RingQueue::<{ 2 * HEADER_SIZE * COUNT }>::default();
        let msg = [0xABu8; HEADER_SIZE];
        let max_msgs = 2 * HEADER_SIZE * COUNT / (HEADER_SIZE + msg.len());
        let mut sent = 0;
        for _ in 0..max_msgs {
            assert!(queue.try_send(&msg).is_ok());
            sent += 1;
        }
        assert_eq!(sent, COUNT);
        assert!(queue.try_send(&msg).is_err());
        for _ in 0..sent {
            assert_eq!(queue.try_recv().unwrap(), msg);
        }
        assert!(queue.try_recv().is_none());
    }

    #[test]
    fn test_message_split_across_wrap() {
        const BUF_SIZE: usize = HEADER_SIZE + 2 + HEADER_SIZE / 2;
        let mut queue = RingQueue::<BUF_SIZE>::default();
        let msg1 = [0x11u8; 2];
        let msg2 = [0x22u8; 2];

        assert!(queue.try_send(&msg1).is_ok());
        assert_eq!(queue.try_recv().unwrap(), msg1);
        assert!(queue.try_send(&msg2).is_ok());
        assert_eq!(queue.try_recv().unwrap(), msg2);
        assert!(queue.try_recv().is_none());
    }
}
