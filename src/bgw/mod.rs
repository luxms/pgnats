use std::ops::{Deref, DerefMut};

use crate::{
    config::Config,
    notification::{PgInstanceNotification, PgInstanceTransition},
    ring_queue::RingQueue,
};

mod context;

pub mod export;
pub mod native;
pub mod posgres;
pub mod run;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WorkerState {
    Master,
    Slave,
}

pub trait Worker {
    fn wait(&self, duration: std::time::Duration) -> bool;

    fn fetch_state(&self) -> WorkerState;
    fn fetch_config(&self) -> Config;

    fn fetch_subject_with_callbacks(&self) -> anyhow::Result<Vec<(String, String)>>;
    fn insert_subject_callback(&self, subject: &str, callback: &str) -> anyhow::Result<()>;
    fn delete_subject_callback(&self, subject: &str, callback: &str) -> anyhow::Result<()>;
    fn call_function(&self, callback: &str, data: &[u8]) -> anyhow::Result<()>;

    fn make_notification(
        &self,
        transition: PgInstanceTransition,
        patroni_url: Option<&str>,
    ) -> Option<PgInstanceNotification>;
}

pub trait SharedQueue<const N: usize> {
    type Unqiue<'a>: Deref<Target = RingQueue<N>> + DerefMut
    where
        Self: 'a;

    type Shared<'a>: Deref<Target = RingQueue<N>>
    where
        Self: 'a;

    fn shared(&self) -> Self::Shared<'_>;
    fn unique(&self) -> Self::Unqiue<'_>;
}
