use bincode::{Decode, Encode};

use crate::config::Config;

#[derive(Encode, Decode)]
pub enum SubscriberMessage {
    NewConfig { config: Config },
    Subscribe { subject: String, fn_name: String },
    Unsubscribe { subject: String, fn_name: String },
}
