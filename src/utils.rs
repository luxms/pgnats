use regex::Regex;
use std::sync::LazyLock;

use crate::errors::PgNatsError;

static REGEX_STREAM_NAME_LAST_PART: LazyLock<Regex> =
  LazyLock::new(|| Regex::new(r"\.[^.]*$").expect("Wrong regex"));

static REGEX_SPECIAL_SYM: LazyLock<Regex> =
  LazyLock::new(|| Regex::new(r"[.^?>*]").expect("Wrong regex"));

pub fn format_message(message_text: impl AsRef<str>) -> String {
  format!("PGNATS: {}", message_text.as_ref())
}

pub fn do_panic_with_message(message_text: impl AsRef<str>) -> ! {
  panic!("PGNATS: {}", message_text.as_ref())
}

pub fn get_stream_name_by_subject(subject: &str) -> String {
  REGEX_SPECIAL_SYM
    .replace_all(
      REGEX_STREAM_NAME_LAST_PART.replace(subject, "").as_ref(),
      "_",
    )
    .to_string()
}

pub trait FromBytes: Sized {
  fn from_bytes(bytes: Vec<u8>) -> Result<Self, PgNatsError>;
}

impl FromBytes for Vec<u8> {
  fn from_bytes(bytes: Vec<u8>) -> Result<Self, PgNatsError> {
    Ok(bytes)
  }
}

impl FromBytes for serde_json::Value {
  fn from_bytes(bytes: Vec<u8>) -> Result<Self, PgNatsError> {
    let string = String::from_bytes(bytes)?;

    serde_json::from_str(&string).map_err(|e| PgNatsError::Deserialize(e.to_string()))
  }
}

impl FromBytes for String {
  fn from_bytes(bytes: Vec<u8>) -> Result<Self, PgNatsError> {
    String::from_utf8(bytes).map_err(|e| PgNatsError::Deserialize(e.to_string()))
  }
}
