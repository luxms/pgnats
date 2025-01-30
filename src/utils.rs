use crate::errors::PgNatsError;

pub fn format_message(message_text: impl AsRef<str>) -> String {
  format!("PGNATS: {}", message_text.as_ref())
}

pub fn do_panic_with_message(message_text: impl AsRef<str>) -> ! {
  panic!("PGNATS: {}", message_text.as_ref())
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
