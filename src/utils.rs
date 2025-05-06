use crate::errors::PgNatsError;

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

impl FromBytes for pgrx::Json {
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, PgNatsError> {
        Ok(Self(serde_json::Value::from_bytes(bytes)?))
    }
}

impl FromBytes for pgrx::JsonB {
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, PgNatsError> {
        Ok(Self(serde_json::Value::from_bytes(bytes)?))
    }
}

impl FromBytes for String {
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, PgNatsError> {
        String::from_utf8(bytes).map_err(|e| PgNatsError::Deserialize(e.to_string()))
    }
}

pub trait ToBytes: Sized {
    fn to_bytes(self) -> Result<Vec<u8>, PgNatsError>;
}

impl ToBytes for Vec<u8> {
    fn to_bytes(self) -> Result<Vec<u8>, PgNatsError> {
        Ok(self)
    }
}

impl ToBytes for serde_json::Value {
    fn to_bytes(self) -> Result<Vec<u8>, PgNatsError> {
        serde_json::to_vec(&self).map_err(PgNatsError::Serialize)
    }
}

impl ToBytes for String {
    fn to_bytes(self) -> Result<Vec<u8>, PgNatsError> {
        Ok(self.into_bytes())
    }
}

impl ToBytes for &str {
    fn to_bytes(self) -> Result<Vec<u8>, PgNatsError> {
        Ok(self.as_bytes().to_vec())
    }
}

impl ToBytes for pgrx::Json {
    fn to_bytes(self) -> Result<Vec<u8>, PgNatsError> {
        self.0.to_bytes()
    }
}

impl ToBytes for pgrx::JsonB {
    fn to_bytes(self) -> Result<Vec<u8>, PgNatsError> {
        self.0.to_bytes()
    }
}
