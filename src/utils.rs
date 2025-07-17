pub trait FromBytes: Sized {
    fn from_bytes(bytes: Vec<u8>) -> anyhow::Result<Self>;
}

impl FromBytes for Vec<u8> {
    fn from_bytes(bytes: Vec<u8>) -> anyhow::Result<Self> {
        Ok(bytes)
    }
}

impl FromBytes for serde_json::Value {
    fn from_bytes(bytes: Vec<u8>) -> anyhow::Result<Self> {
        let string = String::from_bytes(bytes)?;

        Ok(serde_json::from_str(&string)?)
    }
}

impl FromBytes for pgrx::Json {
    fn from_bytes(bytes: Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self(serde_json::Value::from_bytes(bytes)?))
    }
}

impl FromBytes for pgrx::JsonB {
    fn from_bytes(bytes: Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self(serde_json::Value::from_bytes(bytes)?))
    }
}

impl FromBytes for String {
    fn from_bytes(bytes: Vec<u8>) -> anyhow::Result<Self> {
        Ok(String::from_utf8(bytes)?)
    }
}

pub trait ToBytes: Sized {
    fn to_bytes(self) -> anyhow::Result<Vec<u8>>;
}

impl ToBytes for Vec<u8> {
    fn to_bytes(self) -> anyhow::Result<Vec<u8>> {
        Ok(self)
    }
}

impl ToBytes for serde_json::Value {
    fn to_bytes(self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(&self)?)
    }
}

impl ToBytes for String {
    fn to_bytes(self) -> anyhow::Result<Vec<u8>> {
        Ok(self.into_bytes())
    }
}

impl ToBytes for &str {
    fn to_bytes(self) -> anyhow::Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }
}

impl ToBytes for pgrx::Json {
    fn to_bytes(self) -> anyhow::Result<Vec<u8>> {
        self.0.to_bytes()
    }
}

impl ToBytes for pgrx::JsonB {
    fn to_bytes(self) -> anyhow::Result<Vec<u8>> {
        self.0.to_bytes()
    }
}

pub(crate) fn extract_headers(v: serde_json::Value) -> async_nats::HeaderMap {
    let mut map = async_nats::HeaderMap::new();

    if let Some(obj) = v.as_object() {
        for (k, v) in obj {
            if let Some(v) = v.as_str() {
                map.append(k.as_str(), v);
            }
        }
    }

    map
}
