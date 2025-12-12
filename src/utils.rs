use std::ffi::CStr;

use crate::bgw::pgrx_wrappers::dsm::DsmHandle;

use anyhow::Ok;
use pgrx::pg_sys as sys;

struct SysHeapTuple {
    inner: *mut sys::HeapTupleData,
}

impl SysHeapTuple {
    pub fn from_raw(tuple: *mut sys::HeapTupleData) -> Option<Self> {
        if tuple.is_null() {
            None
        } else {
            Some(Self { inner: tuple })
        }
    }
}

impl Drop for SysHeapTuple {
    fn drop(&mut self) {
        unsafe {
            sys::ReleaseSysCache(self.inner);
        }
    }
}

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

pub fn pack_oid_dsmh_to_i64(oid: sys::Oid, dsmh: DsmHandle) -> i64 {
    ((oid.to_u32() as u64) << 32 | (*dsmh as u64)) as i64
}

pub fn unpack_i64_to_oid_dsmh(value: i64) -> (sys::Oid, DsmHandle) {
    let val = value as u64;
    let a = (val >> 32) as u32;
    let b = (val & 0xFFFF_FFFF) as u32;
    (sys::Oid::from_u32(a), DsmHandle::from(b))
}

pub fn get_database_name(oid: sys::Oid) -> Option<String> {
    let db_name = unsafe {
        let db_name = sys::get_database_name(oid);

        if db_name.is_null() {
            return None;
        }

        CStr::from_ptr(db_name)
    };

    Some(db_name.to_string_lossy().to_string())
}

pub fn is_extension_installed(name: &str) -> bool {
    let query = "SELECT 1 FROM pg_extension WHERE extname = $1";

    pgrx::Spi::connect(|client| {
        let result = client.select(query, None, &[name.into()]);
        result.is_ok_and(|tuple| !tuple.is_empty())
    })
}

pub fn resolve_bytea_name(func_oid: sys::Oid) -> anyhow::Result<Option<String>> {
    unsafe {
        let schema_oid = sys::get_func_namespace(func_oid);
        let schema_name = sys::get_namespace_name(schema_oid);
        let fn_name = sys::get_func_name(func_oid);

        if fn_name.is_null() {
            return Ok(None);
        }

        let tuple = sys::SearchSysCache(
            sys::SysCacheIdentifier::PROCOID as i32,
            func_oid.into(),
            0.into(),
            0.into(),
            0.into(),
        );

        let Some(tuple) = SysHeapTuple::from_raw(tuple) else {
            return Ok(None);
        };

        let mut p_argtypes: *mut sys::Oid = std::ptr::null_mut();
        let mut p_argnames: *mut *mut std::os::raw::c_char = std::ptr::null_mut();
        let mut p_argmodes: *mut std::os::raw::c_char = std::ptr::null_mut();

        let num_args = sys::get_func_arg_info(
            tuple.inner,
            &mut p_argtypes,
            &mut p_argnames,
            &mut p_argmodes,
        );

        anyhow::ensure!(num_args == 1, "Argument count must be 1");
        anyhow::ensure!(!p_argtypes.is_null(), "Postgres internal error");
        anyhow::ensure!(*p_argtypes == sys::BYTEAOID, "Argument type must be bytea");

        let fn_name = CStr::from_ptr(fn_name).to_string_lossy().to_string();

        let schema_name = if !schema_name.is_null() {
            Some(CStr::from_ptr(schema_name).to_string_lossy().to_string())
        } else {
            None
        };

        if let Some(schema_name) = schema_name {
            Ok(Some(format!("{schema_name}.{fn_name}")))
        } else {
            Ok(Some(fn_name))
        }
    }
}
