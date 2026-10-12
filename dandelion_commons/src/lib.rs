use std::sync::Arc;

mod error;
pub mod range_pool;
pub mod records;
mod system_info;

pub use error::*;
pub use system_info::SystemInfo;

pub type FunctionId = Arc<String>;

#[derive(Clone, Copy, Debug)]
pub enum SystemFunction {
    HTTP,
    MEMCACHED,
}

impl From<&str> for SystemFunction {
    fn from(value: &str) -> Self {
        match value {
            "HTTP" => Self::HTTP,
            "MEMCACHED" => Self::MEMCACHED,
            _ => panic!("trying to convert non existent system function"),
        }
    }
}

impl Into<&str> for &SystemFunction {
    fn into(self) -> &'static str {
        match self {
            SystemFunction::HTTP => "HTTP",
            SystemFunction::MEMCACHED => "MEMCACHED",
        }
    }
}

impl core::fmt::Display for SystemFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", Into::<&str>::into(self))
    }
}
