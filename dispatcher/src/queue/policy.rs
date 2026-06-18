#[cfg(not(feature = "data_locality"))]
pub mod base;
#[cfg(not(feature = "data_locality"))]
pub use base::*;

#[cfg(feature = "data_locality")]
pub mod data_locality;
#[cfg(feature = "data_locality")]
pub use data_locality::*;
