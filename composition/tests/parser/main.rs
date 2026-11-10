//! Black-box tests for `composition::CompositionTemplate::parse`: syntax, validation, and error
//! reporting, all driven purely through the crate's public API.

#[path = "../common/mod.rs"]
mod common;

mod basic;
mod errors;
mod joins;
