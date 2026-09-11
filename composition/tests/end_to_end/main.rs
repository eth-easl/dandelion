//! End-to-end tests: parse a composition, build and run it via `Composition`'s public API
//! (`start_execution` / `push_invocation_output` / `collect`), and check the data that comes out.
//! These are the black-box counterpart to the `src`-level scheduling/sharding unit tests: they
//! exercise the same machinery but only ever touch it the way an embedder of the crate would.

#[path = "../common/mod.rs"]
mod common;

mod simple;
mod sharding;
