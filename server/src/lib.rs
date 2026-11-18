pub mod config;
pub mod protocol;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/dandelion_frontend.rs"));
}
