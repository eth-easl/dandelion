use anyhow::{Context, Result};

use crate::client::{check_status, decode_status, format_duration, Client};
use crate::config::Settings;
use crate::protocol::PROTOCOL_VERSION;
use crate::Globals;

pub fn run(globals: &Globals) -> Result<()> {
    let settings = Settings::load(globals)?;
    let client = Client::new(&settings)?;
    let url = client.url_for("/");
    let (status, body, elapsed) = client.get("/")?;
    check_status(status, &body)?;
    decode_status(&body).with_context(|| {
        format!("{url} answered, but not with a Dandelion protocol v{PROTOCOL_VERSION} response")
    })?;
    println!(
        "{url}: {status} in {} (protocol v{PROTOCOL_VERSION})",
        format_duration(elapsed)
    );
    Ok(())
}
