use std::env::{self, VarError};

use anyhow::{Result, anyhow};
use tracing_subscriber::EnvFilter;

pub const LOG_ENV: &str = "RET2SHELL_LOG";

pub fn init() -> Result<()> {
  let filter = env_filter()?;

  tracing_subscriber::fmt()
    .with_writer(std::io::stderr)
    .with_env_filter(filter)
    .with_target(false)
    .compact()
    .try_init()
    .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))?;

  Ok(())
}

fn env_filter() -> Result<EnvFilter> {
  match env::var(LOG_ENV) {
    Ok(value) if value.trim().is_empty() => Ok(EnvFilter::new("info")),
    Ok(value) => EnvFilter::try_new(value.trim())
      .map_err(|error| anyhow!("invalid {LOG_ENV} value `{value}`: {error}")),
    Err(VarError::NotPresent) => Ok(EnvFilter::new("info")),
    Err(error) => Err(anyhow!("failed to read {LOG_ENV}: {error}")),
  }
}
