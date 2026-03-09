mod cli;
mod config;
mod install;
mod l10n;
mod privilege;
mod resources;
mod startup;
mod telemetry;
mod terminal;
mod ui;

rust_i18n::i18n!("src/resources/locales", fallback = "en-us");

use anyhow::Result;
use clap::Parser;
use tracing::debug;

use crate::{cli::Cli, config::Ret2BootConfig};

fn main() -> Result<()> {
  let _cli = Cli::parse();

  telemetry::init()?;

  let config_path = Ret2BootConfig::path_display()?;
  let mut config = Ret2BootConfig::load()?;
  let Some(runtime) = startup::initialize(&mut config)? else {
    return Ok(());
  };

  debug!(
      config_path = %config_path,
      locale = %runtime.locale,
      terminal_charset = runtime.terminal_charset.as_config_value(),
      privilege_backend = runtime.privilege_backend,
      supported_locales = ?l10n::supported_locales(),
      "initialized application runtime"
  );

  install::run(&mut config, &runtime)
}
