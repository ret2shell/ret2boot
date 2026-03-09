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
mod update;

rust_i18n::i18n!("src/resources/locales", fallback = "en-us");

use anyhow::Result;
use clap::Parser;

use crate::{cli::Cli, config::Ret2BootConfig};

fn main() -> Result<()> {
  let _cli = Cli::parse();

  let config_path = Ret2BootConfig::path_display()?;
  let mut config = Ret2BootConfig::load()?;
  let Some(runtime) = startup::initialize(&mut config)? else {
    return Ok(());
  };

  let _ = config_path;

  install::run(&mut config, &runtime)
}
