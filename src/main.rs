mod cli;
mod config;
mod errors;
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

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;

use crate::{cli::Cli, config::Ret2BootConfig};

fn main() -> ExitCode {
  let config_path = Ret2BootConfig::path_display().ok();

  match try_main() {
    Ok(()) => ExitCode::SUCCESS,
    Err(error) => {
      errors::print_fatal(&error);

      if let Some(config_path) = config_path {
        eprintln!(
          "{}",
          ui::note(format!("installer state is kept at `{config_path}`"))
        );
      }

      ExitCode::FAILURE
    }
  }
}

fn try_main() -> Result<()> {
  let _cli = Cli::parse();

  let mut config = Ret2BootConfig::load()?;
  let runtime = match startup::initialize(&mut config) {
    Ok(Some(runtime)) => runtime,
    Ok(None) => return Ok(()),
    Err(error) => {
      let _ = errors::record_install_failure(
        &mut config,
        config::InstallFailureStage::Startup,
        None,
        &error,
      );
      return Err(error.context("installer failed during startup"));
    }
  };

  install::run(&mut config, &runtime)
}
