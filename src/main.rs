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

use anyhow::{Context, Result};
use clap::Parser;

use crate::{
  cli::{Cli, CliCommand},
  config::{ROOT_CONFIG_PATH, Ret2BootConfig},
};

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
  let cli = Cli::parse();

  let mut config = Ret2BootConfig::load()?;
  match cli.command.unwrap_or(CliCommand::Install) {
    CliCommand::Install => {
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
    CliCommand::Sync => {
      let runtime = startup::initialize_maintenance(&mut config)?;
      if let Some(system_config) = load_system_config(&runtime)? {
        config = system_config;
      }
      install::sync_existing(&mut config, &runtime)
    }
    CliCommand::Update => {
      let runtime = startup::initialize_maintenance(&mut config)?;
      if let Some(system_config) = load_system_config(&runtime)? {
        config = system_config;
      }
      install::update_existing(&mut config, &runtime)
    }
    CliCommand::Uninstall => {
      let runtime = startup::initialize_maintenance(&mut config)?;
      if let Some(system_config) = load_system_config(&runtime)? {
        config = system_config;
      }
      install::uninstall_existing(&mut config, &runtime)
    }
  }
}

fn load_system_config(runtime: &startup::RuntimeState) -> Result<Option<Ret2BootConfig>> {
  let root_config_path = ROOT_CONFIG_PATH.to_string();

  if runtime
    .run_privileged_command("test", &["-f".to_string(), root_config_path.clone()], &[])
    .is_err()
  {
    return Ok(None);
  }

  let contents = runtime.run_privileged_command_capture("cat", &[root_config_path], &[])?;
  let config = toml::from_str(&contents).context("failed to parse the system installer config")?;

  Ok(Some(config))
}
