use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
  name = "ret2boot",
  version,
  about = "Interactive installer for the Ret2Shell platform"
)]
pub struct Cli {
  #[command(subcommand)]
  pub command: Option<CliCommand>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum CliCommand {
  Install,
  Update,
  Sync,
  Uninstall,
}

#[cfg(test)]
mod tests {
  use clap::Parser;

  use super::{Cli, CliCommand};

  #[test]
  fn defaults_to_install_when_no_subcommand_is_given() {
    let cli = Cli::parse_from(["ret2boot"]);

    assert!(cli.command.is_none());
  }

  #[test]
  fn parses_update_subcommand() {
    let cli = Cli::parse_from(["ret2boot", "update"]);

    assert!(matches!(cli.command, Some(CliCommand::Update)));
  }
}
