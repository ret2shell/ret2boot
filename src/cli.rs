use clap::Parser;

#[derive(Debug, Parser)]
#[command(
  name = "ret2boot",
  version,
  about = "Interactive installer for the Ret2Shell platform"
)]
pub struct Cli {}
