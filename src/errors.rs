use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Error;

use crate::{
  config::{InstallFailureRecord, InstallFailureStage, InstallStepId, Ret2BootConfig},
  ui,
};

pub fn print_fatal(error: &Error) {
  eprintln!("{}", ui::error(error.to_string()));

  let causes = error.chain().skip(1).collect::<Vec<_>>();
  if causes.is_empty() {
    return;
  }

  eprintln!("{}", ui::note("caused by:"));
  for (index, cause) in causes.iter().enumerate() {
    eprintln!("  {}. {}", index + 1, cause);
  }
}

pub fn record_install_failure(
  config: &mut Ret2BootConfig, stage: InstallFailureStage, step_id: Option<InstallStepId>,
  error: &Error,
) -> anyhow::Result<()> {
  let record = InstallFailureRecord {
    stage,
    step_id,
    message: error.to_string(),
    causes: error.chain().skip(1).map(ToString::to_string).collect(),
    occurred_at_unix: SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map(|duration| duration.as_secs())
      .unwrap_or_default(),
  };

  if config.set_install_failure(record) {
    config.save()?;
  }

  Ok(())
}

pub fn clear_install_failure(config: &mut Ret2BootConfig) -> anyhow::Result<()> {
  if config.clear_install_failure() {
    config.save()?;
  }

  Ok(())
}
