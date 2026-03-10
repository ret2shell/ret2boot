use std::fs;

use anyhow::Result;
use rust_i18n::t;
use tracing::info;

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  support::{find_command_path, stage_remote_script},
};
use crate::config::{InstallStepId, InstallTargetRole};

const HELM_INSTALL_SCRIPT_URL: &str =
  "https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3";
const HELM_BINARY_DEST: &str = "/usr/local/bin/helm";

pub struct HelmCliStep;

impl AtomicInstallStep for HelmCliStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::HelmCli
  }

  fn should_include(&self, ctx: &StepPlanContext<'_>) -> bool {
    ctx.node_role() == Some(InstallTargetRole::ControlPlane)
  }

  fn collect(&self, _ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    println!();
    println!("{}", crate::ui::note(t!("install.helm.notice")));

    if let Some(path) = find_command_path("helm") {
      println!(
        "{}",
        crate::ui::note(t!(
          "install.helm.reuse_notice",
          path = path.display().to_string()
        ))
      );
    }

    Ok(())
  }

  fn describe(&self, _ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    let mut details = vec![t!("install.steps.helm.detail").to_string()];

    if let Some(path) = find_command_path("helm") {
      details.push(
        t!(
          "install.steps.helm.reuse",
          path = path.display().to_string()
        )
        .to_string(),
      );
    } else {
      details.push(t!("install.steps.helm.install_path", path = HELM_BINARY_DEST).to_string());
    }

    Ok(InstallStepPlan {
      id: self.id(),
      title: t!("install.steps.helm.title").to_string(),
      details,
    })
  }

  fn install(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    if let Some(path) = find_command_path("helm") {
      ctx.persist_change(
        "install.execution.helm.owned_by_ret2boot",
        "false",
        |config| {
          let changed = config.set_install_step_metadata(self.id(), "owned_by_ret2boot", "false");
          let changed =
            config.set_install_step_metadata(self.id(), "binary_path", path.display().to_string())
              || changed;
          config.remove_install_step_metadata(self.id(), "install_source") || changed
        },
      )?;

      info!(
        step = self.id().as_config_value(),
        path = %path.display(),
        "reusing existing helm binary"
      );
      return Ok(());
    }

    let script_path = stage_remote_script(HELM_INSTALL_SCRIPT_URL, "helm-install")?;
    let envs = vec![
      ("USE_SUDO".to_string(), "false".to_string()),
      ("HELM_INSTALL_DIR".to_string(), "/usr/local/bin".to_string()),
    ];

    let install_result =
      ctx.run_privileged_command("sh", &[script_path.display().to_string()], &envs);
    let _ = fs::remove_file(&script_path);
    install_result?;

    ctx.persist_change(
      "install.execution.helm.owned_by_ret2boot",
      "true",
      |config| {
        let changed = config.set_install_step_metadata(self.id(), "owned_by_ret2boot", "true");
        let changed =
          config.set_install_step_metadata(self.id(), "binary_path", HELM_BINARY_DEST) || changed;
        config.set_install_step_metadata(self.id(), "install_source", HELM_INSTALL_SCRIPT_URL)
          || changed
      },
    )?;

    Ok(())
  }

  fn uninstall(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let owned = ctx
      .config()
      .install_step_metadata(self.id(), "owned_by_ret2boot")
      .is_some_and(|value| value == "true");

    if !owned {
      return Ok(());
    }

    let binary_path = ctx
      .config()
      .install_step_metadata(self.id(), "binary_path")
      .unwrap_or(HELM_BINARY_DEST)
      .to_string();

    ctx.run_privileged_command("rm", &["-f".to_string(), binary_path], &[])?;
    ctx.persist_change(
      "install.execution.helm.owned_by_ret2boot",
      "false",
      |config| {
        let changed = config.remove_install_step_metadata(self.id(), "owned_by_ret2boot");
        let changed = config.remove_install_step_metadata(self.id(), "binary_path") || changed;
        config.remove_install_step_metadata(self.id(), "install_source") || changed
      },
    )?;

    Ok(())
  }
}
