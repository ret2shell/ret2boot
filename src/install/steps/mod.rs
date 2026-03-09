use anyhow::{Result, bail};
use rust_i18n::t;
use tracing::debug;

use crate::{
  config::{InstallStepId, InstallTargetRole, Ret2BootConfig},
  startup::RuntimeState,
};

pub struct InstallStepPlan {
  pub id: InstallStepId,
  pub title: String,
  pub details: Vec<String>,
}

pub struct StepPlanContext<'a> {
  config: &'a Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: &'a str,
}

#[allow(dead_code)]
impl<'a> StepPlanContext<'a> {
  pub fn new(config: &'a Ret2BootConfig, runtime: &'a RuntimeState, config_path: &'a str) -> Self {
    Self {
      config,
      runtime,
      config_path,
    }
  }

  pub fn config(&self) -> &Ret2BootConfig {
    self.config
  }

  #[allow(dead_code)]
  pub fn runtime(&self) -> &RuntimeState {
    self.runtime
  }

  #[allow(dead_code)]
  pub fn config_path(&self) -> &str {
    self.config_path
  }

  pub fn node_role(&self) -> Option<InstallTargetRole> {
    self.config.install.questionnaire.node_role
  }
}

pub struct StepQuestionContext<'a> {
  config: &'a mut Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: &'a str,
}

#[allow(dead_code)]
impl<'a> StepQuestionContext<'a> {
  pub fn new(
    config: &'a mut Ret2BootConfig, runtime: &'a RuntimeState, config_path: &'a str,
  ) -> Self {
    Self {
      config,
      runtime,
      config_path,
    }
  }

  pub fn config(&self) -> &Ret2BootConfig {
    self.config
  }

  pub fn config_mut(&mut self) -> &mut Ret2BootConfig {
    self.config
  }

  pub fn runtime(&self) -> &RuntimeState {
    self.runtime
  }

  pub fn as_plan_context(&self) -> StepPlanContext<'_> {
    StepPlanContext::new(self.config, self.runtime, self.config_path)
  }

  pub fn persist_change<F>(&mut self, field: &'static str, value: &str, update: F) -> Result<bool>
  where
    F: FnOnce(&mut Ret2BootConfig) -> bool, {
    let changed = update(self.config);

    if changed {
      self.config.save()?;
    }

    debug!(
        config_path = %self.config_path,
        field,
        value,
        changed,
        "persisted step questionnaire state"
    );

    Ok(changed)
  }
}

pub struct StepExecutionContext<'a> {
  config: &'a mut Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: &'a str,
}

#[allow(dead_code)]
impl<'a> StepExecutionContext<'a> {
  pub fn new(
    config: &'a mut Ret2BootConfig, runtime: &'a RuntimeState, config_path: &'a str,
  ) -> Self {
    Self {
      config,
      runtime,
      config_path,
    }
  }

  pub fn config(&self) -> &Ret2BootConfig {
    self.config
  }

  pub fn config_mut(&mut self) -> &mut Ret2BootConfig {
    self.config
  }

  pub fn runtime(&self) -> &RuntimeState {
    self.runtime
  }

  pub fn config_path(&self) -> &str {
    self.config_path
  }
}

#[allow(dead_code)]
pub trait AtomicInstallStep {
  fn id(&self) -> InstallStepId;

  fn should_include(&self, _ctx: &StepPlanContext<'_>) -> bool {
    true
  }

  fn collect(&self, _ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    Ok(())
  }

  fn describe(&self, ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan>;

  fn install(&self, _ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    bail!(
      "install step `{}` is not implemented",
      self.id().as_config_value()
    )
  }

  fn uninstall(&self, _ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    bail!(
      "uninstall step `{}` is not implemented",
      self.id().as_config_value()
    )
  }

  fn rollback(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    self.uninstall(ctx)
  }
}

pub fn registry() -> Vec<Box<dyn AtomicInstallStep>> {
  vec![
    Box::new(PreflightValidationStep),
    Box::new(ClusterBootstrapStep),
    Box::new(PlatformDeploymentStep),
  ]
}

struct PreflightValidationStep;

impl AtomicInstallStep for PreflightValidationStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::PreflightValidation
  }

  fn describe(&self, _ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    Ok(InstallStepPlan {
      id: self.id(),
      title: t!("install.steps.preflight").to_string(),
      details: vec![t!("install.steps.preflight_detail").to_string()],
    })
  }
}

struct ClusterBootstrapStep;

impl AtomicInstallStep for ClusterBootstrapStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::ClusterBootstrap
  }

  fn describe(&self, ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    let role = ctx
      .node_role()
      .ok_or_else(|| anyhow::anyhow!("node role is required before planning cluster bootstrap"))?;

    let (title, detail) = match role {
      InstallTargetRole::ControlPlane => (
        t!("install.steps.cluster.control_plane"),
        t!("install.steps.cluster.control_plane_detail"),
      ),
      InstallTargetRole::Worker => (
        t!("install.steps.cluster.worker"),
        t!("install.steps.cluster.worker_detail"),
      ),
    };

    Ok(InstallStepPlan {
      id: self.id(),
      title: title.to_string(),
      details: vec![detail.to_string()],
    })
  }
}

struct PlatformDeploymentStep;

impl AtomicInstallStep for PlatformDeploymentStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::PlatformDeployment
  }

  fn describe(&self, ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    let role = ctx.node_role().ok_or_else(|| {
      anyhow::anyhow!("node role is required before planning platform deployment")
    })?;

    let (title, detail) = match role {
      InstallTargetRole::ControlPlane => (
        t!("install.steps.platform.control_plane"),
        t!("install.steps.platform.control_plane_detail"),
      ),
      InstallTargetRole::Worker => (
        t!("install.steps.platform.worker"),
        t!("install.steps.platform.worker_detail"),
      ),
    };

    Ok(InstallStepPlan {
      id: self.id(),
      title: title.to_string(),
      details: vec![detail.to_string()],
    })
  }
}
