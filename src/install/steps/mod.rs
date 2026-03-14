mod cluster;
mod context;
mod gateway;
mod helm;
mod platform;
mod preflight;
mod support;

use anyhow::{Result, bail};

pub(crate) use self::{
  cluster::ClusterBootstrapStep,
  context::{
    InstallStepPlan, PreflightState, StepExecutionContext, StepPlanContext, StepPreflightContext,
    StepQuestionContext, SystemPackageManager,
  },
  gateway::ApplicationGatewayStep,
  helm::HelmCliStep,
  platform::{
    PlatformDeploymentStep, PlatformSyncMode, PlatformSyncReport, WorkerPlatformProbeStep,
  },
  preflight::PreflightValidationStep,
};
use crate::config::InstallStepId;

#[allow(dead_code)]
pub trait AtomicInstallStep {
  fn id(&self) -> InstallStepId;

  fn preflight(&self, _ctx: &mut StepPreflightContext<'_>) -> Result<bool> {
    Ok(false)
  }

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
    Box::new(HelmCliStep),
    Box::new(ApplicationGatewayStep),
    Box::new(PlatformDeploymentStep),
    Box::new(WorkerPlatformProbeStep),
  ]
}
