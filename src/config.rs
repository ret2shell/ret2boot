use std::{collections::BTreeMap, env, fs, path::PathBuf};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

pub const ROOT_CONFIG_PATH: &str = "/etc/ret2shell/ret2boot.toml";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Ret2BootConfig {
  pub language: Option<String>,
  pub terminal: TerminalConfig,
  pub install: InstallConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TerminalConfig {
  pub charset: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InstallConfig {
  pub questionnaire: InstallQuestionnaire,
  pub review: InstallReview,
  pub execution: InstallExecution,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InstallQuestionnaire {
  pub node_role: Option<InstallTargetRole>,
  pub kubernetes: KubernetesQuestionnaire,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct KubernetesQuestionnaire {
  pub distribution: Option<KubernetesDistribution>,
  pub source: Option<KubernetesInstallSource>,
  pub application_exposure: Option<ApplicationExposureMode>,
  pub worker_join: WorkerJoinConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerJoinConfig {
  pub server_url: Option<String>,
  pub token: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InstallReview {
  pub confirmed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InstallExecution {
  pub phase: InstallExecutionPhase,
  pub steps: Vec<InstallStepProgress>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallExecutionPhase {
  #[default]
  Questionnaire,
  Review,
  Installing,
  Completed,
}

impl InstallExecutionPhase {
  pub fn as_config_value(self) -> &'static str {
    match self {
      Self::Questionnaire => "questionnaire",
      Self::Review => "review",
      Self::Installing => "installing",
      Self::Completed => "completed",
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InstallStepProgress {
  pub id: InstallStepId,
  pub status: InstallStepStatus,
  pub attempts: u32,
  pub last_error: Option<String>,
  pub metadata: BTreeMap<String, String>,
}

impl Default for InstallStepProgress {
  fn default() -> Self {
    Self::pending(InstallStepId::PreflightValidation)
  }
}

impl InstallStepProgress {
  fn pending(id: InstallStepId) -> Self {
    Self {
      id,
      status: InstallStepStatus::Pending,
      attempts: 0,
      last_error: None,
      metadata: BTreeMap::new(),
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallStepId {
  PreflightValidation,
  ClusterBootstrap,
  HelmCli,
  ApplicationGateway,
  PlatformDeployment,
}

impl InstallStepId {
  pub fn as_config_value(self) -> &'static str {
    match self {
      Self::PreflightValidation => "preflight-validation",
      Self::ClusterBootstrap => "cluster-bootstrap",
      Self::HelmCli => "helm-cli",
      Self::ApplicationGateway => "application-gateway",
      Self::PlatformDeployment => "platform-deployment",
    }
  }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallStepStatus {
  #[default]
  Pending,
  InProgress,
  Completed,
  Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallTargetRole {
  ControlPlane,
  Worker,
}

impl InstallTargetRole {
  pub const ALL: [Self; 2] = [Self::ControlPlane, Self::Worker];

  pub fn as_config_value(self) -> &'static str {
    match self {
      Self::ControlPlane => "control-plane",
      Self::Worker => "worker",
    }
  }

  pub fn default_index(self) -> usize {
    match self {
      Self::ControlPlane => 0,
      Self::Worker => 1,
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesDistribution {
  K3s,
  Rke2,
}

impl KubernetesDistribution {
  pub const ALL: [Self; 2] = [Self::K3s, Self::Rke2];

  pub fn as_config_value(self) -> &'static str {
    match self {
      Self::K3s => "k3s",
      Self::Rke2 => "rke2",
    }
  }

  pub fn default_index(self) -> usize {
    match self {
      Self::K3s => 0,
      Self::Rke2 => 1,
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesInstallSource {
  Official,
  ChinaMirror,
}

impl KubernetesInstallSource {
  pub const ALL: [Self; 2] = [Self::Official, Self::ChinaMirror];

  pub fn as_config_value(self) -> &'static str {
    match self {
      Self::Official => "official",
      Self::ChinaMirror => "china-mirror",
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApplicationExposureMode {
  Ingress,
  NodePortExternalNginx,
}

impl ApplicationExposureMode {
  pub const ALL: [Self; 2] = [Self::Ingress, Self::NodePortExternalNginx];

  pub fn as_config_value(self) -> &'static str {
    match self {
      Self::Ingress => "ingress",
      Self::NodePortExternalNginx => "nodeport-external-nginx",
    }
  }

  pub fn default_index(self) -> usize {
    match self {
      Self::Ingress => 0,
      Self::NodePortExternalNginx => 1,
    }
  }
}

impl Ret2BootConfig {
  pub fn load() -> Result<Self> {
    let path = Self::path()?;

    if !path
      .try_exists()
      .with_context(|| format!("failed to check config path `{}`", path.display()))?
    {
      return Ok(Self::default());
    }

    let contents = fs::read_to_string(&path)
      .with_context(|| format!("failed to read config file `{}`", path.display()))?;

    toml::from_str(&contents)
      .with_context(|| format!("failed to parse config file `{}`", path.display()))
  }

  pub fn save(&self) -> Result<()> {
    let path = Self::path()?;

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)
        .with_context(|| format!("failed to create config directory `{}`", parent.display()))?;
    }

    let contents = toml::to_string_pretty(self).context("failed to serialize app config")?;

    fs::write(&path, format!("{contents}\n"))
      .with_context(|| format!("failed to write config file `{}`", path.display()))
  }

  pub fn path() -> Result<PathBuf> {
    if is_root_user() {
      return Ok(PathBuf::from(ROOT_CONFIG_PATH));
    }

    if let Some(path) = xdg_config_home() {
      return Ok(path.join("ret2shell").join("ret2boot.toml"));
    }

    let home = env::var_os("HOME")
      .filter(|value| !value.is_empty())
      .map(PathBuf::from)
      .ok_or_else(|| anyhow!("HOME is not set and XDG_CONFIG_HOME is unavailable"))?;

    Ok(home.join(".config").join("ret2shell").join("ret2boot.toml"))
  }

  pub fn path_display() -> Result<String> {
    Ok(Self::path()?.display().to_string())
  }

  pub fn set_language(&mut self, language: impl Into<String>) -> bool {
    let language = language.into();

    if self.language.as_deref() == Some(language.as_str()) {
      return false;
    }

    self.language = Some(language);
    true
  }

  pub fn set_terminal_charset(&mut self, charset: impl Into<String>) -> bool {
    let charset = charset.into();

    if self.terminal.charset.as_deref() == Some(charset.as_str()) {
      return false;
    }

    self.terminal.charset = Some(charset);
    true
  }

  pub fn set_install_node_role(&mut self, role: InstallTargetRole) -> bool {
    if self.install.questionnaire.node_role == Some(role) {
      return false;
    }

    self.install.questionnaire.node_role = Some(role);
    self.invalidate_install_pipeline();

    true
  }

  pub fn set_install_kubernetes_distribution(
    &mut self, distribution: KubernetesDistribution,
  ) -> bool {
    if self.install.questionnaire.kubernetes.distribution == Some(distribution) {
      return false;
    }

    self.install.questionnaire.kubernetes.distribution = Some(distribution);
    self.invalidate_install_pipeline();

    true
  }

  pub fn set_install_kubernetes_source(&mut self, source: KubernetesInstallSource) -> bool {
    if self.install.questionnaire.kubernetes.source == Some(source) {
      return false;
    }

    self.install.questionnaire.kubernetes.source = Some(source);
    self.invalidate_install_pipeline();

    true
  }

  pub fn set_install_application_exposure(&mut self, exposure: ApplicationExposureMode) -> bool {
    if self.install.questionnaire.kubernetes.application_exposure == Some(exposure) {
      return false;
    }

    self.install.questionnaire.kubernetes.application_exposure = Some(exposure);
    self.invalidate_install_pipeline();

    true
  }

  pub fn set_install_worker_server_url(&mut self, server_url: impl Into<String>) -> bool {
    let server_url = server_url.into();

    if self
      .install
      .questionnaire
      .kubernetes
      .worker_join
      .server_url
      .as_deref()
      == Some(server_url.as_str())
    {
      return false;
    }

    self.install.questionnaire.kubernetes.worker_join.server_url = Some(server_url);
    self.invalidate_install_pipeline();

    true
  }

  pub fn set_install_worker_token(&mut self, token: impl Into<String>) -> bool {
    let token = token.into();

    if self
      .install
      .questionnaire
      .kubernetes
      .worker_join
      .token
      .as_deref()
      == Some(token.as_str())
    {
      return false;
    }

    self.install.questionnaire.kubernetes.worker_join.token = Some(token);
    self.invalidate_install_pipeline();

    true
  }

  pub fn set_install_review_confirmed(&mut self, confirmed: bool) -> bool {
    if self.install.review.confirmed == confirmed {
      return false;
    }

    self.install.review.confirmed = confirmed;

    if !confirmed {
      self.install.execution.phase = InstallExecutionPhase::Review;
      self.install.execution.steps.clear();
    }

    true
  }

  pub fn set_install_phase(&mut self, phase: InstallExecutionPhase) -> bool {
    if self.install.execution.phase == phase {
      return false;
    }

    self.install.execution.phase = phase;
    true
  }

  pub fn sync_install_steps(&mut self, step_ids: &[InstallStepId]) -> bool {
    let current_ids: Vec<InstallStepId> = self
      .install
      .execution
      .steps
      .iter()
      .map(|step| step.id)
      .collect();

    if current_ids == step_ids {
      return false;
    }

    self.install.execution.steps = step_ids
      .iter()
      .copied()
      .map(InstallStepProgress::pending)
      .collect();

    true
  }

  pub fn install_step_status(&self, step_id: InstallStepId) -> Option<InstallStepStatus> {
    self
      .install
      .execution
      .steps
      .iter()
      .find(|step| step.id == step_id)
      .map(|step| step.status)
  }

  pub fn install_step_metadata(&self, step_id: InstallStepId, key: &str) -> Option<&str> {
    self
      .install
      .execution
      .steps
      .iter()
      .find(|step| step.id == step_id)
      .and_then(|step| step.metadata.get(key))
      .map(String::as_str)
  }

  pub fn mark_install_step_started(&mut self, step_id: InstallStepId) -> bool {
    let step = self.ensure_install_step(step_id);
    let previous_status = step.status;
    let previous_attempts = step.attempts;
    let previous_error = step.last_error.clone();

    step.status = InstallStepStatus::InProgress;
    step.attempts += 1;
    step.last_error = None;

    step.status != previous_status
      || step.attempts != previous_attempts
      || step.last_error != previous_error
  }

  pub fn mark_install_step_completed(&mut self, step_id: InstallStepId) -> bool {
    let step = self.ensure_install_step(step_id);
    let previous_status = step.status;
    let previous_error = step.last_error.clone();

    step.status = InstallStepStatus::Completed;
    step.last_error = None;

    step.status != previous_status || step.last_error != previous_error
  }

  pub fn mark_install_step_failed(
    &mut self, step_id: InstallStepId, error: impl Into<String>,
  ) -> bool {
    let step = self.ensure_install_step(step_id);
    let previous_status = step.status;
    let previous_error = step.last_error.clone();

    step.status = InstallStepStatus::Failed;
    step.last_error = Some(error.into());

    step.status != previous_status || step.last_error != previous_error
  }

  pub fn reset_install_step(&mut self, step_id: InstallStepId) -> bool {
    let step = self.ensure_install_step(step_id);
    let previous_status = step.status;
    let previous_error = step.last_error.clone();
    let metadata_was_empty = step.metadata.is_empty();

    step.status = InstallStepStatus::Pending;
    step.last_error = None;
    step.metadata.clear();

    step.status != previous_status || step.last_error != previous_error || !metadata_was_empty
  }

  pub fn set_install_step_metadata(
    &mut self, step_id: InstallStepId, key: impl Into<String>, value: impl Into<String>,
  ) -> bool {
    let step = self.ensure_install_step(step_id);
    let key = key.into();
    let value = value.into();

    if step.metadata.get(&key) == Some(&value) {
      return false;
    }

    step.metadata.insert(key, value);
    true
  }

  pub fn remove_install_step_metadata(&mut self, step_id: InstallStepId, key: &str) -> bool {
    self
      .ensure_install_step(step_id)
      .metadata
      .remove(key)
      .is_some()
  }

  fn invalidate_install_pipeline(&mut self) {
    self.install.review.confirmed = false;
    self.install.execution = InstallExecution::default();
  }

  fn ensure_install_step(&mut self, step_id: InstallStepId) -> &mut InstallStepProgress {
    if let Some(index) = self
      .install
      .execution
      .steps
      .iter()
      .position(|step| step.id == step_id)
    {
      return &mut self.install.execution.steps[index];
    }

    self
      .install
      .execution
      .steps
      .push(InstallStepProgress::pending(step_id));
    self
      .install
      .execution
      .steps
      .last_mut()
      .expect("step exists")
  }
}

fn is_root_user() -> bool {
  unsafe { libc::geteuid() == 0 }
}

fn xdg_config_home() -> Option<PathBuf> {
  let path = PathBuf::from(env::var_os("XDG_CONFIG_HOME")?);

  if path.as_os_str().is_empty() || !path.is_absolute() {
    return None;
  }

  Some(path)
}
