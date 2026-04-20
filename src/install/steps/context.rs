use std::{thread, time::Duration};

use anyhow::Result;
use serde::Deserialize;
use tracing::debug;

use crate::{
  config::{
    InstallStepId, InstallTargetRole, KubernetesDistribution, KubernetesInstallSource,
    PlatformTlsMode, Ret2BootConfig,
  },
  startup::RuntimeState,
};

#[derive(Default, Clone)]
pub struct PreflightState {
  public_network: Option<PublicNetworkIdentity>,
  source_reachability: SourceReachability,
  package_manager: Option<SystemPackageManager>,
  disk_free_bytes: Option<u64>,
}

impl PreflightState {
  pub fn available_sources(
    &self, distribution: KubernetesDistribution,
  ) -> Vec<KubernetesInstallSource> {
    KubernetesInstallSource::ALL
      .into_iter()
      .filter(|source| self.is_source_reachable(distribution, *source))
      .collect()
  }

  pub fn recommended_source(
    &self, distribution: KubernetesDistribution,
  ) -> Option<KubernetesInstallSource> {
    let available = self.available_sources(distribution);

    if available.is_empty() {
      return None;
    }

    if self
      .public_network
      .as_ref()
      .is_some_and(PublicNetworkIdentity::is_mainland_china)
      && available.contains(&KubernetesInstallSource::ChinaMirror)
    {
      return Some(KubernetesInstallSource::ChinaMirror);
    }

    if available.contains(&KubernetesInstallSource::Official) {
      return Some(KubernetesInstallSource::Official);
    }

    available.first().copied()
  }

  pub fn public_network_description(&self) -> Option<String> {
    self
      .public_network
      .as_ref()
      .map(PublicNetworkIdentity::display)
  }

  pub fn public_network_ip(&self) -> Option<&str> {
    self.public_network.as_ref().map(PublicNetworkIdentity::ip)
  }

  pub fn package_manager(&self) -> Option<SystemPackageManager> {
    self.package_manager
  }

  pub fn disk_free_gib(&self) -> Option<u32> {
    self.disk_free_bytes.map(|bytes| {
      (bytes / 1024 / 1024 / 1024)
        .min(u32::MAX as u64)
        .try_into()
        .unwrap_or(u32::MAX)
    })
  }

  pub(crate) fn set_public_network(&mut self, public_network: Option<PublicNetworkIdentity>) {
    self.public_network = public_network;
  }

  pub(crate) fn set_source_reachability(
    &mut self, distribution: KubernetesDistribution, source: KubernetesInstallSource,
    reachable: bool,
  ) {
    match (distribution, source) {
      (KubernetesDistribution::K3s, KubernetesInstallSource::Official) => {
        self.source_reachability.k3s_official = reachable;
      }
      (KubernetesDistribution::K3s, KubernetesInstallSource::ChinaMirror) => {
        self.source_reachability.k3s_china_mirror = reachable;
      }
      (KubernetesDistribution::Rke2, KubernetesInstallSource::Official) => {
        self.source_reachability.rke2_official = reachable;
      }
      (KubernetesDistribution::Rke2, KubernetesInstallSource::ChinaMirror) => {
        self.source_reachability.rke2_china_mirror = reachable;
      }
    }
  }

  pub(crate) fn set_package_manager(&mut self, package_manager: Option<SystemPackageManager>) {
    self.package_manager = package_manager;
  }

  pub(crate) fn set_disk_free_bytes(&mut self, disk_free_bytes: Option<u64>) {
    self.disk_free_bytes = disk_free_bytes;
  }

  fn is_source_reachable(
    &self, distribution: KubernetesDistribution, source: KubernetesInstallSource,
  ) -> bool {
    match (distribution, source) {
      (KubernetesDistribution::K3s, KubernetesInstallSource::Official) => {
        self.source_reachability.k3s_official
      }
      (KubernetesDistribution::K3s, KubernetesInstallSource::ChinaMirror) => {
        self.source_reachability.k3s_china_mirror
      }
      (KubernetesDistribution::Rke2, KubernetesInstallSource::Official) => {
        self.source_reachability.rke2_official
      }
      (KubernetesDistribution::Rke2, KubernetesInstallSource::ChinaMirror) => {
        self.source_reachability.rke2_china_mirror
      }
    }
  }
}

#[derive(Default, Clone)]
struct SourceReachability {
  k3s_official: bool,
  k3s_china_mirror: bool,
  rke2_official: bool,
  rke2_china_mirror: bool,
}

#[derive(Clone, Deserialize)]
pub(crate) struct PublicNetworkIdentity {
  ip: String,
  country_code: Option<String>,
  country: Option<String>,
  region: Option<String>,
  city: Option<String>,
}

impl PublicNetworkIdentity {
  pub(crate) fn ip(&self) -> &str {
    &self.ip
  }

  pub(crate) fn is_mainland_china(&self) -> bool {
    self
      .country_code
      .as_deref()
      .is_some_and(|code| code.eq_ignore_ascii_case("CN"))
  }

  pub(crate) fn display(&self) -> String {
    let mut parts = Vec::new();

    if let Some(city) = self.city.as_deref().filter(|value| !value.is_empty()) {
      parts.push(city.to_string());
    }
    if let Some(region) = self.region.as_deref().filter(|value| !value.is_empty()) {
      parts.push(region.to_string());
    }
    if let Some(country) = self.country.as_deref().filter(|value| !value.is_empty()) {
      parts.push(country.to_string());
    }

    if parts.is_empty() {
      self.ip.clone()
    } else {
      format!("{} ({})", self.ip, parts.join(", "))
    }
  }
}

#[derive(Clone, Copy)]
pub enum SystemPackageManager {
  Apt,
  Dnf,
  Yum,
  Zypper,
  Apk,
  Pacman,
}

const APT_LOCK_TIMEOUT_SECONDS: &str = "300";
const APT_LOCK_RETRY_ATTEMPTS: usize = 3;
const APT_LOCK_RETRY_DELAY_SECONDS: u64 = 5;

impl SystemPackageManager {
  pub(crate) fn detect() -> Option<Self> {
    use super::support::command_exists;

    if command_exists("apt-get") {
      Some(Self::Apt)
    } else if command_exists("dnf") {
      Some(Self::Dnf)
    } else if command_exists("yum") {
      Some(Self::Yum)
    } else if command_exists("zypper") {
      Some(Self::Zypper)
    } else if command_exists("apk") {
      Some(Self::Apk)
    } else if command_exists("pacman") {
      Some(Self::Pacman)
    } else {
      None
    }
  }

  pub fn label(self) -> &'static str {
    match self {
      Self::Apt => "apt-get",
      Self::Dnf => "dnf",
      Self::Yum => "yum",
      Self::Zypper => "zypper",
      Self::Apk => "apk",
      Self::Pacman => "pacman",
    }
  }

  pub fn install_nginx(&self, ctx: &StepExecutionContext<'_>) -> Result<()> {
    match self {
      Self::Apt => {
        self.run_apt_command(ctx, &["update".to_string()], &[])?;
        self.run_apt_command(
          ctx,
          &["install".to_string(), "-y".to_string(), "nginx".to_string()],
          &[("DEBIAN_FRONTEND".to_string(), "noninteractive".to_string())],
        )
      }
      Self::Dnf => ctx.run_privileged_command(
        "dnf",
        &["install".to_string(), "-y".to_string(), "nginx".to_string()],
        &[],
      ),
      Self::Yum => ctx.run_privileged_command(
        "yum",
        &["install".to_string(), "-y".to_string(), "nginx".to_string()],
        &[],
      ),
      Self::Zypper => ctx.run_privileged_command(
        "zypper",
        &[
          "--non-interactive".to_string(),
          "install".to_string(),
          "nginx".to_string(),
        ],
        &[],
      ),
      Self::Apk => {
        ctx.run_privileged_command("apk", &["add".to_string(), "nginx".to_string()], &[])
      }
      Self::Pacman => ctx.run_privileged_command(
        "pacman",
        &[
          "-Sy".to_string(),
          "--noconfirm".to_string(),
          "nginx".to_string(),
        ],
        &[],
      ),
    }
  }

  pub fn remove_nginx(&self, ctx: &StepExecutionContext<'_>) -> Result<()> {
    match self {
      Self::Apt => self.run_apt_command(
        ctx,
        &["remove".to_string(), "-y".to_string(), "nginx".to_string()],
        &[("DEBIAN_FRONTEND".to_string(), "noninteractive".to_string())],
      ),
      Self::Dnf => ctx.run_privileged_command(
        "dnf",
        &["remove".to_string(), "-y".to_string(), "nginx".to_string()],
        &[],
      ),
      Self::Yum => ctx.run_privileged_command(
        "yum",
        &["remove".to_string(), "-y".to_string(), "nginx".to_string()],
        &[],
      ),
      Self::Zypper => ctx.run_privileged_command(
        "zypper",
        &[
          "--non-interactive".to_string(),
          "remove".to_string(),
          "nginx".to_string(),
        ],
        &[],
      ),
      Self::Apk => {
        ctx.run_privileged_command("apk", &["del".to_string(), "nginx".to_string()], &[])
      }
      Self::Pacman => ctx.run_privileged_command(
        "pacman",
        &[
          "-R".to_string(),
          "--noconfirm".to_string(),
          "nginx".to_string(),
        ],
        &[],
      ),
    }
  }

  pub(crate) fn from_label(label: &str) -> Option<Self> {
    match label {
      "apt-get" => Some(Self::Apt),
      "dnf" => Some(Self::Dnf),
      "yum" => Some(Self::Yum),
      "zypper" => Some(Self::Zypper),
      "apk" => Some(Self::Apk),
      "pacman" => Some(Self::Pacman),
      _ => None,
    }
  }

  fn run_apt_command(
    &self, ctx: &StepExecutionContext<'_>, args: &[String], envs: &[(String, String)],
  ) -> Result<()> {
    debug_assert!(matches!(self, Self::Apt));

    let mut full_args = vec![
      "-o".to_string(),
      format!("DPkg::Lock::Timeout={APT_LOCK_TIMEOUT_SECONDS}"),
    ];
    full_args.extend(args.iter().cloned());

    let mut last_error = None;
    for attempt in 1..=APT_LOCK_RETRY_ATTEMPTS {
      match ctx.run_privileged_command("apt-get", &full_args, envs) {
        Ok(()) => return Ok(()),
        Err(error) if apt_lock_error(&error) && attempt < APT_LOCK_RETRY_ATTEMPTS => {
          last_error = Some(error);
          thread::sleep(Duration::from_secs(APT_LOCK_RETRY_DELAY_SECONDS));
        }
        Err(error) => return Err(error),
      }
    }

    Err(last_error.expect("apt retry loop should capture the lock error"))
  }
}

fn apt_lock_error(error: &anyhow::Error) -> bool {
  let message = error.to_string();

  message.contains("/var/lib/dpkg/lock-frontend")
    || message.contains("/var/lib/dpkg/lock")
    || message.contains("Unable to acquire the dpkg frontend lock")
    || message.contains("Could not get lock")
}

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

  pub fn runtime(&self) -> &RuntimeState {
    self.runtime
  }

  pub fn config_path(&self) -> &str {
    self.config_path
  }

  pub fn node_role(&self) -> Option<InstallTargetRole> {
    self.config.install.questionnaire.node_role
  }

  pub fn kubernetes_distribution(&self) -> Option<KubernetesDistribution> {
    self.config.install.questionnaire.kubernetes.distribution
  }

  pub fn kubernetes_source(&self) -> Option<KubernetesInstallSource> {
    self.config.install.questionnaire.kubernetes.source
  }

  pub fn application_exposure(&self) -> Option<crate::config::ApplicationExposureMode> {
    self
      .config
      .install
      .questionnaire
      .kubernetes
      .application_exposure
  }

  pub fn kubernetes_disable_traefik(&self) -> Option<bool> {
    self
      .config
      .install
      .questionnaire
      .kubernetes
      .bootstrap
      .disable_traefik
  }

  pub fn kubernetes_enable_china_registry_mirrors(&self) -> Option<bool> {
    self
      .config
      .install
      .questionnaire
      .kubernetes
      .mirrors
      .enable_china_registry_mirrors
  }

  pub fn worker_server_url(&self) -> Option<&str> {
    self
      .config
      .install
      .questionnaire
      .kubernetes
      .worker_join
      .server_url
      .as_deref()
  }

  pub fn worker_token(&self) -> Option<&str> {
    self
      .config
      .install
      .questionnaire
      .kubernetes
      .worker_join
      .token
      .as_deref()
  }

  pub fn platform_tls_mode(&self) -> Option<PlatformTlsMode> {
    self.config.install.questionnaire.platform.tls.mode
  }

  pub fn platform_tls_secret_name(&self) -> Option<&str> {
    self
      .config
      .install
      .questionnaire
      .platform
      .tls
      .secret_name
      .as_deref()
  }

  pub fn platform_tls_domains(&self) -> &[String] {
    &self.config.install.questionnaire.platform.tls.domains
  }

  pub fn platform_tls_acme_email(&self) -> Option<&str> {
    self
      .config
      .install
      .questionnaire
      .platform
      .tls
      .acme_email
      .as_deref()
  }

  pub fn platform_tls_acme_dns_provider(&self) -> Option<&str> {
    self
      .config
      .install
      .questionnaire
      .platform
      .tls
      .acme_dns_provider
      .as_deref()
  }

  pub fn platform_tls_acme_dns_credentials(&self) -> Option<&str> {
    self
      .config
      .install
      .questionnaire
      .platform
      .tls
      .acme_dns_credentials
      .as_deref()
  }

  pub fn platform_tls_certificate_path(&self) -> Option<&str> {
    self
      .config
      .install
      .questionnaire
      .platform
      .tls
      .certificate_path
      .as_deref()
  }

  pub fn platform_tls_key_path(&self) -> Option<&str> {
    self
      .config
      .install
      .questionnaire
      .platform
      .tls
      .key_path
      .as_deref()
  }

  pub fn platform_nodeport_guard_enabled(&self) -> Option<bool> {
    self
      .config
      .install
      .questionnaire
      .platform
      .nodeport_security
      .guard_enabled
  }
}

pub struct StepQuestionContext<'a> {
  config: &'a mut Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: &'a str,
  preflight_state: &'a PreflightState,
}

#[allow(dead_code)]
impl<'a> StepQuestionContext<'a> {
  pub fn new(
    config: &'a mut Ret2BootConfig, runtime: &'a RuntimeState, config_path: &'a str,
    preflight_state: &'a PreflightState,
  ) -> Self {
    Self {
      config,
      runtime,
      config_path,
      preflight_state,
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

  pub fn preflight_state(&self) -> &PreflightState {
    self.preflight_state
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
      self.runtime.persist_system_config_copy(self.config)?;
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

pub struct StepPreflightContext<'a> {
  config: &'a Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: &'a str,
  state: &'a mut PreflightState,
}

#[allow(dead_code)]
impl<'a> StepPreflightContext<'a> {
  pub fn new(
    config: &'a Ret2BootConfig, runtime: &'a RuntimeState, config_path: &'a str,
    state: &'a mut PreflightState,
  ) -> Self {
    Self {
      config,
      runtime,
      config_path,
      state,
    }
  }

  pub fn config(&self) -> &Ret2BootConfig {
    self.config
  }

  pub fn runtime(&self) -> &RuntimeState {
    self.runtime
  }

  pub fn config_path(&self) -> &str {
    self.config_path
  }

  pub fn state(&self) -> &PreflightState {
    self.state
  }

  pub fn state_mut(&mut self) -> &mut PreflightState {
    self.state
  }
}

pub struct StepExecutionContext<'a> {
  config: &'a mut Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: &'a str,
  preflight_state: &'a PreflightState,
}

#[allow(dead_code)]
impl<'a> StepExecutionContext<'a> {
  pub fn new(
    config: &'a mut Ret2BootConfig, runtime: &'a RuntimeState, config_path: &'a str,
    preflight_state: &'a PreflightState,
  ) -> Self {
    Self {
      config,
      runtime,
      config_path,
      preflight_state,
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

  pub fn preflight_state(&self) -> &PreflightState {
    self.preflight_state
  }

  pub fn as_plan_context(&self) -> StepPlanContext<'_> {
    StepPlanContext::new(self.config, self.runtime, self.config_path)
  }

  pub fn run_privileged_command(
    &self, program: &str, args: &[String], envs: &[(String, String)],
  ) -> Result<()> {
    self.runtime.run_privileged_command(program, args, envs)
  }

  pub fn run_privileged_command_capture(
    &self, program: &str, args: &[String], envs: &[(String, String)],
  ) -> Result<String> {
    self
      .runtime
      .run_privileged_command_capture(program, args, envs)
  }

  pub fn persist_change<F>(&mut self, field: &'static str, value: &str, update: F) -> Result<bool>
  where
    F: FnOnce(&mut Ret2BootConfig) -> bool, {
    let changed = update(self.config);

    if changed {
      self.config.save()?;
      self.runtime.persist_system_config_copy(self.config)?;
    }

    debug!(
      config_path = %self.config_path,
      field,
      value,
      changed,
      "persisted step execution state"
    );

    Ok(changed)
  }
}

#[cfg(test)]
mod tests {
  use anyhow::anyhow;

  use super::apt_lock_error;

  #[test]
  fn detects_dpkg_lock_frontend_errors() {
    let error = anyhow!(
      "privileged command `apt-get` exited with status Some(100): E: Could not get lock /var/lib/dpkg/lock-frontend. It is held by process 33265 (unattended-upgr)"
    );

    assert!(apt_lock_error(&error));
  }

  #[test]
  fn ignores_non_lock_apt_errors() {
    let error = anyhow!(
      "privileged command `apt-get` exited with status Some(100): E: Unable to locate package nginx"
    );

    assert!(!apt_lock_error(&error));
  }
}
