use std::{
  collections::BTreeSet,
  env,
  ffi::CString,
  fs,
  mem::MaybeUninit,
  path::{Path, PathBuf},
  process::Command,
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::blocking::Client;
use rust_i18n::t;
use serde::Deserialize;
use tracing::{debug, info};

use crate::{
  config::{
    ApplicationExposureMode, InstallStepId, InstallTargetRole, KubernetesDistribution,
    KubernetesInstallSource, Ret2BootConfig,
  },
  install::collectors::{Collector, InputCollector, SingleSelectCollector},
  startup::RuntimeState,
  ui,
};

const CLUSTER_CIDR: &str = "10.42.0.0/16";
const NODE_CIDR_MASK_SIZE: u8 = 20;
const NODE_MAX_PODS: u16 = 3072;
const K3S_CONFIG_DEST: &str = "/etc/rancher/k3s/config.yaml";
const K3S_KUBELET_CONFIG_DEST: &str = "/etc/rancher/k3s/kubelet.config";
const RKE2_CONFIG_DEST: &str = "/etc/rancher/rke2/config.yaml";
const RKE2_KUBELET_CONFIG_DEST: &str = "/etc/rancher/rke2/kubelet.conf";
const HELM_INSTALL_SCRIPT_URL: &str =
  "https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3";
const HELM_BINARY_DEST: &str = "/usr/local/bin/helm";
const NGINX_BINARY_DEST: &str = "/usr/sbin/nginx";
const NGINX_MAIN_CONF: &str = "/etc/nginx/nginx.conf";
const NGINX_SITE_AVAILABLE: &str = "/etc/nginx/sites-available/ret2boot.conf";
const NGINX_SITE_ENABLED: &str = "/etc/nginx/sites-enabled/ret2boot.conf";
const NGINX_SITE_INCLUDE: &str = "/etc/nginx/conf.d/ret2boot-sites-enabled.conf";
const NGINX_STREAM_AVAILABLE: &str = "/etc/nginx/ret2boot-stream-available/ret2boot.conf";
const NGINX_STREAM_ENABLED: &str = "/etc/nginx/ret2boot-stream-enabled/ret2boot.conf";
const NGINX_STREAM_INCLUDE_MARKER: &str = "include /etc/nginx/ret2boot-stream-enabled/*.conf;";
const K3S_MANIFEST_DIR: &str = "/var/lib/rancher/k3s/server/manifests";
const RKE2_MANIFEST_DIR: &str = "/var/lib/rancher/rke2/server/manifests";
const K3S_TRAEFIK_CONFIG_DEST: &str =
  "/var/lib/rancher/k3s/server/manifests/ret2boot-traefik-config.yaml";
const RKE2_TRAEFIK_CONFIG_DEST: &str =
  "/var/lib/rancher/rke2/server/manifests/ret2boot-traefik-config.yaml";
const RKE2_INGRESS_NGINX_CONFIG_DEST: &str =
  "/var/lib/rancher/rke2/server/manifests/ret2boot-ingress-nginx-config.yaml";
const PREFLIGHT_MIN_DISK_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const PREFLIGHT_WARN_DISK_FREE_BYTES: u64 = 20 * 1024 * 1024 * 1024;
const PREFLIGHT_MIN_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const PREFLIGHT_WARN_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const GATEWAY_HTTP_PORT_CANDIDATES: [u16; 6] = [10080, 11080, 12080, 13080, 14080, 15080];
const GATEWAY_HTTPS_PORT_CANDIDATES: [u16; 6] = [10443, 11443, 12443, 13443, 14443, 15443];

#[derive(Default, Clone)]
pub struct PreflightState {
  public_network: Option<PublicNetworkIdentity>,
  source_reachability: SourceReachability,
  package_manager: Option<SystemPackageManager>,
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

  pub fn package_manager(&self) -> Option<SystemPackageManager> {
    self.package_manager
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

  fn set_source_reachability(
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
}

#[derive(Default, Clone)]
struct SourceReachability {
  k3s_official: bool,
  k3s_china_mirror: bool,
  rke2_official: bool,
  rke2_china_mirror: bool,
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

impl SystemPackageManager {
  fn detect() -> Option<Self> {
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

  fn label(self) -> &'static str {
    match self {
      Self::Apt => "apt-get",
      Self::Dnf => "dnf",
      Self::Yum => "yum",
      Self::Zypper => "zypper",
      Self::Apk => "apk",
      Self::Pacman => "pacman",
    }
  }

  fn install_nginx(self, ctx: &StepExecutionContext<'_>) -> Result<()> {
    match self {
      Self::Apt => {
        ctx.run_privileged_command("apt-get", &["update".to_string()], &[])?;
        ctx.run_privileged_command(
          "apt-get",
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

  fn remove_nginx(self, ctx: &StepExecutionContext<'_>) -> Result<()> {
    match self {
      Self::Apt => ctx.run_privileged_command(
        "apt-get",
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
}

#[derive(Clone, Deserialize)]
struct PublicNetworkIdentity {
  ip: String,
  country_code: Option<String>,
  country: Option<String>,
  region: Option<String>,
  city: Option<String>,
}

impl PublicNetworkIdentity {
  fn is_mainland_china(&self) -> bool {
    self
      .country_code
      .as_deref()
      .is_some_and(|code| code.eq_ignore_ascii_case("CN"))
  }

  fn display(&self) -> String {
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

enum PreflightStatus {
  Passed,
  Warning,
  Failed,
}

struct PreflightCheck {
  label: String,
  detail: String,
  status: PreflightStatus,
}

struct PreflightReport {
  checks: Vec<PreflightCheck>,
}

impl PreflightReport {
  fn collect() -> Result<(Self, PreflightState)> {
    let client = Client::builder()
      .https_only(true)
      .timeout(Duration::from_secs(5))
      .build()
      .context("failed to build preflight HTTP client")?;

    println!();
    println!("{}", ui::section(t!("install.preflight.title")));

    let mut state = PreflightState::default();

    let mut checks = Vec::new();
    checks.push(run_preflight_check(
      t!("install.preflight.checks.downloader").to_string(),
      check_downloader,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.systemd").to_string(),
      check_systemd,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.package_manager").to_string(),
      || {
        state.package_manager = SystemPackageManager::detect();
        check_package_manager(state.package_manager)
      },
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.public_network").to_string(),
      || {
        state.public_network = probe_public_network(&client);
        check_public_network(&state.public_network)
      },
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.github").to_string(),
      || check_github_connectivity(probe_endpoint(&client, "https://github.com")),
    ));

    let k3s_label = t!("install.preflight.checks.k3s_sources").to_string();
    checks.push(run_preflight_check(k3s_label.clone(), || {
      let official = probe_endpoint(&client, "https://get.k3s.io");
      let mirror = probe_endpoint(
        &client,
        "https://rancher-mirror.rancher.cn/k3s/k3s-install.sh",
      );
      state.set_source_reachability(
        KubernetesDistribution::K3s,
        KubernetesInstallSource::Official,
        official,
      );
      state.set_source_reachability(
        KubernetesDistribution::K3s,
        KubernetesInstallSource::ChinaMirror,
        mirror,
      );

      check_source_connectivity(
        k3s_label,
        &[
          endpoint_reachability("get.k3s.io", official),
          endpoint_reachability("rancher-mirror.rancher.cn/k3s", mirror),
        ],
      )
    }));

    let rke2_label = t!("install.preflight.checks.rke2_sources").to_string();
    checks.push(run_preflight_check(rke2_label.clone(), || {
      let official = probe_endpoint(&client, "https://get.rke2.io");
      let mirror = probe_endpoint(&client, "https://rancher-mirror.rancher.cn/rke2/install.sh");
      state.set_source_reachability(
        KubernetesDistribution::Rke2,
        KubernetesInstallSource::Official,
        official,
      );
      state.set_source_reachability(
        KubernetesDistribution::Rke2,
        KubernetesInstallSource::ChinaMirror,
        mirror,
      );

      check_source_connectivity(
        rke2_label,
        &[
          endpoint_reachability("get.rke2.io", official),
          endpoint_reachability("rancher-mirror.rancher.cn/rke2", mirror),
        ],
      )
    }));

    checks.push(run_preflight_check(
      t!("install.preflight.checks.disk").to_string(),
      check_disk_capacity,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.memory").to_string(),
      check_memory_capacity,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.ports").to_string(),
      check_port_state,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.sysctl").to_string(),
      check_sysctl_state,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.cgroup_memory").to_string(),
      check_cgroup_memory,
    ));

    let overlay_label = t!("install.preflight.checks.overlay").to_string();
    checks.push(run_preflight_check(overlay_label.clone(), || {
      check_kernel_feature(overlay_label, kernel_feature_state_overlay())
    }));

    let br_netfilter_label = t!("install.preflight.checks.br_netfilter").to_string();
    checks.push(run_preflight_check(br_netfilter_label.clone(), || {
      check_kernel_feature(br_netfilter_label, kernel_feature_state_br_netfilter())
    }));

    Ok((Self { checks }, state))
  }

  fn has_failures(&self) -> bool {
    self
      .checks
      .iter()
      .any(|check| matches!(check.status, PreflightStatus::Failed))
  }

  fn has_warnings(&self) -> bool {
    self
      .checks
      .iter()
      .any(|check| matches!(check.status, PreflightStatus::Warning))
  }

  fn print(&self) {
    println!();
    println!(
      "{}",
      if self.has_failures() {
        ui::warning(t!("install.preflight.summary.failed"))
      } else if self.has_warnings() {
        ui::note(t!("install.preflight.summary.warning"))
      } else {
        ui::success(t!("install.preflight.summary.passed"))
      }
    );
  }
}

struct EndpointReachability<'a> {
  label: &'a str,
  reachable: bool,
}

enum KernelFeatureState {
  Ready,
  Loadable,
  Missing,
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

  pub fn application_exposure(&self) -> Option<ApplicationExposureMode> {
    self
      .config
      .install
      .questionnaire
      .kubernetes
      .application_exposure
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
}

pub struct StepQuestionContext<'a> {
  config: &'a mut Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: &'a str,
  preflight_state: &'a PreflightState,
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
      "persisted step execution state"
    );

    Ok(changed)
  }
}

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
  ]
}

struct PreflightValidationStep;

impl AtomicInstallStep for PreflightValidationStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::PreflightValidation
  }

  fn preflight(&self, ctx: &mut StepPreflightContext<'_>) -> Result<bool> {
    let (report, state) = PreflightReport::collect()?;
    *ctx.state_mut() = state;
    report.print();

    if report.has_failures() {
      bail!(t!("install.preflight.summary.failed").to_string())
    }

    Ok(true)
  }

  fn describe(&self, _ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    Ok(InstallStepPlan {
      id: self.id(),
      title: t!("install.steps.preflight").to_string(),
      details: vec![t!("install.steps.preflight_detail").to_string()],
    })
  }

  fn install(&self, _ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    Ok(())
  }

  fn uninstall(&self, _ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    Ok(())
  }
}

struct ClusterBootstrapStep;

impl AtomicInstallStep for ClusterBootstrapStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::ClusterBootstrap
  }

  fn collect(&self, ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    let role = ctx
      .as_plan_context()
      .node_role()
      .ok_or_else(|| anyhow!("node role is required before collecting cluster settings"))?;

    if role == InstallTargetRole::Worker {
      println!();
      println!("{}", ui::warning(t!("install.kubernetes.worker_hint")));
    }

    let distribution_default = ctx
      .config()
      .install
      .questionnaire
      .kubernetes
      .distribution
      .unwrap_or(KubernetesDistribution::K3s)
      .default_index();

    let distribution_options = KubernetesDistribution::ALL
      .iter()
      .copied()
      .map(kubernetes_distribution_label)
      .collect();

    let distribution = KubernetesDistribution::ALL[SingleSelectCollector::new(
      t!("install.kubernetes.distribution.prompt"),
      distribution_options,
    )
    .with_default(distribution_default)
    .collect_index()?];

    ctx.persist_change(
      "install.questionnaire.kubernetes.distribution",
      distribution.as_config_value(),
      |config| config.set_install_kubernetes_distribution(distribution),
    )?;

    let source_default = ctx.config().install.questionnaire.kubernetes.source;
    let available_sources = ctx.preflight_state().available_sources(distribution);

    if available_sources.is_empty() {
      bail!(t!("install.kubernetes.source.none_available").to_string());
    }

    if available_sources.len() < KubernetesInstallSource::ALL.len() {
      println!("{}", ui::note(t!("install.kubernetes.source.filtered")));
    }

    let recommended_source = ctx.preflight_state().recommended_source(distribution);
    if let (Some(public_network), Some(source)) = (
      ctx.preflight_state().public_network_description(),
      recommended_source,
    ) {
      println!(
        "{}",
        ui::note(t!(
          "install.kubernetes.source.recommended",
          public_network = public_network,
          source = kubernetes_source_label(distribution, source)
        ))
      );
    }

    let source = if available_sources.len() == 1 {
      let source = available_sources[0];
      println!(
        "{}",
        ui::note(t!(
          "install.kubernetes.source.auto_selected",
          source = kubernetes_source_label(distribution, source)
        ))
      );
      source
    } else {
      let default_source = source_default
        .filter(|source| available_sources.contains(source))
        .or(recommended_source)
        .unwrap_or(available_sources[0]);
      let default_index = available_sources
        .iter()
        .position(|source| *source == default_source)
        .unwrap_or(0);
      let source_options = available_sources
        .iter()
        .copied()
        .map(|source| kubernetes_source_label(distribution, source))
        .collect();

      available_sources[SingleSelectCollector::new(
        t!("install.kubernetes.source.prompt"),
        source_options,
      )
      .with_default(default_index)
      .collect_index()?]
    };

    ctx.persist_change(
      "install.questionnaire.kubernetes.source",
      source.as_config_value(),
      |config| config.set_install_kubernetes_source(source),
    )?;

    if role == InstallTargetRole::Worker {
      println!();
      println!("{}", ui::note(worker_server_url_hint(distribution)));

      let server_prompt = InputCollector::new(t!("install.kubernetes.worker.server_url.prompt"));
      let server_prompt = match &ctx
        .config()
        .install
        .questionnaire
        .kubernetes
        .worker_join
        .server_url
      {
        Some(default) => server_prompt.with_default(default.clone()),
        None => server_prompt,
      };
      let server_url = server_prompt.collect()?;
      let server_url = server_url.trim().to_string();

      ctx.persist_change(
        "install.questionnaire.kubernetes.worker_join.server_url",
        &server_url,
        |config| config.set_install_worker_server_url(server_url.clone()),
      )?;

      let token_prompt = InputCollector::new(t!("install.kubernetes.worker.token.prompt"));
      let token_prompt = match &ctx
        .config()
        .install
        .questionnaire
        .kubernetes
        .worker_join
        .token
      {
        Some(default) => token_prompt.with_default(default.clone()),
        None => token_prompt,
      };
      let token = token_prompt.collect()?;
      let token = token.trim().to_string();

      ctx.persist_change(
        "install.questionnaire.kubernetes.worker_join.token",
        "[redacted]",
        |config| config.set_install_worker_token(token.clone()),
      )?;
    }

    Ok(())
  }

  fn describe(&self, ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    let spec = ClusterInstallSpec::from_plan_context(ctx)?;

    let (title, detail) = match spec.role {
      InstallTargetRole::ControlPlane => (
        t!("install.steps.cluster.control_plane"),
        t!("install.steps.cluster.control_plane_detail"),
      ),
      InstallTargetRole::Worker => (
        t!("install.steps.cluster.worker"),
        t!("install.steps.cluster.worker_detail"),
      ),
    };

    let mut details = vec![
      detail.to_string(),
      t!(
        "install.steps.cluster.selected_distribution",
        distribution = kubernetes_distribution_label(spec.distribution)
      )
      .to_string(),
      t!(
        "install.steps.cluster.selected_source",
        source = kubernetes_source_label(spec.distribution, spec.source)
      )
      .to_string(),
      t!("install.steps.cluster.pod_cidr", cidr = CLUSTER_CIDR).to_string(),
      t!(
        "install.steps.cluster.node_cidr_mask",
        mask = NODE_CIDR_MASK_SIZE
      )
      .to_string(),
      t!("install.steps.cluster.max_pods", max_pods = NODE_MAX_PODS).to_string(),
    ];

    if let Some(exposure) = spec.application_exposure {
      details.push(
        t!(
          "install.steps.cluster.application_exposure",
          exposure = application_exposure_label(exposure)
        )
        .to_string(),
      );

      if exposure == ApplicationExposureMode::NodePortExternalNginx {
        details.push(t!("install.steps.cluster.gateway_ports_shifted").to_string());
      }
    }

    if let Some(server_url) = spec.worker_server_url.as_deref() {
      details.push(t!("install.steps.cluster.worker_server", server = server_url).to_string());
      details.push(t!("install.steps.cluster.worker_token_saved").to_string());
    }

    Ok(InstallStepPlan {
      id: self.id(),
      title: title.to_string(),
      details,
    })
  }

  fn install(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let spec = ClusterInstallSpec::from_plan_context(&ctx.as_plan_context())?;

    info!(
      step = self.id().as_config_value(),
      distribution = spec.distribution.as_config_value(),
      source = spec.source.as_config_value(),
      role = spec.role.as_config_value(),
      "installing kubernetes distribution"
    );

    match spec.distribution {
      KubernetesDistribution::K3s => install_k3s(ctx, &spec),
      KubernetesDistribution::Rke2 => install_rke2(ctx, &spec),
    }
  }

  fn uninstall(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let spec = ClusterInstallSpec::from_plan_context(&ctx.as_plan_context())?;

    info!(
      step = self.id().as_config_value(),
      distribution = spec.distribution.as_config_value(),
      role = spec.role.as_config_value(),
      "uninstalling kubernetes distribution"
    );

    match spec.distribution {
      KubernetesDistribution::K3s => uninstall_k3s(ctx, &spec),
      KubernetesDistribution::Rke2 => uninstall_rke2(ctx),
    }
  }

  fn rollback(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let spec = ClusterInstallSpec::from_plan_context(&ctx.as_plan_context())?;

    match spec.distribution {
      KubernetesDistribution::K3s => rollback_k3s(ctx, &spec),
      KubernetesDistribution::Rke2 => rollback_rke2(ctx),
    }
  }
}

struct HelmCliStep;

impl AtomicInstallStep for HelmCliStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::HelmCli
  }

  fn collect(&self, _ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    println!();
    println!("{}", ui::note(t!("install.helm.notice")));

    if let Some(path) = find_command_path("helm") {
      println!(
        "{}",
        ui::note(t!(
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

    ctx.run_privileged_command("rm", &["-f".to_string(), binary_path.clone()], &[])?;
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

struct ApplicationGatewayStep;

impl AtomicInstallStep for ApplicationGatewayStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::ApplicationGateway
  }

  fn should_include(&self, ctx: &StepPlanContext<'_>) -> bool {
    ctx.node_role() == Some(InstallTargetRole::ControlPlane)
  }

  fn collect(&self, ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    let default = ctx
      .config()
      .install
      .questionnaire
      .kubernetes
      .application_exposure
      .unwrap_or(ApplicationExposureMode::Ingress)
      .default_index();
    let options = ApplicationExposureMode::ALL
      .iter()
      .copied()
      .map(application_exposure_label)
      .collect();

    let exposure = ApplicationExposureMode::ALL[SingleSelectCollector::new(
      t!("install.exposure.prompt"),
      options,
    )
    .with_default(default)
    .collect_index()?];

    ctx.persist_change(
      "install.questionnaire.kubernetes.application_exposure",
      exposure.as_config_value(),
      |config| config.set_install_application_exposure(exposure),
    )?;

    println!();
    match exposure {
      ApplicationExposureMode::Ingress => {
        println!("{}", ui::note(t!("install.exposure.ingress_notice")));
      }
      ApplicationExposureMode::NodePortExternalNginx => {
        println!("{}", ui::note(t!("install.exposure.nodeport_notice")));

        if let Some(package_manager) = ctx.preflight_state().package_manager() {
          println!(
            "{}",
            ui::note(t!(
              "install.exposure.package_manager_notice",
              package_manager = package_manager.label()
            ))
          );
        }

        if let Some(path) = detect_nginx_binary_path() {
          println!(
            "{}",
            ui::note(t!(
              "install.exposure.nginx_reuse_notice",
              path = path.display().to_string()
            ))
          );
        } else if nginx_service_exists() {
          println!(
            "{}",
            ui::note(t!("install.exposure.nginx_service_reuse_notice"))
          );
        }
      }
    }

    Ok(())
  }

  fn describe(&self, ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    let exposure = ctx.application_exposure().ok_or_else(|| {
      anyhow!("application exposure mode is required before planning gateway setup")
    })?;

    let mut details = vec![
      t!(
        "install.steps.gateway.selected_mode",
        exposure = application_exposure_label(exposure)
      )
      .to_string(),
    ];

    match exposure {
      ApplicationExposureMode::Ingress => {
        details.push(t!("install.steps.gateway.ingress_detail").to_string());
      }
      ApplicationExposureMode::NodePortExternalNginx => {
        details.push(t!("install.steps.gateway.nodeport_detail").to_string());
        details.push(t!("install.steps.gateway.port_reservation").to_string());

        if let Some(path) = detect_nginx_binary_path() {
          details.push(
            t!(
              "install.steps.gateway.nginx_reuse",
              path = path.display().to_string()
            )
            .to_string(),
          );
        } else if nginx_service_exists() {
          details.push(t!("install.steps.gateway.nginx_service_reuse").to_string());
        } else {
          details.push(t!("install.steps.gateway.nginx_install").to_string());
        }
      }
    }

    Ok(InstallStepPlan {
      id: self.id(),
      title: t!("install.steps.gateway.title").to_string(),
      details,
    })
  }

  fn install(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let exposure = ctx
      .as_plan_context()
      .application_exposure()
      .ok_or_else(|| anyhow!("application exposure mode is required before installing gateway"))?;

    ctx.persist_change(
      "install.execution.gateway.mode",
      exposure.as_config_value(),
      |config| config.set_install_step_metadata(self.id(), "mode", exposure.as_config_value()),
    )?;

    if exposure == ApplicationExposureMode::Ingress {
      return Ok(());
    }

    let package_manager = ctx
      .preflight_state()
      .package_manager()
      .or_else(SystemPackageManager::detect)
      .ok_or_else(|| anyhow!("no supported package manager is available for nginx installation"))?;
    let nginx_existed = detect_nginx_binary_path().is_some() || nginx_service_exists();

    if !nginx_existed {
      package_manager.install_nginx(ctx)?;
    }

    let gateway_http_port =
      cluster_gateway_port_metadata(ctx, "gateway_http_port", GATEWAY_HTTP_PORT_CANDIDATES[0])?;
    let gateway_https_port =
      cluster_gateway_port_metadata(ctx, "gateway_https_port", GATEWAY_HTTPS_PORT_CANDIDATES[0])?;

    install_external_nginx_gateway(ctx, gateway_http_port, gateway_https_port)?;
    ctx.run_privileged_command(
      "systemctl",
      &[
        "enable".to_string(),
        "--now".to_string(),
        "nginx.service".to_string(),
      ],
      &[],
    )?;
    let nginx_binary =
      detect_nginx_binary_path().unwrap_or_else(|| PathBuf::from(NGINX_BINARY_DEST));
    ctx.run_privileged_command(
      &nginx_binary.display().to_string(),
      &["-t".to_string()],
      &[],
    )?;
    ctx.run_privileged_command(
      "systemctl",
      &["reload".to_string(), "nginx.service".to_string()],
      &[],
    )?;

    let package_manager_label = package_manager.label().to_string();
    let binary_path = nginx_binary.display().to_string();

    ctx.persist_change("install.execution.gateway.nginx", &binary_path, |config| {
      let changed = config.set_install_step_metadata(
        self.id(),
        "package_manager",
        package_manager_label.clone(),
      );
      let changed =
        config.set_install_step_metadata(self.id(), "binary_path", binary_path.clone()) || changed;
      config.set_install_step_metadata(
        self.id(),
        "installed_by_ret2boot",
        if nginx_existed { "false" } else { "true" },
      ) || changed
    })?;

    Ok(())
  }

  fn uninstall(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let mode = ctx
      .config()
      .install_step_metadata(self.id(), "mode")
      .unwrap_or(ApplicationExposureMode::Ingress.as_config_value());

    if mode != ApplicationExposureMode::NodePortExternalNginx.as_config_value() {
      return Ok(());
    }

    cleanup_external_nginx_gateway(ctx)?;

    if let Some(nginx_binary) = detect_nginx_binary_path() {
      let _ = ctx.run_privileged_command(
        &nginx_binary.display().to_string(),
        &["-t".to_string()],
        &[],
      );
      let _ = ctx.run_privileged_command(
        "systemctl",
        &["reload".to_string(), "nginx.service".to_string()],
        &[],
      );
    }

    let installed_by_ret2boot = ctx
      .config()
      .install_step_metadata(self.id(), "installed_by_ret2boot")
      .is_some_and(|value| value == "true");

    if installed_by_ret2boot {
      let package_manager = ctx
        .config()
        .install_step_metadata(self.id(), "package_manager")
        .and_then(system_package_manager_from_label)
        .or_else(|| ctx.preflight_state().package_manager())
        .or_else(SystemPackageManager::detect)
        .ok_or_else(|| anyhow!("unable to determine package manager for nginx removal"))?;

      let _ = ctx.run_privileged_command(
        "systemctl",
        &[
          "disable".to_string(),
          "--now".to_string(),
          "nginx.service".to_string(),
        ],
        &[],
      );
      package_manager.remove_nginx(ctx)?;
    }

    ctx.persist_change("install.execution.gateway.cleanup", "done", |config| {
      let changed = config.remove_install_step_metadata(self.id(), "mode");
      let changed = config.remove_install_step_metadata(self.id(), "package_manager") || changed;
      let changed = config.remove_install_step_metadata(self.id(), "binary_path") || changed;
      config.remove_install_step_metadata(self.id(), "installed_by_ret2boot") || changed
    })?;

    Ok(())
  }
}

#[derive(Clone)]
struct ClusterInstallSpec {
  role: InstallTargetRole,
  distribution: KubernetesDistribution,
  source: KubernetesInstallSource,
  application_exposure: Option<ApplicationExposureMode>,
  worker_server_url: Option<String>,
  worker_token: Option<String>,
}

impl ClusterInstallSpec {
  fn from_plan_context(ctx: &StepPlanContext<'_>) -> Result<Self> {
    let role = ctx
      .node_role()
      .ok_or_else(|| anyhow!("node role is required before planning cluster installation"))?;
    let distribution = ctx.kubernetes_distribution().ok_or_else(|| {
      anyhow!("kubernetes distribution is required before planning cluster installation")
    })?;
    let source = ctx.kubernetes_source().ok_or_else(|| {
      anyhow!("kubernetes source is required before planning cluster installation")
    })?;
    let application_exposure = ctx.application_exposure();
    let worker_server_url = ctx.worker_server_url().map(str::to_string);
    let worker_token = ctx.worker_token().map(str::to_string);

    if role == InstallTargetRole::ControlPlane && application_exposure.is_none() {
      bail!("application exposure mode is required before planning control-plane installation");
    }

    if role == InstallTargetRole::Worker {
      if worker_server_url.is_none() {
        bail!("worker server URL is required before planning worker cluster installation");
      }
      if worker_token.is_none() {
        bail!("worker join token is required before planning worker cluster installation");
      }
    }

    Ok(Self {
      role,
      distribution,
      source,
      application_exposure,
      worker_server_url,
      worker_token,
    })
  }
}

fn install_k3s(ctx: &mut StepExecutionContext<'_>, spec: &ClusterInstallSpec) -> Result<()> {
  if let Some((http_port, https_port)) = selected_gateway_ports_for_cluster(ctx, spec)? {
    let manifest_path = stage_text_file(
      "k3s-traefik-config",
      "yaml",
      render_k3s_traefik_ports_config(http_port, https_port),
    )?;
    install_staged_file(ctx, &manifest_path, K3S_TRAEFIK_CONFIG_DEST)?;
    let _ = fs::remove_file(&manifest_path);
  }

  let staged = stage_k3s_config(spec)?;
  install_staged_file(ctx, &staged.config, K3S_CONFIG_DEST)?;
  install_staged_file(ctx, &staged.kubelet_config, K3S_KUBELET_CONFIG_DEST)?;

  let script_path = stage_remote_script(k3s_script_url(spec.source), "k3s-install")?;

  let mut envs = vec![(
    "INSTALL_K3S_EXEC".to_string(),
    match spec.role {
      InstallTargetRole::ControlPlane => "server".to_string(),
      InstallTargetRole::Worker => "agent".to_string(),
    },
  )];

  if spec.source == KubernetesInstallSource::ChinaMirror {
    envs.push(("INSTALL_K3S_MIRROR".to_string(), "cn".to_string()));
  }

  let result = ctx.run_privileged_command("sh", &[script_path.display().to_string()], &envs);
  let _ = fs::remove_file(&script_path);
  staged.cleanup();
  result
}

fn uninstall_k3s(ctx: &StepExecutionContext<'_>, spec: &ClusterInstallSpec) -> Result<()> {
  let script_name = match spec.role {
    InstallTargetRole::ControlPlane => "k3s-uninstall.sh",
    InstallTargetRole::Worker => "k3s-agent-uninstall.sh",
  };
  let uninstall_script = find_existing_path(&[
    PathBuf::from("/usr/local/bin").join(script_name),
    PathBuf::from("/opt/bin").join(script_name),
  ])
  .ok_or_else(|| anyhow!("unable to locate `{script_name}` for cleanup"))?;

  ctx.run_privileged_command(&uninstall_script.display().to_string(), &[], &[])?;
  cleanup_k3s_configs(ctx)?;
  cleanup_k3s_gateway_manifests(ctx)
}

fn install_rke2(ctx: &mut StepExecutionContext<'_>, spec: &ClusterInstallSpec) -> Result<()> {
  if let Some((http_port, https_port)) = selected_gateway_ports_for_cluster(ctx, spec)? {
    let traefik_manifest = stage_text_file(
      "rke2-traefik-config",
      "yaml",
      render_rke2_traefik_ports_config(http_port, https_port),
    )?;
    install_staged_file(ctx, &traefik_manifest, RKE2_TRAEFIK_CONFIG_DEST)?;
    let _ = fs::remove_file(&traefik_manifest);

    let ingress_manifest = stage_text_file(
      "rke2-ingress-config",
      "yaml",
      render_rke2_ingress_nginx_ports_config(http_port, https_port),
    )?;
    install_staged_file(ctx, &ingress_manifest, RKE2_INGRESS_NGINX_CONFIG_DEST)?;
    let _ = fs::remove_file(&ingress_manifest);
  }

  let staged = stage_rke2_config(spec)?;
  install_staged_file(ctx, &staged.config, RKE2_CONFIG_DEST)?;
  install_staged_file(ctx, &staged.kubelet_config, RKE2_KUBELET_CONFIG_DEST)?;

  let script_path = stage_remote_script(rke2_script_url(spec.source), "rke2-install")?;

  let mut envs = vec![
    ("INSTALL_RKE2_METHOD".to_string(), "tar".to_string()),
    (
      "INSTALL_RKE2_TYPE".to_string(),
      match spec.role {
        InstallTargetRole::ControlPlane => "server".to_string(),
        InstallTargetRole::Worker => "agent".to_string(),
      },
    ),
  ];

  if spec.source == KubernetesInstallSource::ChinaMirror {
    envs.push(("INSTALL_RKE2_MIRROR".to_string(), "cn".to_string()));
  }

  let install_result =
    ctx.run_privileged_command("sh", &[script_path.display().to_string()], &envs);
  let _ = fs::remove_file(&script_path);
  staged.cleanup();
  install_result?;

  let service_name = match spec.role {
    InstallTargetRole::ControlPlane => "rke2-server.service",
    InstallTargetRole::Worker => "rke2-agent.service",
  };

  ctx.run_privileged_command(
    "systemctl",
    &[
      "enable".to_string(),
      "--now".to_string(),
      service_name.to_string(),
    ],
    &[],
  )
}

fn uninstall_rke2(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let uninstall_script = find_existing_path(&[
    PathBuf::from("/usr/local/bin/rke2-uninstall.sh"),
    PathBuf::from("/opt/rke2/bin/rke2-uninstall.sh"),
    PathBuf::from("/usr/bin/rke2-uninstall.sh"),
  ])
  .ok_or_else(|| anyhow!("unable to locate `rke2-uninstall.sh` for cleanup"))?;

  ctx.run_privileged_command(&uninstall_script.display().to_string(), &[], &[])?;
  cleanup_rke2_configs(ctx)?;
  cleanup_rke2_gateway_manifests(ctx)
}

fn rollback_k3s(ctx: &StepExecutionContext<'_>, spec: &ClusterInstallSpec) -> Result<()> {
  if let Some(script_name) = match spec.role {
    InstallTargetRole::ControlPlane => find_existing_path(&[
      PathBuf::from("/usr/local/bin/k3s-uninstall.sh"),
      PathBuf::from("/opt/bin/k3s-uninstall.sh"),
    ]),
    InstallTargetRole::Worker => find_existing_path(&[
      PathBuf::from("/usr/local/bin/k3s-agent-uninstall.sh"),
      PathBuf::from("/opt/bin/k3s-agent-uninstall.sh"),
    ]),
  } {
    let _ = ctx.run_privileged_command(&script_name.display().to_string(), &[], &[]);
  }

  cleanup_k3s_configs(ctx)?;
  cleanup_k3s_gateway_manifests(ctx)
}

fn rollback_rke2(ctx: &StepExecutionContext<'_>) -> Result<()> {
  if let Some(script_name) = find_existing_path(&[
    PathBuf::from("/usr/local/bin/rke2-uninstall.sh"),
    PathBuf::from("/opt/rke2/bin/rke2-uninstall.sh"),
    PathBuf::from("/usr/bin/rke2-uninstall.sh"),
  ]) {
    let _ = ctx.run_privileged_command(&script_name.display().to_string(), &[], &[]);
  }

  cleanup_rke2_configs(ctx)?;
  cleanup_rke2_gateway_manifests(ctx)
}

fn check_downloader() -> PreflightCheck {
  let available = ["curl", "wget"]
    .into_iter()
    .filter(|binary| command_exists(binary))
    .collect::<Vec<_>>();

  if available.is_empty() {
    return PreflightCheck {
      label: t!("install.preflight.checks.downloader").to_string(),
      detail: t!("install.preflight.details.downloader_missing").to_string(),
      status: PreflightStatus::Failed,
    };
  }

  PreflightCheck {
    label: t!("install.preflight.checks.downloader").to_string(),
    detail: t!(
      "install.preflight.details.downloader_available",
      available = available.join(", ")
    )
    .to_string(),
    status: PreflightStatus::Passed,
  }
}

fn check_systemd() -> PreflightCheck {
  let active = Path::new("/run/systemd/system").is_dir() && command_exists("systemctl");

  PreflightCheck {
    label: t!("install.preflight.checks.systemd").to_string(),
    detail: if active {
      t!("install.preflight.details.systemd_ready").to_string()
    } else {
      t!("install.preflight.details.systemd_missing").to_string()
    },
    status: if active {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Failed
    },
  }
}

fn check_package_manager(package_manager: Option<SystemPackageManager>) -> PreflightCheck {
  PreflightCheck {
    label: t!("install.preflight.checks.package_manager").to_string(),
    detail: match package_manager {
      Some(package_manager) => t!(
        "install.preflight.details.package_manager_ready",
        package_manager = package_manager.label()
      )
      .to_string(),
      None => t!("install.preflight.details.package_manager_missing").to_string(),
    },
    status: if package_manager.is_some() {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Failed
    },
  }
}

fn check_github_connectivity(reachable: bool) -> PreflightCheck {
  PreflightCheck {
    label: t!("install.preflight.checks.github").to_string(),
    detail: if reachable {
      t!("install.preflight.details.github_reachable").to_string()
    } else {
      t!("install.preflight.details.github_unreachable").to_string()
    },
    status: if reachable {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Warning
    },
  }
}

fn check_public_network(public_network: &Option<PublicNetworkIdentity>) -> PreflightCheck {
  PreflightCheck {
    label: t!("install.preflight.checks.public_network").to_string(),
    detail: match public_network {
      Some(identity) => t!(
        "install.preflight.details.public_network_detected",
        public_network = identity.display()
      )
      .to_string(),
      None => t!("install.preflight.details.public_network_unknown").to_string(),
    },
    status: if public_network.is_some() {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Warning
    },
  }
}

fn check_source_connectivity(
  label: String, endpoints: &[EndpointReachability<'_>],
) -> PreflightCheck {
  let reachable = endpoints
    .iter()
    .filter(|endpoint| endpoint.reachable)
    .map(|endpoint| endpoint.label)
    .collect::<Vec<_>>();
  let unreachable = endpoints
    .iter()
    .filter(|endpoint| !endpoint.reachable)
    .map(|endpoint| endpoint.label)
    .collect::<Vec<_>>();

  match (reachable.is_empty(), unreachable.is_empty()) {
    (false, true) => PreflightCheck {
      label,
      detail: t!(
        "install.preflight.details.sources_all_reachable",
        reachable = reachable.join(", ")
      )
      .to_string(),
      status: PreflightStatus::Passed,
    },
    (false, false) => PreflightCheck {
      label,
      detail: t!(
        "install.preflight.details.sources_partial",
        reachable = reachable.join(", "),
        unreachable = unreachable.join(", ")
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
    (true, false) => PreflightCheck {
      label,
      detail: t!(
        "install.preflight.details.sources_missing",
        unreachable = unreachable.join(", ")
      )
      .to_string(),
      status: PreflightStatus::Failed,
    },
    (true, true) => PreflightCheck {
      label,
      detail: t!("install.preflight.details.sources_unknown").to_string(),
      status: PreflightStatus::Failed,
    },
  }
}

fn check_cgroup_memory() -> PreflightCheck {
  let available = cgroup_memory_available();

  PreflightCheck {
    label: t!("install.preflight.checks.cgroup_memory").to_string(),
    detail: if available {
      t!("install.preflight.details.cgroup_memory_ready").to_string()
    } else {
      t!("install.preflight.details.cgroup_memory_missing").to_string()
    },
    status: if available {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Failed
    },
  }
}

fn check_disk_capacity() -> PreflightCheck {
  let result = disk_free_bytes("/var/lib").or_else(|_| disk_free_bytes("/"));

  match result {
    Ok(free_bytes) if free_bytes < PREFLIGHT_MIN_DISK_FREE_BYTES => PreflightCheck {
      label: t!("install.preflight.checks.disk").to_string(),
      detail: t!(
        "install.preflight.details.disk_failed",
        free = format_gib(free_bytes)
      )
      .to_string(),
      status: PreflightStatus::Failed,
    },
    Ok(free_bytes) if free_bytes < PREFLIGHT_WARN_DISK_FREE_BYTES => PreflightCheck {
      label: t!("install.preflight.checks.disk").to_string(),
      detail: t!(
        "install.preflight.details.disk_warning",
        free = format_gib(free_bytes)
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
    Ok(free_bytes) => PreflightCheck {
      label: t!("install.preflight.checks.disk").to_string(),
      detail: t!(
        "install.preflight.details.disk_ready",
        free = format_gib(free_bytes)
      )
      .to_string(),
      status: PreflightStatus::Passed,
    },
    Err(error) => PreflightCheck {
      label: t!("install.preflight.checks.disk").to_string(),
      detail: t!(
        "install.preflight.details.disk_unknown",
        error = error.to_string()
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
  }
}

fn check_memory_capacity() -> PreflightCheck {
  match memory_total_bytes() {
    Ok(total_bytes) if total_bytes < PREFLIGHT_MIN_MEMORY_BYTES => PreflightCheck {
      label: t!("install.preflight.checks.memory").to_string(),
      detail: t!(
        "install.preflight.details.memory_failed",
        total = format_gib(total_bytes)
      )
      .to_string(),
      status: PreflightStatus::Failed,
    },
    Ok(total_bytes) if total_bytes < PREFLIGHT_WARN_MEMORY_BYTES => PreflightCheck {
      label: t!("install.preflight.checks.memory").to_string(),
      detail: t!(
        "install.preflight.details.memory_warning",
        total = format_gib(total_bytes)
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
    Ok(total_bytes) => PreflightCheck {
      label: t!("install.preflight.checks.memory").to_string(),
      detail: t!(
        "install.preflight.details.memory_ready",
        total = format_gib(total_bytes)
      )
      .to_string(),
      status: PreflightStatus::Passed,
    },
    Err(error) => PreflightCheck {
      label: t!("install.preflight.checks.memory").to_string(),
      detail: t!(
        "install.preflight.details.memory_unknown",
        error = error.to_string()
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
  }
}

fn check_port_state() -> PreflightCheck {
  let listening = listening_tcp_ports();
  let all_nodes_in_use = [10250_u16]
    .into_iter()
    .filter(|port| listening.contains(port))
    .collect::<Vec<_>>();
  let control_plane_in_use = [6443_u16, 9345, 2379, 2380]
    .into_iter()
    .filter(|port| listening.contains(port))
    .collect::<Vec<_>>();

  if !all_nodes_in_use.is_empty() {
    return PreflightCheck {
      label: t!("install.preflight.checks.ports").to_string(),
      detail: t!(
        "install.preflight.details.ports_failed",
        ports = format_ports(&all_nodes_in_use)
      )
      .to_string(),
      status: PreflightStatus::Failed,
    };
  }

  if !control_plane_in_use.is_empty() {
    return PreflightCheck {
      label: t!("install.preflight.checks.ports").to_string(),
      detail: t!(
        "install.preflight.details.ports_warning",
        ports = format_ports(&control_plane_in_use)
      )
      .to_string(),
      status: PreflightStatus::Warning,
    };
  }

  PreflightCheck {
    label: t!("install.preflight.checks.ports").to_string(),
    detail: t!("install.preflight.details.ports_ready").to_string(),
    status: PreflightStatus::Passed,
  }
}

fn check_sysctl_state() -> PreflightCheck {
  let sysctls = [
    ("net.ipv4.ip_forward", "/proc/sys/net/ipv4/ip_forward"),
    (
      "net.bridge.bridge-nf-call-iptables",
      "/proc/sys/net/bridge/bridge-nf-call-iptables",
    ),
    (
      "net.bridge.bridge-nf-call-ip6tables",
      "/proc/sys/net/bridge/bridge-nf-call-ip6tables",
    ),
  ];

  let mismatches = sysctls
    .into_iter()
    .filter_map(|(name, path)| match fs::read_to_string(path) {
      Ok(value) => {
        let trimmed = value.trim().to_string();
        if trimmed == "1" {
          None
        } else {
          Some(format!("{name}={trimmed}"))
        }
      }
      Err(_) => Some(format!("{name}=missing")),
    })
    .collect::<Vec<_>>();

  if mismatches.is_empty() {
    return PreflightCheck {
      label: t!("install.preflight.checks.sysctl").to_string(),
      detail: t!("install.preflight.details.sysctl_ready").to_string(),
      status: PreflightStatus::Passed,
    };
  }

  PreflightCheck {
    label: t!("install.preflight.checks.sysctl").to_string(),
    detail: t!(
      "install.preflight.details.sysctl_warning",
      values = mismatches.join(", ")
    )
    .to_string(),
    status: PreflightStatus::Warning,
  }
}

fn check_kernel_feature(label: String, state: KernelFeatureState) -> PreflightCheck {
  let (detail, status) = match state {
    KernelFeatureState::Ready => (
      t!("install.preflight.details.kernel_ready").to_string(),
      PreflightStatus::Passed,
    ),
    KernelFeatureState::Loadable => (
      t!("install.preflight.details.kernel_loadable").to_string(),
      PreflightStatus::Warning,
    ),
    KernelFeatureState::Missing => (
      t!("install.preflight.details.kernel_missing").to_string(),
      PreflightStatus::Failed,
    ),
  };

  PreflightCheck {
    label,
    detail,
    status,
  }
}

fn probe_endpoint(client: &Client, url: &str) -> bool {
  client
    .get(url)
    .header(
      "User-Agent",
      format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
    )
    .send()
    .and_then(|response| response.error_for_status())
    .is_ok()
}

fn probe_public_network(client: &Client) -> Option<PublicNetworkIdentity> {
  client
    .get("https://api.ip.sb/geoip")
    .header(
      "User-Agent",
      format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
    )
    .send()
    .ok()?
    .error_for_status()
    .ok()?
    .json::<PublicNetworkIdentity>()
    .ok()
}

fn endpoint_reachability(label: &'static str, reachable: bool) -> EndpointReachability<'static> {
  EndpointReachability { label, reachable }
}

fn disk_free_bytes(path: &str) -> Result<u64> {
  let path = CString::new(path).context("invalid disk path")?;
  let mut stat = MaybeUninit::<libc::statvfs>::uninit();

  let result = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
  if result != 0 {
    return Err(anyhow!(
      "failed to stat filesystem for `{}`",
      path.to_string_lossy()
    ));
  }

  let stat = unsafe { stat.assume_init() };
  Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

fn memory_total_bytes() -> Result<u64> {
  let contents = fs::read_to_string("/proc/meminfo").context("failed to read /proc/meminfo")?;

  let kibibytes = contents
    .lines()
    .find_map(|line| {
      let value = line.strip_prefix("MemTotal:")?.trim();
      value.split_whitespace().next()?.parse::<u64>().ok()
    })
    .ok_or_else(|| anyhow!("unable to parse MemTotal from /proc/meminfo"))?;

  Ok(kibibytes.saturating_mul(1024))
}

fn listening_tcp_ports() -> BTreeSet<u16> {
  ["/proc/net/tcp", "/proc/net/tcp6"]
    .into_iter()
    .filter_map(|path| fs::read_to_string(path).ok())
    .flat_map(|contents| {
      contents
        .lines()
        .skip(1)
        .filter_map(|line| {
          let columns = line.split_whitespace().collect::<Vec<_>>();
          if columns.get(3).copied() != Some("0A") {
            return None;
          }

          let port = columns.get(1)?.split(':').nth(1)?;
          u16::from_str_radix(port, 16).ok()
        })
        .collect::<Vec<_>>()
    })
    .collect()
}

fn format_ports(ports: &[u16]) -> String {
  ports
    .iter()
    .map(u16::to_string)
    .collect::<Vec<_>>()
    .join(", ")
}

fn format_gib(bytes: u64) -> String {
  format!("{:.1} GiB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
}

fn cgroup_memory_available() -> bool {
  if let Ok(controllers) = fs::read_to_string("/sys/fs/cgroup/cgroup.controllers")
    && controllers
      .split_whitespace()
      .any(|controller| controller == "memory")
  {
    return true;
  }

  fs::read_to_string("/proc/cgroups")
    .ok()
    .map(|contents| {
      contents.lines().any(|line| {
        let columns = line.split_whitespace().collect::<Vec<_>>();
        columns.len() >= 4 && columns[0] == "memory" && columns[3] == "1"
      })
    })
    .unwrap_or(false)
}

fn kernel_feature_state_overlay() -> KernelFeatureState {
  kernel_feature_state(
    "/sys/module/overlay",
    "/proc/filesystems",
    "overlay",
    "overlay",
  )
}

fn kernel_feature_state_br_netfilter() -> KernelFeatureState {
  kernel_feature_state(
    "/sys/module/br_netfilter",
    "/proc/sys/net/bridge/bridge-nf-call-iptables",
    "",
    "br_netfilter",
  )
}

fn kernel_feature_state(
  module_path: &str, marker_path: &str, marker_text: &str, module_name: &str,
) -> KernelFeatureState {
  if Path::new(module_path).exists()
    || (Path::new(marker_path).is_file()
      && (marker_text.is_empty() || file_contains(marker_path, marker_text)))
  {
    return KernelFeatureState::Ready;
  }

  if modprobe_can_load(module_name) {
    return KernelFeatureState::Loadable;
  }

  KernelFeatureState::Missing
}

fn modprobe_can_load(module_name: &str) -> bool {
  command_exists("modprobe")
    && Command::new("modprobe")
      .args(["-n", "-q", module_name])
      .status()
      .map(|status| status.success())
      .unwrap_or(false)
}

fn file_contains(path: &str, needle: &str) -> bool {
  fs::read_to_string(path)
    .map(|contents| contents.contains(needle))
    .unwrap_or(false)
}

fn command_exists(binary: &str) -> bool {
  find_command_path(binary).is_some()
}

fn detect_nginx_binary_path() -> Option<PathBuf> {
  find_existing_path(&[
    PathBuf::from("/usr/sbin/nginx"),
    PathBuf::from("/usr/bin/nginx"),
    PathBuf::from("/sbin/nginx"),
    PathBuf::from("/bin/nginx"),
  ])
  .or_else(|| find_command_path("nginx"))
}

fn nginx_service_exists() -> bool {
  command_exists("systemctl")
    && Command::new("systemctl")
      .args(["cat", "nginx.service"])
      .status()
      .map(|status| status.success())
      .unwrap_or(false)
}

fn find_command_path(binary: &str) -> Option<PathBuf> {
  env::var_os("PATH").and_then(|paths| {
    env::split_paths(&paths).find_map(|dir| {
      let candidate = dir.join(binary);
      candidate.is_file().then_some(candidate)
    })
  })
}

fn system_package_manager_from_label(label: &str) -> Option<SystemPackageManager> {
  match label {
    "apt-get" => Some(SystemPackageManager::Apt),
    "dnf" => Some(SystemPackageManager::Dnf),
    "yum" => Some(SystemPackageManager::Yum),
    "zypper" => Some(SystemPackageManager::Zypper),
    "apk" => Some(SystemPackageManager::Apk),
    "pacman" => Some(SystemPackageManager::Pacman),
    _ => None,
  }
}

fn preflight_status_tag(status: &PreflightStatus) -> String {
  match status {
    PreflightStatus::Passed => {
      ui::status_tag(t!("install.preflight.status.ok"), ui::BadgeTone::Success)
    }
    PreflightStatus::Warning => ui::status_tag(
      t!("install.preflight.status.warning"),
      ui::BadgeTone::Pending,
    ),
    PreflightStatus::Failed => {
      ui::status_tag(t!("install.preflight.status.failed"), ui::BadgeTone::Danger)
    }
  }
}

fn preflight_pending_line(label: &str) -> String {
  format!(
    "{} {}",
    ui::status_tag(
      t!("install.preflight.status.running"),
      ui::BadgeTone::Active
    ),
    label
  )
}

fn preflight_result_line(check: &PreflightCheck) -> String {
  format!(
    "{} {} - {}",
    preflight_status_tag(&check.status),
    check.label,
    check.detail
  )
}

fn run_preflight_check<F>(label: String, run: F) -> PreflightCheck
where
  F: FnOnce() -> PreflightCheck, {
  ui::transient_line(preflight_pending_line(&label));
  let check = run();
  ui::transient_line_done(preflight_result_line(&check));
  check
}

fn stage_remote_script(url: &str, prefix: &str) -> Result<PathBuf> {
  let client = Client::builder()
    .https_only(true)
    .timeout(Duration::from_secs(30))
    .build()
    .context("failed to build install script HTTP client")?;

  let script = client
    .get(url)
    .send()
    .with_context(|| format!("failed to request install script `{url}`"))?
    .error_for_status()
    .with_context(|| format!("install script source `{url}` returned an error status"))?
    .text()
    .with_context(|| format!("failed to read install script `{url}`"))?;

  let path = unique_temp_path(prefix, "sh");
  fs::write(&path, script).with_context(|| format!("failed to write `{}`", path.display()))?;
  Ok(path)
}

fn stage_text_file(prefix: &str, extension: &str, contents: String) -> Result<PathBuf> {
  let path = unique_temp_path(prefix, extension);
  fs::write(&path, contents).with_context(|| format!("failed to write `{}`", path.display()))?;
  Ok(path)
}

fn stage_k3s_config(spec: &ClusterInstallSpec) -> Result<StagedClusterConfig> {
  let config = unique_temp_path("k3s-config", "yaml");
  let kubelet_config = unique_temp_path("k3s-kubelet", "yaml");

  fs::write(&config, render_k3s_config(spec))
    .with_context(|| format!("failed to write `{}`", config.display()))?;
  fs::write(&kubelet_config, render_kubelet_config())
    .with_context(|| format!("failed to write `{}`", kubelet_config.display()))?;

  Ok(StagedClusterConfig {
    config,
    kubelet_config,
  })
}

fn stage_rke2_config(spec: &ClusterInstallSpec) -> Result<StagedClusterConfig> {
  let config = unique_temp_path("rke2-config", "yaml");
  let kubelet_config = unique_temp_path("rke2-kubelet", "yaml");

  fs::write(&config, render_rke2_config(spec))
    .with_context(|| format!("failed to write `{}`", config.display()))?;
  fs::write(&kubelet_config, render_kubelet_config())
    .with_context(|| format!("failed to write `{}`", kubelet_config.display()))?;

  Ok(StagedClusterConfig {
    config,
    kubelet_config,
  })
}

fn install_staged_file(ctx: &StepExecutionContext<'_>, source: &Path, dest: &str) -> Result<()> {
  ctx.run_privileged_command(
    "install",
    &[
      "-D".to_string(),
      "-m".to_string(),
      "600".to_string(),
      source.display().to_string(),
      dest.to_string(),
    ],
    &[],
  )
}

fn cleanup_k3s_configs(ctx: &StepExecutionContext<'_>) -> Result<()> {
  ctx.run_privileged_command(
    "rm",
    &[
      "-f".to_string(),
      K3S_CONFIG_DEST.to_string(),
      K3S_KUBELET_CONFIG_DEST.to_string(),
    ],
    &[],
  )?;
  let _ = ctx.run_privileged_command("rmdir", &["/etc/rancher/k3s".to_string()], &[]);
  Ok(())
}

fn cleanup_rke2_configs(ctx: &StepExecutionContext<'_>) -> Result<()> {
  ctx.run_privileged_command(
    "rm",
    &[
      "-f".to_string(),
      RKE2_CONFIG_DEST.to_string(),
      RKE2_KUBELET_CONFIG_DEST.to_string(),
    ],
    &[],
  )?;
  let _ = ctx.run_privileged_command("rmdir", &["/etc/rancher/rke2".to_string()], &[]);
  Ok(())
}

fn cleanup_k3s_gateway_manifests(ctx: &StepExecutionContext<'_>) -> Result<()> {
  ctx.run_privileged_command(
    "rm",
    &["-f".to_string(), K3S_TRAEFIK_CONFIG_DEST.to_string()],
    &[],
  )?;
  let _ = ctx.run_privileged_command("rmdir", &[K3S_MANIFEST_DIR.to_string()], &[]);
  Ok(())
}

fn cleanup_rke2_gateway_manifests(ctx: &StepExecutionContext<'_>) -> Result<()> {
  ctx.run_privileged_command(
    "rm",
    &[
      "-f".to_string(),
      RKE2_TRAEFIK_CONFIG_DEST.to_string(),
      RKE2_INGRESS_NGINX_CONFIG_DEST.to_string(),
    ],
    &[],
  )?;
  let _ = ctx.run_privileged_command("rmdir", &[RKE2_MANIFEST_DIR.to_string()], &[]);
  Ok(())
}

fn selected_gateway_ports_for_cluster(
  ctx: &mut StepExecutionContext<'_>, spec: &ClusterInstallSpec,
) -> Result<Option<(u16, u16)>> {
  if spec.role != InstallTargetRole::ControlPlane
    || spec.application_exposure != Some(ApplicationExposureMode::NodePortExternalNginx)
  {
    return Ok(None);
  }

  if let (Some(http_port), Some(https_port)) = (
    ctx
      .config()
      .install_step_metadata(InstallStepId::ClusterBootstrap, "gateway_http_port")
      .and_then(|value| value.parse::<u16>().ok()),
    ctx
      .config()
      .install_step_metadata(InstallStepId::ClusterBootstrap, "gateway_https_port")
      .and_then(|value| value.parse::<u16>().ok()),
  ) {
    return Ok(Some((http_port, https_port)));
  }

  let (http_port, https_port) = choose_available_gateway_ports()?;
  ctx.persist_change(
    "install.execution.cluster.gateway_http_port",
    &http_port.to_string(),
    |config| {
      config.set_install_step_metadata(
        InstallStepId::ClusterBootstrap,
        "gateway_http_port",
        http_port.to_string(),
      )
    },
  )?;
  ctx.persist_change(
    "install.execution.cluster.gateway_https_port",
    &https_port.to_string(),
    |config| {
      config.set_install_step_metadata(
        InstallStepId::ClusterBootstrap,
        "gateway_https_port",
        https_port.to_string(),
      )
    },
  )?;

  Ok(Some((http_port, https_port)))
}

fn render_k3s_config(spec: &ClusterInstallSpec) -> String {
  let mut lines = Vec::new();

  if spec.role == InstallTargetRole::ControlPlane {
    lines.push(format!("cluster-cidr: {CLUSTER_CIDR}"));
    lines.push("kube-controller-manager-arg:".to_string());
    lines.push(format!("  - node-cidr-mask-size={NODE_CIDR_MASK_SIZE}"));
  } else {
    lines.push(format!(
      "server: {}",
      yaml_quote(
        spec
          .worker_server_url
          .as_deref()
          .expect("worker URL exists")
      )
    ));
    lines.push(format!(
      "token: {}",
      yaml_quote(spec.worker_token.as_deref().expect("worker token exists"))
    ));
  }

  lines.push("kubelet-arg:".to_string());
  lines.push(format!("  - config={K3S_KUBELET_CONFIG_DEST}"));
  lines.push(String::new());

  lines.join("\n")
}

fn render_rke2_config(spec: &ClusterInstallSpec) -> String {
  let mut lines = Vec::new();

  if spec.role == InstallTargetRole::ControlPlane {
    lines.push(format!("cluster-cidr: {CLUSTER_CIDR}"));
    lines.push("kube-controller-manager-arg:".to_string());
    lines.push(format!("  - node-cidr-mask-size={NODE_CIDR_MASK_SIZE}"));
  } else {
    lines.push(format!(
      "server: {}",
      yaml_quote(
        spec
          .worker_server_url
          .as_deref()
          .expect("worker URL exists")
      )
    ));
    lines.push(format!(
      "token: {}",
      yaml_quote(spec.worker_token.as_deref().expect("worker token exists"))
    ));
  }

  lines.push("kubelet-arg:".to_string());
  lines.push(format!("  - config={RKE2_KUBELET_CONFIG_DEST}"));
  lines.push(String::new());

  lines.join("\n")
}

fn render_kubelet_config() -> String {
  format!(
    "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\nmaxPods: {NODE_MAX_PODS}\n"
  )
}

fn yaml_quote(value: &str) -> String {
  format!("'{}'", value.replace('\'', "''"))
}

fn render_k3s_traefik_ports_config(http_port: u16, https_port: u16) -> String {
  format!(
    "apiVersion: helm.cattle.io/v1\nkind: HelmChartConfig\nmetadata:\n  name: traefik\n  namespace: kube-system\nspec:\n  valuesContent: |-\n    ports:\n      web:\n        exposedPort: {http_port}\n      websecure:\n        exposedPort: {https_port}\n"
  )
}

fn render_rke2_traefik_ports_config(http_port: u16, https_port: u16) -> String {
  format!(
    "apiVersion: helm.cattle.io/v1\nkind: HelmChartConfig\nmetadata:\n  name: rke2-traefik\n  namespace: kube-system\nspec:\n  valuesContent: |-\n    ports:\n      web:\n        exposedPort: {http_port}\n      websecure:\n        exposedPort: {https_port}\n"
  )
}

fn render_rke2_ingress_nginx_ports_config(http_port: u16, https_port: u16) -> String {
  format!(
    "apiVersion: helm.cattle.io/v1\nkind: HelmChartConfig\nmetadata:\n  name: rke2-ingress-nginx\n  namespace: kube-system\nspec:\n  valuesContent: |-\n    controller:\n      service:\n        ports:\n          http: {http_port}\n          https: {https_port}\n"
  )
}

fn choose_available_gateway_ports() -> Result<(u16, u16)> {
  let listening = listening_tcp_ports();

  for http_port in GATEWAY_HTTP_PORT_CANDIDATES {
    if listening.contains(&http_port) {
      continue;
    }

    if let Some(https_port) = GATEWAY_HTTPS_PORT_CANDIDATES
      .into_iter()
      .find(|https_port| !listening.contains(https_port))
    {
      return Ok((http_port, https_port));
    }
  }

  bail!("unable to find available high ports for the kubernetes gateway")
}

fn install_external_nginx_gateway(
  ctx: &StepExecutionContext<'_>, http_port: u16, https_port: u16,
) -> Result<()> {
  install_directory(ctx, "/etc/nginx/sites-available")?;
  install_directory(ctx, "/etc/nginx/sites-enabled")?;
  install_directory(ctx, "/etc/nginx/conf.d")?;
  install_directory(ctx, "/etc/nginx/ret2boot-stream-available")?;
  install_directory(ctx, "/etc/nginx/ret2boot-stream-enabled")?;

  if !nginx_http_sites_enabled_already_included()? {
    let include_path = stage_text_file(
      "nginx-sites-include",
      "conf",
      "include /etc/nginx/sites-enabled/*.conf;\n".to_string(),
    )?;
    install_staged_file(ctx, &include_path, NGINX_SITE_INCLUDE)?;
    let _ = fs::remove_file(&include_path);
  }

  let site_path = stage_text_file(
    "nginx-ret2boot-site",
    "conf",
    render_nginx_http_site(http_port),
  )?;
  install_staged_file(ctx, &site_path, NGINX_SITE_AVAILABLE)?;
  let _ = fs::remove_file(&site_path);
  ensure_symlink(ctx, NGINX_SITE_AVAILABLE, NGINX_SITE_ENABLED)?;

  let stream_path = stage_text_file(
    "nginx-ret2boot-stream",
    "conf",
    render_nginx_stream_site(https_port),
  )?;
  install_staged_file(ctx, &stream_path, NGINX_STREAM_AVAILABLE)?;
  let _ = fs::remove_file(&stream_path);
  ensure_symlink(ctx, NGINX_STREAM_AVAILABLE, NGINX_STREAM_ENABLED)?;

  ensure_nginx_stream_include(ctx)
}

fn cleanup_external_nginx_gateway(ctx: &StepExecutionContext<'_>) -> Result<()> {
  ctx.run_privileged_command(
    "rm",
    &[
      "-f".to_string(),
      NGINX_SITE_ENABLED.to_string(),
      NGINX_SITE_AVAILABLE.to_string(),
      NGINX_SITE_INCLUDE.to_string(),
      NGINX_STREAM_ENABLED.to_string(),
      NGINX_STREAM_AVAILABLE.to_string(),
    ],
    &[],
  )?;
  let _ = remove_nginx_stream_include(ctx);
  let _ = ctx.run_privileged_command("rmdir", &["/etc/nginx/sites-enabled".to_string()], &[]);
  let _ = ctx.run_privileged_command("rmdir", &["/etc/nginx/sites-available".to_string()], &[]);
  let _ = ctx.run_privileged_command(
    "rmdir",
    &["/etc/nginx/ret2boot-stream-enabled".to_string()],
    &[],
  );
  let _ = ctx.run_privileged_command(
    "rmdir",
    &["/etc/nginx/ret2boot-stream-available".to_string()],
    &[],
  );
  Ok(())
}

fn render_nginx_http_site(http_port: u16) -> String {
  format!(
    "server {{\n  listen 80 default_server;\n  listen [::]:80 default_server;\n\n  location / {{\n    proxy_http_version 1.1;\n    proxy_set_header Host $host;\n    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n    proxy_set_header X-Forwarded-Proto $scheme;\n    proxy_pass http://127.0.0.1:{http_port};\n  }}\n}}\n"
  )
}

fn render_nginx_stream_site(https_port: u16) -> String {
  format!(
    "stream {{\n  server {{\n    listen 443;\n    listen [::]:443;\n    proxy_pass 127.0.0.1:{https_port};\n    ssl_preread on;\n  }}\n}}\n"
  )
}

fn install_directory(ctx: &StepExecutionContext<'_>, path: &str) -> Result<()> {
  ctx.run_privileged_command(
    "install",
    &[
      "-d".to_string(),
      "-m".to_string(),
      "755".to_string(),
      path.to_string(),
    ],
    &[],
  )
}

fn ensure_symlink(ctx: &StepExecutionContext<'_>, source: &str, target: &str) -> Result<()> {
  ctx.run_privileged_command(
    "ln",
    &["-sfn".to_string(), source.to_string(), target.to_string()],
    &[],
  )
}

fn ensure_nginx_stream_include(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let contents = fs::read_to_string(NGINX_MAIN_CONF)
    .with_context(|| format!("failed to read `{NGINX_MAIN_CONF}`"))?;

  if contents.contains(NGINX_STREAM_INCLUDE_MARKER) {
    return Ok(());
  }

  let Some(http_index) = contents.find("http {") else {
    bail!("unable to locate the http block in `{NGINX_MAIN_CONF}`")
  };

  let updated = format!(
    "{}{}\n\n{}",
    &contents[..http_index],
    NGINX_STREAM_INCLUDE_MARKER,
    &contents[http_index..]
  );
  let staged = stage_text_file("nginx-main", "conf", updated)?;
  install_staged_file(ctx, &staged, NGINX_MAIN_CONF)?;
  let _ = fs::remove_file(&staged);
  Ok(())
}

fn nginx_http_sites_enabled_already_included() -> Result<bool> {
  Ok(
    fs::read_to_string(NGINX_MAIN_CONF)
      .with_context(|| format!("failed to read `{NGINX_MAIN_CONF}`"))?
      .contains("/etc/nginx/sites-enabled"),
  )
}

fn remove_nginx_stream_include(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let contents = fs::read_to_string(NGINX_MAIN_CONF)
    .with_context(|| format!("failed to read `{NGINX_MAIN_CONF}`"))?;

  if !contents.contains(NGINX_STREAM_INCLUDE_MARKER) {
    return Ok(());
  }

  let updated = contents.replace(&format!("{NGINX_STREAM_INCLUDE_MARKER}\n\n"), "");
  let updated = updated.replace(&format!("{NGINX_STREAM_INCLUDE_MARKER}\n"), "");
  let staged = stage_text_file("nginx-main", "conf", updated)?;
  install_staged_file(ctx, &staged, NGINX_MAIN_CONF)?;
  let _ = fs::remove_file(&staged);
  Ok(())
}

fn cluster_gateway_port_metadata(
  ctx: &StepExecutionContext<'_>, key: &str, fallback: u16,
) -> Result<u16> {
  Ok(
    ctx
      .config()
      .install_step_metadata(InstallStepId::ClusterBootstrap, key)
      .and_then(|value| value.parse::<u16>().ok())
      .unwrap_or(fallback),
  )
}

struct StagedClusterConfig {
  config: PathBuf,
  kubelet_config: PathBuf,
}

impl StagedClusterConfig {
  fn cleanup(&self) {
    let _ = fs::remove_file(&self.config);
    let _ = fs::remove_file(&self.kubelet_config);
  }
}

fn unique_temp_path(prefix: &str, extension: &str) -> PathBuf {
  let stamp = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|duration| duration.as_nanos())
    .unwrap_or_default();

  env::temp_dir().join(format!("{prefix}-{stamp}.{}", extension))
}

fn find_existing_path(candidates: &[PathBuf]) -> Option<PathBuf> {
  candidates.iter().find(|path| path.is_file()).cloned()
}

fn k3s_script_url(source: KubernetesInstallSource) -> &'static str {
  match source {
    KubernetesInstallSource::Official => "https://get.k3s.io",
    KubernetesInstallSource::ChinaMirror => "https://rancher-mirror.rancher.cn/k3s/k3s-install.sh",
  }
}

fn rke2_script_url(source: KubernetesInstallSource) -> &'static str {
  match source {
    KubernetesInstallSource::Official => "https://get.rke2.io",
    KubernetesInstallSource::ChinaMirror => "https://rancher-mirror.rancher.cn/rke2/install.sh",
  }
}

fn kubernetes_distribution_label(distribution: KubernetesDistribution) -> String {
  match distribution {
    KubernetesDistribution::K3s => t!("install.kubernetes.distribution.options.k3s").to_string(),
    KubernetesDistribution::Rke2 => t!("install.kubernetes.distribution.options.rke2").to_string(),
  }
}

fn kubernetes_source_label(
  distribution: KubernetesDistribution, source: KubernetesInstallSource,
) -> String {
  match (distribution, source) {
    (KubernetesDistribution::K3s, KubernetesInstallSource::Official) => {
      t!("install.kubernetes.source.k3s.official").to_string()
    }
    (KubernetesDistribution::K3s, KubernetesInstallSource::ChinaMirror) => {
      t!("install.kubernetes.source.k3s.china_mirror").to_string()
    }
    (KubernetesDistribution::Rke2, KubernetesInstallSource::Official) => {
      t!("install.kubernetes.source.rke2.official").to_string()
    }
    (KubernetesDistribution::Rke2, KubernetesInstallSource::ChinaMirror) => {
      t!("install.kubernetes.source.rke2.china_mirror").to_string()
    }
  }
}

fn application_exposure_label(exposure: ApplicationExposureMode) -> String {
  match exposure {
    ApplicationExposureMode::Ingress => t!("install.exposure.options.ingress").to_string(),
    ApplicationExposureMode::NodePortExternalNginx => {
      t!("install.exposure.options.nodeport_external_nginx").to_string()
    }
  }
}

fn worker_server_url_hint(distribution: KubernetesDistribution) -> String {
  match distribution {
    KubernetesDistribution::K3s => t!("install.kubernetes.worker.server_url_hint.k3s").to_string(),
    KubernetesDistribution::Rke2 => {
      t!("install.kubernetes.worker.server_url_hint.rke2").to_string()
    }
  }
}

#[allow(dead_code)]
fn _ensure_path_exists(path: &Path) -> Result<()> {
  if path.exists() {
    return Ok(());
  }

  bail!("path `{}` does not exist", path.display())
}
