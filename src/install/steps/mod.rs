use std::{
  env, fs,
  path::{Path, PathBuf},
  process::Command,
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::blocking::Client;
use rust_i18n::t;
use tracing::{debug, info};

use crate::{
  config::{
    InstallStepId, InstallTargetRole, KubernetesDistribution, KubernetesInstallSource,
    Ret2BootConfig,
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
  fn collect() -> Result<Self> {
    let client = Client::builder()
      .https_only(true)
      .timeout(Duration::from_secs(5))
      .build()
      .context("failed to build preflight HTTP client")?;

    let github = probe_endpoint(&client, "https://github.com");
    let k3s_official = probe_endpoint(&client, "https://get.k3s.io");
    let k3s_mirror = probe_endpoint(
      &client,
      "https://rancher-mirror.rancher.cn/k3s/k3s-install.sh",
    );
    let rke2_official = probe_endpoint(&client, "https://get.rke2.io");
    let rke2_mirror = probe_endpoint(&client, "https://rancher-mirror.rancher.cn/rke2/install.sh");

    let mut checks = Vec::new();
    checks.push(check_downloader());
    checks.push(check_systemd());
    checks.push(check_github_connectivity(github));
    checks.push(check_source_connectivity(
      t!("install.preflight.checks.k3s_sources").to_string(),
      &[
        endpoint_reachability("get.k3s.io", k3s_official),
        endpoint_reachability("rancher-mirror.rancher.cn/k3s", k3s_mirror),
      ],
    ));
    checks.push(check_source_connectivity(
      t!("install.preflight.checks.rke2_sources").to_string(),
      &[
        endpoint_reachability("get.rke2.io", rke2_official),
        endpoint_reachability("rancher-mirror.rancher.cn/rke2", rke2_mirror),
      ],
    ));
    checks.push(check_cgroup_memory());
    checks.push(check_kernel_feature(
      t!("install.preflight.checks.overlay").to_string(),
      kernel_feature_state_overlay(),
    ));
    checks.push(check_kernel_feature(
      t!("install.preflight.checks.br_netfilter").to_string(),
      kernel_feature_state_br_netfilter(),
    ));

    Ok(Self { checks })
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
    println!("{}", ui::section(t!("install.preflight.title")));

    for check in &self.checks {
      println!(
        "{} {} - {}",
        preflight_status_tag(&check.status),
        check.label,
        check.detail
      );
    }

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
}

pub struct StepPreflightContext<'a> {
  config: &'a Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: &'a str,
}

#[allow(dead_code)]
impl<'a> StepPreflightContext<'a> {
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

  pub fn as_plan_context(&self) -> StepPlanContext<'_> {
    StepPlanContext::new(self.config, self.runtime, self.config_path)
  }

  pub fn run_privileged_command(
    &self, program: &str, args: &[String], envs: &[(String, String)],
  ) -> Result<()> {
    self.runtime.run_privileged_command(program, args, envs)
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
  ]
}

struct PreflightValidationStep;

impl AtomicInstallStep for PreflightValidationStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::PreflightValidation
  }

  fn preflight(&self, _ctx: &mut StepPreflightContext<'_>) -> Result<bool> {
    let report = PreflightReport::collect()?;
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

    let source_default = ctx
      .config()
      .install
      .questionnaire
      .kubernetes
      .source
      .unwrap_or(KubernetesInstallSource::Official)
      .default_index();

    let source_options = KubernetesInstallSource::ALL
      .iter()
      .copied()
      .map(|source| kubernetes_source_label(distribution, source))
      .collect();

    let source = KubernetesInstallSource::ALL[SingleSelectCollector::new(
      t!("install.kubernetes.source.prompt"),
      source_options,
    )
    .with_default(source_default)
    .collect_index()?];

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

#[derive(Clone)]
struct ClusterInstallSpec {
  role: InstallTargetRole,
  distribution: KubernetesDistribution,
  source: KubernetesInstallSource,
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
    let worker_server_url = ctx.worker_server_url().map(str::to_string);
    let worker_token = ctx.worker_token().map(str::to_string);

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
      worker_server_url,
      worker_token,
    })
  }
}

fn install_k3s(ctx: &StepExecutionContext<'_>, spec: &ClusterInstallSpec) -> Result<()> {
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
  cleanup_k3s_configs(ctx)
}

fn install_rke2(ctx: &StepExecutionContext<'_>, spec: &ClusterInstallSpec) -> Result<()> {
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
  cleanup_rke2_configs(ctx)
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

  cleanup_k3s_configs(ctx)
}

fn rollback_rke2(ctx: &StepExecutionContext<'_>) -> Result<()> {
  if let Some(script_name) = find_existing_path(&[
    PathBuf::from("/usr/local/bin/rke2-uninstall.sh"),
    PathBuf::from("/opt/rke2/bin/rke2-uninstall.sh"),
    PathBuf::from("/usr/bin/rke2-uninstall.sh"),
  ]) {
    let _ = ctx.run_privileged_command(&script_name.display().to_string(), &[], &[]);
  }

  cleanup_rke2_configs(ctx)
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

fn endpoint_reachability(label: &'static str, reachable: bool) -> EndpointReachability<'static> {
  EndpointReachability { label, reachable }
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
  env::var_os("PATH")
    .is_some_and(|paths| env::split_paths(&paths).any(|dir| dir.join(binary).is_file()))
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
