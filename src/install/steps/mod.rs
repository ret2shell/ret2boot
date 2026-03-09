use std::{
  env, fs,
  path::{Path, PathBuf},
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
  vec![Box::new(ClusterBootstrapStep)]
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

  if spec.role == InstallTargetRole::Worker {
    envs.push((
      "K3S_URL".to_string(),
      spec
        .worker_server_url
        .clone()
        .expect("worker server URL exists"),
    ));
    envs.push((
      "K3S_TOKEN".to_string(),
      spec.worker_token.clone().expect("worker token exists"),
    ));
  }

  let result = ctx.run_privileged_command("sh", &[script_path.display().to_string()], &envs);
  let _ = fs::remove_file(&script_path);
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

  ctx.run_privileged_command(&uninstall_script.display().to_string(), &[], &[])
}

fn install_rke2(ctx: &StepExecutionContext<'_>, spec: &ClusterInstallSpec) -> Result<()> {
  let script_path = stage_remote_script(rke2_script_url(spec.source), "rke2-install")?;

  if spec.role == InstallTargetRole::Worker {
    let config_path = stage_worker_join_config(spec)?;
    let copy_result = ctx.run_privileged_command(
      "install",
      &[
        "-D".to_string(),
        "-m".to_string(),
        "600".to_string(),
        config_path.display().to_string(),
        "/etc/rancher/rke2/config.yaml".to_string(),
      ],
      &[],
    );
    let _ = fs::remove_file(&config_path);
    copy_result?;
  }

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

  ctx.run_privileged_command(&uninstall_script.display().to_string(), &[], &[])
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

fn stage_worker_join_config(spec: &ClusterInstallSpec) -> Result<PathBuf> {
  let server_url = spec
    .worker_server_url
    .as_deref()
    .ok_or_else(|| anyhow!("worker server URL is required for worker installation"))?;
  let token = spec
    .worker_token
    .as_deref()
    .ok_or_else(|| anyhow!("worker token is required for worker installation"))?;
  let path = unique_temp_path("rke2-agent-config", "yaml");
  let contents = format!("server: {server_url}\ntoken: {token}\n");
  fs::write(&path, contents).with_context(|| format!("failed to write `{}`", path.display()))?;
  Ok(path)
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
