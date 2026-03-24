use std::{
  collections::{BTreeMap, BTreeSet},
  fs::{self, File},
  io::Read,
  net::Ipv4Addr,
  path::{Path, PathBuf},
  thread,
  time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use rust_i18n::t;
use serde::Deserialize;
use serde_yaml::{Deserializer, Value as YamlValue};

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  support::{
    command_exists, find_existing_path, install_directory, install_staged_file, stage_text_file,
    yaml_quote,
  },
};
use crate::{
  config::{
    InstallStepId, InstallTargetRole, KubernetesDistribution, PlatformServiceDeploymentMode,
    PlatformServiceId, PlatformStorageMode,
  },
  install::collectors::{Collector, InputCollector, SingleSelectCollector},
  ui, update,
};

const PLATFORM_VALUES_DEST: &str = "/etc/ret2shell/ret2boot-platform-values.yaml";
const PLATFORM_STORAGE_DEST: &str = "/etc/ret2shell/ret2boot-platform-storage.yaml";
const PLATFORM_NAMESPACE: &str = "ret2shell-platform";
const CHALLENGE_NAMESPACE: &str = "ret2shell-challenge";
const HELM_RELEASE_NAME: &str = "ret2shell";
const HELM_RELEASE_TIMEOUT: &str = "15m0s";
const PLATFORM_INGRESS_PATH: &str = "/";
const PLATFORM_INGRESS_PATH_TYPE: &str = "Prefix";
const INTERNAL_REGISTRY_NODE_PORT: u16 = 30310;
const LOCAL_PATH_PROVISIONER: &str = "rancher.io/local-path";

#[derive(Clone, Copy)]
struct PlatformServiceDefinition {
  id: PlatformServiceId,
  allow_disabled: bool,
  fixed_local_path: bool,
  legacy_local_disk_gib: u32,
  legacy_storage_class: &'static str,
}

const PLATFORM_SERVICE_DEFINITIONS: [PlatformServiceDefinition; 6] = [
  PlatformServiceDefinition {
    id: PlatformServiceId::Platform,
    allow_disabled: false,
    fixed_local_path: true,
    legacy_local_disk_gib: 420,
    legacy_storage_class: "ret2shell-storage-platform",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Database,
    allow_disabled: false,
    fixed_local_path: false,
    legacy_local_disk_gib: 140,
    legacy_storage_class: "ret2shell-storage-database",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Cache,
    allow_disabled: false,
    fixed_local_path: false,
    legacy_local_disk_gib: 10,
    legacy_storage_class: "ret2shell-storage-cache",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Queue,
    allow_disabled: false,
    fixed_local_path: false,
    legacy_local_disk_gib: 10,
    legacy_storage_class: "ret2shell-storage-queue",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Registry,
    allow_disabled: true,
    fixed_local_path: false,
    legacy_local_disk_gib: 300,
    legacy_storage_class: "ret2shell-storage-registry",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Logs,
    allow_disabled: true,
    fixed_local_path: false,
    legacy_local_disk_gib: 3,
    legacy_storage_class: "ret2shell-storage-logs",
  },
];

struct ExternalFieldDefinition {
  key: &'static str,
  label_key: &'static str,
  default: Option<&'static str>,
  secret: bool,
}

pub struct PlatformDeploymentStep;
pub struct WorkerPlatformProbeStep;

#[derive(Clone, Copy)]
pub(crate) enum PlatformSyncMode {
  InstallLatest,
  SyncRecorded,
  UpdateLatest,
}

pub(crate) struct PlatformSyncReport {
  pub release_exists: bool,
  pub chart_changed: bool,
  pub values_changed: bool,
  pub config_changed: bool,
  pub blocked_changed: bool,
  pub storage_changed: bool,
}

impl PlatformSyncReport {
  pub(crate) fn has_changes(&self) -> bool {
    !self.release_exists
      || self.chart_changed
      || self.values_changed
      || self.config_changed
      || self.blocked_changed
      || self.storage_changed
  }
}

struct ChartReference {
  version: String,
  path: PathBuf,
  download_url: String,
  release_url: String,
}

#[derive(Debug, Deserialize)]
struct HelmReleaseSummary {
  name: String,
  chart: String,
}

#[derive(Default)]
struct ClusterReleaseState {
  release_exists: bool,
  chart_version: Option<String>,
  values: Option<YamlValue>,
  config_toml: Option<String>,
  blocked_content: Option<String>,
}

impl PlatformDeploymentStep {
  pub(crate) fn sync_existing(
    &self, ctx: &mut StepExecutionContext<'_>, mode: PlatformSyncMode,
  ) -> Result<PlatformSyncReport> {
    sync_platform_release(self, ctx, mode)
  }
}

impl AtomicInstallStep for PlatformDeploymentStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::PlatformDeployment
  }

  fn should_include(&self, ctx: &StepPlanContext<'_>) -> bool {
    ctx.node_role() == Some(InstallTargetRole::ControlPlane)
  }

  fn collect(&self, ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    print_platform_bootstrap_notice();

    let remaining_disk = ctx.preflight_state().disk_free_gib().ok_or_else(|| {
      anyhow!("unable to detect the remaining disk capacity for platform planning")
    })?;
    ctx.persist_change(
      "install.questionnaire.platform.remaining_disk_gib",
      &remaining_disk.to_string(),
      |config| config.set_platform_remaining_disk_gib(remaining_disk),
    )?;
    println!(
      "{}",
      ui::note(t!(
        "install.platform.detected_remaining_disk",
        remaining = remaining_disk
      ))
    );

    let suggested_requested = ctx
      .config()
      .install
      .questionnaire
      .platform
      .requested_disk_gib
      .unwrap_or_else(|| remaining_disk.min(platform_legacy_total_disk_gib()).max(1));
    let requested_disk = collect_u32_gib_input(
      t!("install.platform.requested_disk.prompt").to_string(),
      suggested_requested.min(remaining_disk).max(1),
      1,
      Some(remaining_disk),
    )?;
    ctx.persist_change(
      "install.questionnaire.platform.requested_disk_gib",
      &requested_disk.to_string(),
      |config| config.set_platform_requested_disk_gib(requested_disk),
    )?;

    println!();
    println!("{}", ui::section(t!("install.platform.services_intro")));

    for definition in PLATFORM_SERVICE_DEFINITIONS {
      collect_platform_service(ctx, &definition)?;
    }

    collect_platform_disk_plan(ctx)?;
    collect_platform_public_host(ctx)?;
    ensure_generated_platform_credentials(ctx)?;

    Ok(())
  }

  fn describe(&self, ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    let summary = platform_plan_summary(ctx)?;
    let _ = render_platform_values_yaml(&summary)?;
    let _ = render_platform_storage_manifest(&summary);
    let mut details = vec![
      t!(
        "install.steps.platform_plan.namespaces",
        platform = PLATFORM_NAMESPACE,
        challenge = CHALLENGE_NAMESPACE
      )
      .to_string(),
      t!(
        "install.steps.platform_plan.public_host",
        host = summary.public_host.as_str()
      )
      .to_string(),
      t!(
        "install.steps.platform_plan.disk_budget",
        remaining = summary.remaining_disk_gib,
        requested = summary.requested_disk_gib,
        allocated = summary.allocated_local_disk_gib
      )
      .to_string(),
    ];

    if summary.unallocated_local_disk_gib > 0 {
      details.push(
        t!(
          "install.steps.platform_plan.unallocated_disk",
          remaining = summary.unallocated_local_disk_gib
        )
        .to_string(),
      );
    }

    for service in &summary.services {
      details.push(format!(
        "{}: {}",
        platform_service_label(service.id),
        platform_service_summary_line(service)
      ));
    }

    Ok(InstallStepPlan {
      id: self.id(),
      title: t!("install.steps.platform_plan.title").to_string(),
      details,
    })
  }

  fn install(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    self
      .sync_existing(ctx, PlatformSyncMode::InstallLatest)
      .map(|_| ())
  }

  fn uninstall(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let helm_envs = helm_envs(&ctx.as_plan_context())?;
    let storage_path = ctx
      .config()
      .install_step_metadata(self.id(), "storage_path")
      .map(str::to_string);

    ctx.run_privileged_command(
      "helm",
      &[
        "uninstall".to_string(),
        HELM_RELEASE_NAME.to_string(),
        "-n".to_string(),
        PLATFORM_NAMESPACE.to_string(),
        "--ignore-not-found".to_string(),
        "--wait".to_string(),
        "--timeout".to_string(),
        "5m0s".to_string(),
      ],
      &helm_envs,
    )?;

    if let Some(storage_path) = storage_path.filter(|path| PathBuf::from(path).is_file()) {
      ClusterAccess::from_plan_context(&ctx.as_plan_context())?
        .delete_manifest(ctx, &storage_path)?;
    }

    ctx.run_privileged_command(
      "rm",
      &[
        "-f".to_string(),
        PLATFORM_VALUES_DEST.to_string(),
        PLATFORM_STORAGE_DEST.to_string(),
      ],
      &[],
    )?;
    ctx.persist_change("install.execution.platform.release", "removed", |config| {
      let changed = config.remove_install_step_metadata(self.id(), "values_path");
      let changed = config.remove_install_step_metadata(self.id(), "storage_path") || changed;
      let changed = config.remove_install_step_metadata(self.id(), "release_name") || changed;
      let changed = config.remove_install_step_metadata(self.id(), "release_namespace") || changed;
      let changed = config.remove_install_step_metadata(self.id(), "chart_version") || changed;
      let changed = config.remove_install_step_metadata(self.id(), "chart_path") || changed;
      let changed = config.remove_install_step_metadata(self.id(), "chart_download_url") || changed;
      config.remove_install_step_metadata(self.id(), "chart_release_url") || changed
    })?;
    Ok(())
  }
}

impl AtomicInstallStep for WorkerPlatformProbeStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::WorkerPlatformProbe
  }

  fn should_include(&self, ctx: &StepPlanContext<'_>) -> bool {
    ctx.node_role() == Some(InstallTargetRole::Worker)
  }

  fn describe(&self, _ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    Ok(InstallStepPlan {
      id: self.id(),
      title: t!("install.steps.worker_probe.title").to_string(),
      details: vec![t!("install.steps.worker_probe.detail").to_string()],
    })
  }

  fn install(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let distribution = ctx
      .as_plan_context()
      .kubernetes_distribution()
      .ok_or_else(|| {
        anyhow!("kubernetes distribution is required before probing platform status")
      })?;

    println!();
    println!("{}", ui::section(t!("install.worker_probe.title")));
    println!("{}", ui::note(t!("install.worker_probe.intro")));

    match distribution {
      KubernetesDistribution::K3s => {
        if !command_exists("k3s") {
          println!("{}", ui::warning(t!("install.worker_probe.cli_missing")));
          return Ok(());
        }

        run_worker_probe_command(
          ctx,
          "k3s",
          &[
            "kubectl".to_string(),
            "-n".to_string(),
            PLATFORM_NAMESPACE.to_string(),
            "get".to_string(),
            "deployment".to_string(),
            "ret2shell-platform".to_string(),
            "-o".to_string(),
            "wide".to_string(),
          ],
          &[],
        )?
      }
      KubernetesDistribution::Rke2 => {
        let kubectl = find_existing_path(&[
          std::path::PathBuf::from("/var/lib/rancher/rke2/bin/kubectl"),
          std::path::PathBuf::from("/usr/local/bin/kubectl"),
        ])
        .ok_or_else(|| anyhow!("unable to locate the rke2 kubectl binary for worker probing"))?;
        run_worker_probe_command(
          ctx,
          &kubectl.display().to_string(),
          &[
            "-n".to_string(),
            PLATFORM_NAMESPACE.to_string(),
            "get".to_string(),
            "deployment".to_string(),
            "ret2shell-platform".to_string(),
            "-o".to_string(),
            "wide".to_string(),
          ],
          &[(
            "KUBECONFIG".to_string(),
            "/etc/rancher/rke2/rke2.yaml".to_string(),
          )],
        )?;
      }
    }

    match distribution {
      KubernetesDistribution::K3s => run_worker_probe_command(
        ctx,
        "k3s",
        &[
          "kubectl".to_string(),
          "-n".to_string(),
          PLATFORM_NAMESPACE.to_string(),
          "get".to_string(),
          "pods".to_string(),
          "-l".to_string(),
          "app.kubernetes.io/component=platform".to_string(),
          "-o".to_string(),
          "wide".to_string(),
        ],
        &[],
      )?,
      KubernetesDistribution::Rke2 => {
        let kubectl = find_existing_path(&[
          std::path::PathBuf::from("/var/lib/rancher/rke2/bin/kubectl"),
          std::path::PathBuf::from("/usr/local/bin/kubectl"),
        ])
        .ok_or_else(|| anyhow!("unable to locate the rke2 kubectl binary for worker probing"))?;
        run_worker_probe_command(
          ctx,
          &kubectl.display().to_string(),
          &[
            "-n".to_string(),
            PLATFORM_NAMESPACE.to_string(),
            "get".to_string(),
            "pods".to_string(),
            "-l".to_string(),
            "app.kubernetes.io/component=platform".to_string(),
            "-o".to_string(),
            "wide".to_string(),
          ],
          &[(
            "KUBECONFIG".to_string(),
            "/etc/rancher/rke2/rke2.yaml".to_string(),
          )],
        )?;
      }
    }

    println!("{}", ui::success(t!("install.worker_probe.done")));

    Ok(())
  }

  fn uninstall(&self, _ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    Ok(())
  }
}

struct PlatformPlanSummary {
  remaining_disk_gib: u32,
  requested_disk_gib: u32,
  allocated_local_disk_gib: u32,
  unallocated_local_disk_gib: u32,
  public_host: String,
  ingress_class_name: String,
  signing_key: String,
  blocked_content: String,
  internal_database_password: String,
  internal_cache_password: String,
  internal_queue_token: String,
  internal_registry_host: String,
  services: Vec<ResolvedPlatformServicePlan>,
}

struct ResolvedPlatformServicePlan {
  id: PlatformServiceId,
  deployment: PlatformServiceDeploymentMode,
  storage_mode: Option<PlatformStorageMode>,
  storage_class_name: Option<String>,
  local_disk_gib: Option<u32>,
  persistence_size_gib: Option<u32>,
  external_values: BTreeMap<String, String>,
  external_summary: Vec<(String, String)>,
}

fn platform_plan_summary(ctx: &StepPlanContext<'_>) -> Result<PlatformPlanSummary> {
  let remaining_disk_gib = ctx
    .config()
    .install
    .questionnaire
    .platform
    .remaining_disk_gib
    .ok_or_else(|| anyhow!("remaining disk capacity is required before planning the platform"))?;
  let requested_disk_gib = ctx
    .config()
    .install
    .questionnaire
    .platform
    .requested_disk_gib
    .ok_or_else(|| anyhow!("requested disk budget is required before planning the platform"))?;
  let public_host = normalize_public_host(
    ctx
      .config()
      .install
      .questionnaire
      .platform
      .public_host
      .as_deref()
      .ok_or_else(|| {
        anyhow!("a public host is required before planning the platform deployment")
      })?,
  )?;
  let ingress_class_name =
    platform_ingress_class(ctx.kubernetes_distribution().ok_or_else(|| {
      anyhow!("kubernetes distribution is required before planning the platform deployment")
    })?)
    .to_string();
  let signing_key = required_platform_secret(
    ctx
      .config()
      .install
      .questionnaire
      .platform
      .signing_key
      .as_deref(),
    "platform signing key",
  )?;
  let blocked_content = ctx
    .config()
    .install
    .questionnaire
    .platform
    .blocked_content
    .clone()
    .unwrap_or_default();

  let services = PLATFORM_SERVICE_DEFINITIONS
    .iter()
    .map(|definition| resolve_platform_service_plan(ctx, definition))
    .collect::<Result<Vec<_>>>()?;
  let internal_database_password = required_internal_secret(
    &services,
    PlatformServiceId::Database,
    ctx
      .config()
      .install
      .questionnaire
      .platform
      .internal_credentials
      .database_password
      .as_deref(),
    "internal postgresql password",
  )?;
  let internal_cache_password = required_internal_secret(
    &services,
    PlatformServiceId::Cache,
    ctx
      .config()
      .install
      .questionnaire
      .platform
      .internal_credentials
      .cache_password
      .as_deref(),
    "internal valkey password",
  )?;
  let internal_queue_token = required_internal_secret(
    &services,
    PlatformServiceId::Queue,
    ctx
      .config()
      .install
      .questionnaire
      .platform
      .internal_credentials
      .queue_token
      .as_deref(),
    "internal nats token",
  )?;
  let allocated_local_disk_gib = services
    .iter()
    .filter_map(|service| service.local_disk_gib)
    .sum();

  if allocated_local_disk_gib > requested_disk_gib {
    bail!("platform local disk allocations exceed the requested disk budget");
  }

  Ok(PlatformPlanSummary {
    remaining_disk_gib,
    requested_disk_gib,
    allocated_local_disk_gib,
    unallocated_local_disk_gib: requested_disk_gib - allocated_local_disk_gib,
    public_host: public_host.clone(),
    ingress_class_name,
    signing_key,
    blocked_content,
    internal_database_password,
    internal_cache_password,
    internal_queue_token,
    internal_registry_host: derive_internal_registry_host(&public_host),
    services,
  })
}

fn resolve_platform_service_plan(
  ctx: &StepPlanContext<'_>, definition: &PlatformServiceDefinition,
) -> Result<ResolvedPlatformServicePlan> {
  let stored = ctx
    .config()
    .platform_service_config(definition.id)
    .cloned()
    .unwrap_or_default();

  let deployment = if definition.fixed_local_path {
    PlatformServiceDeploymentMode::Local
  } else {
    stored.deployment.ok_or_else(|| {
      anyhow!(
        "deployment mode for service `{}` is required before planning the platform",
        definition.id.as_config_value()
      )
    })?
  };

  let storage_mode = if deployment == PlatformServiceDeploymentMode::Local {
    if definition.fixed_local_path {
      Some(PlatformStorageMode::LocalPath)
    } else {
      Some(stored.storage_mode.ok_or_else(|| {
        anyhow!(
          "storage mode for service `{}` is required before planning the platform",
          definition.id.as_config_value()
        )
      })?)
    }
  } else {
    None
  };

  let storage_class_name = match storage_mode {
    Some(PlatformStorageMode::LocalPath) => Some(definition.legacy_storage_class.to_string()),
    Some(PlatformStorageMode::CustomStorageClass) => {
      Some(stored.storage_class_name.clone().ok_or_else(|| {
        anyhow!(
          "custom storage class for service `{}` is required before planning the platform",
          definition.id.as_config_value()
        )
      })?)
    }
    _ => None,
  };

  let external_values = if deployment == PlatformServiceDeploymentMode::External {
    stored.external.clone()
  } else {
    BTreeMap::new()
  };

  let external_summary = if deployment == PlatformServiceDeploymentMode::External {
    external_field_definitions(definition.id)
      .iter()
      .filter(|field| !field.secret)
      .filter_map(|field| {
        external_values
          .get(field.key)
          .filter(|value| !value.trim().is_empty())
          .map(|value| (t!(field.label_key).to_string(), value.clone()))
      })
      .collect()
  } else {
    Vec::new()
  };

  let local_disk_gib = if uses_local_disk(definition.id, deployment, storage_mode) {
    Some(stored.local_disk_gib.ok_or_else(|| {
      anyhow!(
        "local disk allocation for service `{}` is required before planning the platform",
        definition.id.as_config_value()
      )
    })?)
  } else {
    None
  };
  let persistence_size_gib = if deployment == PlatformServiceDeploymentMode::Local {
    Some(match storage_mode {
      Some(PlatformStorageMode::LocalPath) => local_disk_gib
        .ok_or_else(|| anyhow!("local storage size is required before planning the platform"))?,
      Some(PlatformStorageMode::CustomStorageClass) => definition.legacy_local_disk_gib,
      None => {
        bail!(
          "storage mode for service `{}` is required before planning the platform",
          definition.id.as_config_value()
        )
      }
    })
  } else {
    None
  };

  Ok(ResolvedPlatformServicePlan {
    id: definition.id,
    deployment,
    storage_mode,
    storage_class_name,
    local_disk_gib,
    persistence_size_gib,
    external_values,
    external_summary,
  })
}

fn print_platform_bootstrap_notice() {
  println!();
  println!("{}", ui::section(t!("install.platform.resources_intro")));
  println!("{}", ui::note(t!("install.platform.questionnaire_intro")));
  println!("{}", ui::note(t!("install.platform.storage_notice")));
}

fn collect_platform_public_host(ctx: &mut StepQuestionContext<'_>) -> Result<()> {
  let default_host = ctx
    .config()
    .install
    .questionnaire
    .platform
    .public_host
    .clone()
    .or_else(|| {
      ctx
        .preflight_state()
        .public_network_ip()
        .map(str::to_string)
    })
    .unwrap_or_else(|| "ret2shell.local".to_string());
  let public_host = normalize_public_host(
    &InputCollector::new(t!("install.platform.public_host.prompt"))
      .with_default(default_host)
      .collect()?,
  )?;

  ctx.persist_change(
    "install.questionnaire.platform.public_host",
    &public_host,
    |config| config.set_platform_public_host(public_host.clone()),
  )?;

  Ok(())
}

fn ensure_generated_platform_credentials(ctx: &mut StepQuestionContext<'_>) -> Result<()> {
  let signing_key = ctx
    .config()
    .install
    .questionnaire
    .platform
    .signing_key
    .clone();
  let database_password = ctx
    .config()
    .install
    .questionnaire
    .platform
    .internal_credentials
    .database_password
    .clone();
  let cache_password = ctx
    .config()
    .install
    .questionnaire
    .platform
    .internal_credentials
    .cache_password
    .clone();
  let queue_token = ctx
    .config()
    .install
    .questionnaire
    .platform
    .internal_credentials
    .queue_token
    .clone();
  let blocked_content = ctx
    .config()
    .install
    .questionnaire
    .platform
    .blocked_content
    .clone();

  ensure_generated_secret(
    ctx,
    "install.questionnaire.platform.signing_key",
    signing_key.as_deref(),
    32,
    |config, value| config.set_platform_signing_key(value),
  )?;
  ensure_generated_secret(
    ctx,
    "install.questionnaire.platform.database_password",
    database_password.as_deref(),
    24,
    |config, value| config.set_platform_internal_database_password(value),
  )?;
  ensure_generated_secret(
    ctx,
    "install.questionnaire.platform.cache_password",
    cache_password.as_deref(),
    24,
    |config, value| config.set_platform_internal_cache_password(value),
  )?;
  ensure_generated_secret(
    ctx,
    "install.questionnaire.platform.queue_token",
    queue_token.as_deref(),
    24,
    |config, value| config.set_platform_internal_queue_token(value),
  )?;

  if blocked_content.is_none() {
    ctx.persist_change(
      "install.questionnaire.platform.blocked_content",
      "",
      |config| config.set_platform_blocked_content(String::new()),
    )?;
  }

  Ok(())
}

fn ensure_generated_secret<F>(
  ctx: &mut StepQuestionContext<'_>, field: &'static str, current: Option<&str>, bytes: usize,
  update: F,
) -> Result<()>
where
  F: FnOnce(&mut crate::config::Ret2BootConfig, String) -> bool, {
  if current.is_some_and(|value| !value.trim().is_empty()) {
    return Ok(());
  }

  let generated = generate_secret_hex(bytes)?;
  ctx.persist_change(field, "[generated]", |config| {
    update(config, generated.clone())
  })?;

  Ok(())
}

fn generate_secret_hex(bytes: usize) -> Result<String> {
  let mut file = File::open("/dev/urandom").context("failed to open /dev/urandom")?;
  let mut buffer = vec![0_u8; bytes];
  file
    .read_exact(&mut buffer)
    .context("failed to read random bytes from /dev/urandom")?;

  Ok(buffer.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn collect_platform_service(
  ctx: &mut StepQuestionContext<'_>, definition: &PlatformServiceDefinition,
) -> Result<()> {
  println!();
  println!(
    "{}",
    ui::section(t!(
      "install.platform.service_heading",
      service = platform_service_label(definition.id)
    ))
  );
  println!("{}", ui::note(platform_service_description(definition.id)));

  if definition.fixed_local_path {
    ctx.persist_change(
      "install.questionnaire.platform.service.deployment",
      PlatformServiceDeploymentMode::Local.as_config_value(),
      |config| {
        config.set_platform_service_deployment(definition.id, PlatformServiceDeploymentMode::Local)
      },
    )?;
    ctx.persist_change(
      "install.questionnaire.platform.service.storage_mode",
      PlatformStorageMode::LocalPath.as_config_value(),
      |config| {
        config.set_platform_service_storage_mode(definition.id, PlatformStorageMode::LocalPath)
      },
    )?;
    ctx.persist_change(
      "install.questionnaire.platform.service.storage_class_name",
      definition.legacy_storage_class,
      |config| {
        config
          .set_platform_service_storage_class_name(definition.id, definition.legacy_storage_class)
      },
    )?;
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.external",
      "cleared",
      |config| config.clear_platform_service_external_values(definition.id),
    )?;

    println!(
      "{}",
      ui::note(t!("install.platform.service_platform_fixed"))
    );
    return Ok(());
  }

  let deployment_modes = deployment_modes_for_service(definition);
  let deployment_default = ctx
    .config()
    .platform_service_config(definition.id)
    .and_then(|service| service.deployment)
    .unwrap_or(PlatformServiceDeploymentMode::Local);
  let deployment_default_index = deployment_modes
    .iter()
    .position(|mode| *mode == deployment_default)
    .unwrap_or(0);
  let deployment_options = deployment_modes
    .iter()
    .copied()
    .map(platform_service_deployment_label)
    .collect();
  let deployment = deployment_modes[SingleSelectCollector::new(
    t!("install.platform.service_deployment.prompt"),
    deployment_options,
  )
  .with_default(deployment_default_index)
  .collect_index()?];

  ctx.persist_change(
    "install.questionnaire.platform.service.deployment",
    deployment.as_config_value(),
    |config| config.set_platform_service_deployment(definition.id, deployment),
  )?;

  if deployment != PlatformServiceDeploymentMode::Local {
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.storage_mode",
      "cleared",
      |config| config.clear_platform_service_storage_mode(definition.id),
    )?;
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.storage_class_name",
      "cleared",
      |config| config.clear_platform_service_storage_class_name(definition.id),
    )?;
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.local_disk_gib",
      "cleared",
      |config| config.clear_platform_service_local_disk_gib(definition.id),
    )?;
    collect_external_platform_service(ctx, definition)?;
    return Ok(());
  }

  let _ = ctx.persist_change(
    "install.questionnaire.platform.service.external",
    "cleared",
    |config| config.clear_platform_service_external_values(definition.id),
  )?;

  let storage_modes = [
    PlatformStorageMode::LocalPath,
    PlatformStorageMode::CustomStorageClass,
  ];
  let storage_default = ctx
    .config()
    .platform_service_config(definition.id)
    .and_then(|service| service.storage_mode)
    .unwrap_or(PlatformStorageMode::LocalPath);
  let storage_default_index = storage_modes
    .iter()
    .position(|mode| *mode == storage_default)
    .unwrap_or(0);
  let storage_options = storage_modes
    .iter()
    .copied()
    .map(platform_storage_mode_label)
    .collect();
  let storage_mode = storage_modes[SingleSelectCollector::new(
    t!("install.platform.service_storage.prompt"),
    storage_options,
  )
  .with_default(storage_default_index)
  .collect_index()?];

  ctx.persist_change(
    "install.questionnaire.platform.service.storage_mode",
    storage_mode.as_config_value(),
    |config| config.set_platform_service_storage_mode(definition.id, storage_mode),
  )?;

  if storage_mode == PlatformStorageMode::CustomStorageClass {
    let class_prompt = InputCollector::new(t!("install.platform.service_storage_class.prompt"));
    let class_prompt = match ctx
      .config()
      .platform_service_config(definition.id)
      .and_then(|service| service.storage_class_name.clone())
    {
      Some(default) => class_prompt.with_default(default),
      None => class_prompt.with_default(definition.legacy_storage_class),
    };
    let storage_class = class_prompt.collect()?.trim().to_string();

    ctx.persist_change(
      "install.questionnaire.platform.service.storage_class_name",
      &storage_class,
      |config| config.set_platform_service_storage_class_name(definition.id, storage_class.clone()),
    )?;
  } else {
    ctx.persist_change(
      "install.questionnaire.platform.service.storage_class_name",
      definition.legacy_storage_class,
      |config| {
        config
          .set_platform_service_storage_class_name(definition.id, definition.legacy_storage_class)
      },
    )?;
  }

  Ok(())
}

fn collect_external_platform_service(
  ctx: &mut StepQuestionContext<'_>, definition: &PlatformServiceDefinition,
) -> Result<()> {
  let fields = external_field_definitions(definition.id);

  if fields.is_empty() {
    return Ok(());
  }

  println!("{}", ui::note(t!("install.platform.external_intro")));

  for field in fields {
    let prompt = InputCollector::new(t!(field.label_key));
    let prompt = match ctx
      .config()
      .platform_service_external_value(definition.id, field.key)
      .map(str::to_string)
      .or_else(|| field.default.map(str::to_string))
    {
      Some(default) => prompt.with_default(default),
      None => prompt,
    };

    let value = prompt.collect()?.trim().to_string();
    let persisted_value = if field.secret {
      "[redacted]"
    } else {
      value.as_str()
    };

    ctx.persist_change(
      "install.questionnaire.platform.service.external",
      persisted_value,
      |config| config.set_platform_service_external_value(definition.id, field.key, value.clone()),
    )?;
  }

  Ok(())
}

fn collect_platform_disk_plan(ctx: &mut StepQuestionContext<'_>) -> Result<()> {
  let remaining_disk = ctx
    .config()
    .install
    .questionnaire
    .platform
    .remaining_disk_gib
    .ok_or_else(|| anyhow!("remaining disk capacity is required before planning local storage"))?;
  let mut requested_disk = ctx
    .config()
    .install
    .questionnaire
    .platform
    .requested_disk_gib
    .ok_or_else(|| anyhow!("requested disk capacity is required before planning local storage"))?;
  let local_disk_services = local_disk_services(ctx.config());

  if local_disk_services.is_empty() {
    return Ok(());
  }

  let minimum_requested = local_disk_services.len() as u32;
  if requested_disk < minimum_requested {
    println!(
      "{}",
      ui::warning(t!("install.platform.disk_budget_too_small"))
    );
    requested_disk = collect_u32_gib_input(
      t!("install.platform.requested_disk.prompt").to_string(),
      minimum_requested.min(remaining_disk).max(1),
      minimum_requested,
      Some(remaining_disk),
    )?;
    ctx.persist_change(
      "install.questionnaire.platform.requested_disk_gib",
      &requested_disk.to_string(),
      |config| config.set_platform_requested_disk_gib(requested_disk),
    )?;
  }

  loop {
    println!();
    println!("{}", ui::section(t!("install.platform.disk_plan_intro")));

    let defaults = scaled_platform_disk_defaults(&local_disk_services, requested_disk);
    let mut assigned_total = 0_u32;
    let mut answers = Vec::new();

    for definition in &local_disk_services {
      let default_value = ctx
        .config()
        .platform_service_config(definition.id)
        .and_then(|service| service.local_disk_gib)
        .unwrap_or_else(|| {
          *defaults
            .get(&definition.id)
            .unwrap_or(&definition.legacy_local_disk_gib)
        });
      let value = collect_u32_gib_input(
        t!(
          "install.platform.service_disk.prompt",
          service = platform_service_label(definition.id)
        )
        .to_string(),
        default_value.max(1),
        1,
        Some(requested_disk),
      )?;
      assigned_total = assigned_total.saturating_add(value);
      answers.push((definition.id, value));
    }

    if assigned_total > requested_disk {
      println!(
        "{}",
        ui::warning(t!(
          "install.platform.disk_plan_overflow",
          assigned = assigned_total,
          budget = requested_disk
        ))
      );
      continue;
    }

    for (service, value) in answers {
      ctx.persist_change(
        "install.questionnaire.platform.service.local_disk_gib",
        &value.to_string(),
        |config| config.set_platform_service_local_disk_gib(service, value),
      )?;
    }

    for definition in PLATFORM_SERVICE_DEFINITIONS {
      if !local_disk_services
        .iter()
        .any(|item| item.id == definition.id)
      {
        let _ = ctx.persist_change(
          "install.questionnaire.platform.service.local_disk_gib",
          "cleared",
          |config| config.clear_platform_service_local_disk_gib(definition.id),
        )?;
      }
    }

    if assigned_total < requested_disk {
      println!(
        "{}",
        ui::note(t!(
          "install.platform.disk_plan_unused",
          unused = requested_disk - assigned_total
        ))
      );
    }

    break;
  }

  Ok(())
}

fn platform_service_label(service: PlatformServiceId) -> String {
  match service {
    PlatformServiceId::Platform => t!("install.platform.services.platform").to_string(),
    PlatformServiceId::Database => t!("install.platform.services.database").to_string(),
    PlatformServiceId::Cache => t!("install.platform.services.cache").to_string(),
    PlatformServiceId::Queue => t!("install.platform.services.queue").to_string(),
    PlatformServiceId::Registry => t!("install.platform.services.registry").to_string(),
    PlatformServiceId::Logs => t!("install.platform.services.logs").to_string(),
  }
}

fn platform_service_description(service: PlatformServiceId) -> String {
  match service {
    PlatformServiceId::Platform => t!("install.platform.service_desc.platform").to_string(),
    PlatformServiceId::Database => t!("install.platform.service_desc.database").to_string(),
    PlatformServiceId::Cache => t!("install.platform.service_desc.cache").to_string(),
    PlatformServiceId::Queue => t!("install.platform.service_desc.queue").to_string(),
    PlatformServiceId::Registry => t!("install.platform.service_desc.registry").to_string(),
    PlatformServiceId::Logs => t!("install.platform.service_desc.logs").to_string(),
  }
}

fn platform_service_deployment_label(mode: PlatformServiceDeploymentMode) -> String {
  match mode {
    PlatformServiceDeploymentMode::Local => t!("install.platform.deployment.local").to_string(),
    PlatformServiceDeploymentMode::External => {
      t!("install.platform.deployment.external").to_string()
    }
    PlatformServiceDeploymentMode::Disabled => {
      t!("install.platform.deployment.disabled").to_string()
    }
  }
}

fn platform_storage_mode_label(mode: PlatformStorageMode) -> String {
  match mode {
    PlatformStorageMode::LocalPath => t!("install.platform.storage.local_path").to_string(),
    PlatformStorageMode::CustomStorageClass => {
      t!("install.platform.storage.custom_storage_class").to_string()
    }
  }
}

fn deployment_modes_for_service(
  definition: &PlatformServiceDefinition,
) -> Vec<PlatformServiceDeploymentMode> {
  let mut modes = vec![
    PlatformServiceDeploymentMode::Local,
    PlatformServiceDeploymentMode::External,
  ];
  if definition.allow_disabled {
    modes.push(PlatformServiceDeploymentMode::Disabled);
  }
  modes
}

fn external_field_definitions(service: PlatformServiceId) -> &'static [ExternalFieldDefinition] {
  match service {
    PlatformServiceId::Platform => &[],
    PlatformServiceId::Database => &[
      ExternalFieldDefinition {
        key: "host",
        label_key: "install.platform.external.database.host",
        default: Some("database"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "port",
        label_key: "install.platform.external.database.port",
        default: Some("5432"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "database",
        label_key: "install.platform.external.database.name",
        default: Some("ret2shell"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "username",
        label_key: "install.platform.external.database.username",
        default: Some("ret2shell"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "password",
        label_key: "install.platform.external.database.password",
        default: None,
        secret: true,
      },
      ExternalFieldDefinition {
        key: "ssl_mode",
        label_key: "install.platform.external.database.ssl_mode",
        default: Some("disable"),
        secret: false,
      },
    ],
    PlatformServiceId::Cache => &[ExternalFieldDefinition {
      key: "url",
      label_key: "install.platform.external.cache.url",
      default: Some("redis://cache:6379"),
      secret: false,
    }],
    PlatformServiceId::Queue => &[
      ExternalFieldDefinition {
        key: "host",
        label_key: "install.platform.external.queue.host",
        default: Some("message_queue"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "port",
        label_key: "install.platform.external.queue.port",
        default: Some("4222"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "token",
        label_key: "install.platform.external.queue.token",
        default: None,
        secret: true,
      },
    ],
    PlatformServiceId::Registry => &[
      ExternalFieldDefinition {
        key: "external",
        label_key: "install.platform.external.registry.external",
        default: Some("registry.example.com:5000"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "server",
        label_key: "install.platform.external.registry.server",
        default: Some("registry.internal:5000"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "insecure",
        label_key: "install.platform.external.registry.insecure",
        default: Some("false"),
        secret: false,
      },
      ExternalFieldDefinition {
        key: "username",
        label_key: "install.platform.external.registry.username",
        default: None,
        secret: false,
      },
      ExternalFieldDefinition {
        key: "password",
        label_key: "install.platform.external.registry.password",
        default: None,
        secret: true,
      },
    ],
    PlatformServiceId::Logs => &[ExternalFieldDefinition {
      key: "endpoint",
      label_key: "install.platform.external.logs.endpoint",
      default: Some("http://logs.example.com:9428"),
      secret: false,
    }],
  }
}

fn local_disk_services(config: &crate::config::Ret2BootConfig) -> Vec<PlatformServiceDefinition> {
  PLATFORM_SERVICE_DEFINITIONS
    .iter()
    .filter_map(|definition| {
      let deployment = if definition.fixed_local_path {
        PlatformServiceDeploymentMode::Local
      } else {
        config.platform_service_config(definition.id)?.deployment?
      };
      let storage_mode = if definition.fixed_local_path {
        Some(PlatformStorageMode::LocalPath)
      } else {
        config.platform_service_config(definition.id)?.storage_mode
      };

      uses_local_disk(definition.id, deployment, storage_mode).then_some(*definition)
    })
    .collect()
}

fn uses_local_disk(
  service: PlatformServiceId, deployment: PlatformServiceDeploymentMode,
  storage_mode: Option<PlatformStorageMode>,
) -> bool {
  if deployment != PlatformServiceDeploymentMode::Local {
    return false;
  }

  let definition = PLATFORM_SERVICE_DEFINITIONS
    .iter()
    .find(|definition| definition.id == service)
    .expect("platform service definition exists");
  definition.fixed_local_path || storage_mode == Some(PlatformStorageMode::LocalPath)
}

fn platform_legacy_total_disk_gib() -> u32 {
  PLATFORM_SERVICE_DEFINITIONS
    .iter()
    .map(|definition| definition.legacy_local_disk_gib)
    .sum()
}

fn scaled_platform_disk_defaults(
  services: &[PlatformServiceDefinition], target_total_gib: u32,
) -> BTreeMap<PlatformServiceId, u32> {
  let minimum_total = services.len() as u32;
  let extra_total = target_total_gib.saturating_sub(minimum_total);
  let base_total = services
    .iter()
    .map(|definition| definition.legacy_local_disk_gib as u64)
    .sum::<u64>()
    .max(1);
  let mut remaining_extra = extra_total;
  let mut allocations = BTreeMap::new();

  for (index, definition) in services.iter().enumerate() {
    let extra = if index + 1 == services.len() {
      remaining_extra
    } else {
      let scaled = (definition.legacy_local_disk_gib as u64 * extra_total as u64) / base_total;
      let scaled = scaled.min(remaining_extra as u64) as u32;
      remaining_extra -= scaled;
      scaled
    };

    allocations.insert(definition.id, 1 + extra);
  }

  allocations
}

fn collect_u32_gib_input(
  prompt: String, default_value: u32, min_value: u32, max_value: Option<u32>,
) -> Result<u32> {
  let mut default_value = default_value.max(min_value);
  if let Some(max_value) = max_value {
    default_value = default_value.min(max_value);
  }

  loop {
    let value = InputCollector::new(prompt.clone())
      .with_default(default_value.to_string())
      .collect()?;
    let trimmed = value.trim();

    let Ok(parsed) = trimmed.parse::<u32>() else {
      println!(
        "{}",
        ui::warning(t!("install.platform.input.invalid_integer"))
      );
      continue;
    };

    if parsed < min_value {
      println!(
        "{}",
        ui::warning(t!("install.platform.input.too_small", minimum = min_value))
      );
      continue;
    }

    if let Some(max_value) = max_value
      && parsed > max_value
    {
      println!(
        "{}",
        ui::warning(t!("install.platform.input.too_large", maximum = max_value))
      );
      continue;
    }

    return Ok(parsed);
  }
}

fn run_worker_probe_command(
  ctx: &StepExecutionContext<'_>, program: &str, args: &[String], envs: &[(String, String)],
) -> Result<()> {
  match ctx.run_privileged_command(program, args, envs) {
    Ok(()) => Ok(()),
    Err(error) => {
      let error_text = error.to_string();
      println!("{}", ui::warning(t!("install.worker_probe.failed")));
      println!(
        "{}",
        ui::note(t!(
          "install.worker_probe.failed_detail",
          error = error_text.as_str()
        ))
      );
      Ok(())
    }
  }
}

fn sync_platform_release(
  step: &PlatformDeploymentStep, ctx: &mut StepExecutionContext<'_>, mode: PlatformSyncMode,
) -> Result<PlatformSyncReport> {
  let summary = platform_plan_summary(&ctx.as_plan_context())?;
  let cluster_access = ClusterAccess::from_plan_context(&ctx.as_plan_context())?;
  let helm_envs = helm_envs(&ctx.as_plan_context())?;
  let chart = resolve_chart_reference(ctx, step, mode)?;

  install_directory(ctx, "/etc/ret2shell")?;
  cluster_access.wait_for_nodes_ready(ctx)?;

  let desired_values = render_platform_values_yaml(&summary)?;
  let _ = sync_managed_file(ctx, PLATFORM_VALUES_DEST, &desired_values)?;
  let storage_changed = sync_storage_manifest(
    ctx,
    step,
    &cluster_access,
    render_platform_storage_manifest(&summary),
  )?;

  let rendered_artifacts = render_chart_artifacts(ctx, &chart, &helm_envs)?;
  let cluster_state = query_cluster_release_state(ctx, &cluster_access, &helm_envs)?;
  let report = PlatformSyncReport {
    release_exists: cluster_state.release_exists,
    chart_changed: cluster_state.chart_version.as_deref() != Some(chart.version.as_str()),
    values_changed: cluster_state.values != Some(parse_yaml_value(&desired_values)?),
    config_changed: cluster_state.config_toml.as_deref()
      != Some(rendered_artifacts.config_toml.as_str()),
    blocked_changed: cluster_state.blocked_content.as_deref()
      != Some(rendered_artifacts.blocked_content.as_str()),
    storage_changed,
  };

  persist_platform_chart_metadata(step, ctx, &chart)?;

  if !report.has_changes() {
    return Ok(report);
  }

  ctx.run_privileged_command(
    "helm",
    &[
      "upgrade".to_string(),
      "--install".to_string(),
      HELM_RELEASE_NAME.to_string(),
      chart.path.display().to_string(),
      "-n".to_string(),
      PLATFORM_NAMESPACE.to_string(),
      "--create-namespace".to_string(),
      "-f".to_string(),
      PLATFORM_VALUES_DEST.to_string(),
      "--wait".to_string(),
      "--timeout".to_string(),
      HELM_RELEASE_TIMEOUT.to_string(),
    ],
    &helm_envs,
  )?;

  if report.release_exists {
    cluster_access.restart_release_workloads(ctx)?;
  }

  Ok(report)
}

fn resolve_chart_reference(
  ctx: &StepExecutionContext<'_>, step: &PlatformDeploymentStep, mode: PlatformSyncMode,
) -> Result<ChartReference> {
  match mode {
    PlatformSyncMode::InstallLatest | PlatformSyncMode::UpdateLatest => {
      let chart = update::download_ret2shell_chart()?;
      copy_chart_to_system_cache(
        ctx,
        ChartReference {
          version: chart.version,
          path: chart.path,
          download_url: chart.download_url,
          release_url: chart.release_url,
        },
      )
    }
    PlatformSyncMode::SyncRecorded => {
      let chart_version = ctx
        .config()
        .install_step_metadata(step.id(), "chart_version")
        .map(str::to_string)
        .ok_or_else(|| {
          anyhow!("the installed ret2shell chart version is unknown; run `ret2boot update` first")
        })?;
      let configured_chart_path = ctx
        .config()
        .install_step_metadata(step.id(), "chart_path")
        .map(PathBuf::from)
        .ok_or_else(|| {
          anyhow!("the cached ret2shell chart path is missing; run `ret2boot update` first")
        })?;
      let chart_path = if configured_chart_path.is_file() {
        configured_chart_path
      } else {
        let system_chart_path = system_chart_cache_path(&chart_version);

        if system_chart_path.is_file() {
          system_chart_path
        } else {
          bail!(
            "the cached ret2shell chart `{}` is missing; run `ret2boot update` first",
            configured_chart_path.display()
          );
        }
      };

      Ok(ChartReference {
        version: chart_version,
        path: chart_path,
        download_url: ctx
          .config()
          .install_step_metadata(step.id(), "chart_download_url")
          .unwrap_or_default()
          .to_string(),
        release_url: ctx
          .config()
          .install_step_metadata(step.id(), "chart_release_url")
          .unwrap_or_default()
          .to_string(),
      })
    }
  }
}

fn copy_chart_to_system_cache(
  ctx: &StepExecutionContext<'_>, chart: ChartReference,
) -> Result<ChartReference> {
  let system_chart_path = system_chart_cache_path(&chart.version);
  if !chart_cache_copy_required(&chart.path, &system_chart_path) {
    return Ok(ChartReference {
      path: system_chart_path,
      ..chart
    });
  }

  let system_chart_dir = system_chart_path.parent().expect("chart cache has parent");

  install_directory(ctx, &system_chart_dir.display().to_string())?;
  ctx.run_privileged_command(
    "install",
    &[
      "-m".to_string(),
      "644".to_string(),
      chart.path.display().to_string(),
      system_chart_path.display().to_string(),
    ],
    &[],
  )?;

  Ok(ChartReference {
    path: system_chart_path,
    ..chart
  })
}

fn chart_cache_copy_required(source_path: &Path, target_path: &Path) -> bool {
  source_path != target_path
}

fn system_chart_cache_path(version: &str) -> PathBuf {
  update::system_cache_dir_path()
    .join("charts")
    .join("ret2shell")
    .join(version)
    .join(format!("ret2shell-{version}.tgz"))
}

fn persist_platform_chart_metadata(
  step: &PlatformDeploymentStep, ctx: &mut StepExecutionContext<'_>, chart: &ChartReference,
) -> Result<()> {
  ctx.persist_change(
    "install.execution.platform.values",
    PLATFORM_VALUES_DEST,
    |config| config.set_install_step_metadata(step.id(), "values_path", PLATFORM_VALUES_DEST),
  )?;
  ctx.persist_change(
    "install.execution.platform.release",
    HELM_RELEASE_NAME,
    |config| {
      let changed = config.set_install_step_metadata(step.id(), "release_name", HELM_RELEASE_NAME);
      let changed =
        config.set_install_step_metadata(step.id(), "release_namespace", PLATFORM_NAMESPACE)
          || changed;
      let changed =
        config.set_install_step_metadata(step.id(), "chart_version", chart.version.clone())
          || changed;
      let changed =
        config.set_install_step_metadata(step.id(), "chart_path", chart.path.display().to_string())
          || changed;
      let changed = config.set_install_step_metadata(
        step.id(),
        "chart_download_url",
        chart.download_url.clone(),
      ) || changed;
      config.set_install_step_metadata(step.id(), "chart_release_url", chart.release_url.clone())
        || changed
    },
  )?;

  Ok(())
}

fn sync_managed_file(ctx: &StepExecutionContext<'_>, dest: &str, contents: &str) -> Result<bool> {
  if read_privileged_text_file(ctx, dest)?.as_deref() == Some(contents) {
    return Ok(false);
  }

  let staged = stage_text_file("ret2boot-managed", "yaml", contents.to_string())?;
  install_staged_file(ctx, &staged, dest)?;
  let _ = fs::remove_file(&staged);

  Ok(true)
}

fn sync_storage_manifest(
  ctx: &mut StepExecutionContext<'_>, step: &PlatformDeploymentStep,
  cluster_access: &ClusterAccess, desired_storage_manifest: Option<String>,
) -> Result<bool> {
  let previous_storage_path = ctx
    .config()
    .install_step_metadata(step.id(), "storage_path")
    .map(str::to_string);
  let previous_storage_contents = previous_storage_path
    .as_deref()
    .filter(|path| Path::new(path).is_file())
    .map(|path| read_privileged_text_file(ctx, path))
    .transpose()?
    .flatten();
  let desired_storage_classes = desired_storage_manifest
    .as_deref()
    .map(extract_storage_class_names)
    .transpose()?
    .unwrap_or_default();
  let mut missing_storage_classes = false;
  for storage_class in &desired_storage_classes {
    if !cluster_access.storage_class_exists(ctx, storage_class)? {
      missing_storage_classes = true;
      break;
    }
  }
  let manifest_changed =
    previous_storage_contents.as_deref() != desired_storage_manifest.as_deref();

  if manifest_changed
    && let Some(previous_storage_path) = previous_storage_path
      .as_deref()
      .filter(|path| Path::new(path).is_file())
  {
    cluster_access.delete_manifest(ctx, previous_storage_path)?;
  }

  match desired_storage_manifest {
    Some(storage_manifest) => {
      let file_changed = sync_managed_file(ctx, PLATFORM_STORAGE_DEST, &storage_manifest)?;

      if manifest_changed || file_changed || missing_storage_classes {
        cluster_access.apply_manifest(ctx, PLATFORM_STORAGE_DEST)?;
      }

      ctx.persist_change(
        "install.execution.platform.storage",
        PLATFORM_STORAGE_DEST,
        |config| config.set_install_step_metadata(step.id(), "storage_path", PLATFORM_STORAGE_DEST),
      )?;

      Ok(manifest_changed || file_changed || missing_storage_classes)
    }
    None => {
      if let Some(previous_storage_path) = previous_storage_path
        .as_deref()
        .filter(|path| Path::new(path).is_file())
      {
        cluster_access.delete_manifest(ctx, previous_storage_path)?;
      }

      let removed_file = if Path::new(PLATFORM_STORAGE_DEST).is_file() {
        ctx.run_privileged_command(
          "rm",
          &["-f".to_string(), PLATFORM_STORAGE_DEST.to_string()],
          &[],
        )?;
        true
      } else {
        false
      };

      ctx.persist_change("install.execution.platform.storage", "removed", |config| {
        config.remove_install_step_metadata(step.id(), "storage_path")
      })?;

      Ok(manifest_changed || removed_file)
    }
  }
}

fn extract_storage_class_names(manifest: &str) -> Result<Vec<String>> {
  let documents = Deserializer::from_str(manifest)
    .map(YamlValue::deserialize)
    .collect::<std::result::Result<Vec<_>, _>>()
    .context("failed to parse the rendered storage manifest")?;

  Ok(
    documents
      .into_iter()
      .filter(|document| document["kind"].as_str() == Some("StorageClass"))
      .filter_map(|document| document["metadata"]["name"].as_str().map(str::to_string))
      .collect(),
  )
}

fn read_privileged_text_file(ctx: &StepExecutionContext<'_>, path: &str) -> Result<Option<String>> {
  if !Path::new(path).is_file() {
    return Ok(None);
  }

  ctx
    .run_privileged_command_capture("cat", &[path.to_string()], &[])
    .map(Some)
}

fn query_cluster_release_state(
  ctx: &StepExecutionContext<'_>, cluster_access: &ClusterAccess, helm_envs: &[(String, String)],
) -> Result<ClusterReleaseState> {
  let Some(release) = current_helm_release(ctx, helm_envs)? else {
    return Ok(ClusterReleaseState::default());
  };

  Ok(ClusterReleaseState {
    release_exists: true,
    chart_version: parse_release_chart_version(&release.chart),
    values: Some(current_release_values(ctx, helm_envs)?),
    config_toml: cluster_access.capture_namespaced_object_template(
      ctx,
      PLATFORM_NAMESPACE,
      "secret",
      "ret2shell-config",
      "{{index .data \"config.toml\" | base64decode}}",
    )?,
    blocked_content: cluster_access.capture_namespaced_object_template(
      ctx,
      PLATFORM_NAMESPACE,
      "configmap",
      "ret2shell-blocked",
      "{{index .data \"blocked.txt\"}}",
    )?,
  })
}

fn current_helm_release(
  ctx: &StepExecutionContext<'_>, helm_envs: &[(String, String)],
) -> Result<Option<HelmReleaseSummary>> {
  let output = ctx.run_privileged_command_capture(
    "helm",
    &[
      "list".to_string(),
      "-n".to_string(),
      PLATFORM_NAMESPACE.to_string(),
      "--filter".to_string(),
      format!("^{HELM_RELEASE_NAME}$"),
      "-o".to_string(),
      "json".to_string(),
    ],
    helm_envs,
  )?;
  let releases: Vec<HelmReleaseSummary> =
    serde_json::from_str(output.trim()).context("failed to parse `helm list` output")?;

  Ok(
    releases
      .into_iter()
      .find(|release| release.name == HELM_RELEASE_NAME),
  )
}

fn current_release_values(
  ctx: &StepExecutionContext<'_>, helm_envs: &[(String, String)],
) -> Result<YamlValue> {
  let output = ctx.run_privileged_command_capture(
    "helm",
    &[
      "get".to_string(),
      "values".to_string(),
      HELM_RELEASE_NAME.to_string(),
      "-n".to_string(),
      PLATFORM_NAMESPACE.to_string(),
      "-o".to_string(),
      "yaml".to_string(),
    ],
    helm_envs,
  )?;

  parse_yaml_value(&output)
}

struct RenderedChartArtifacts {
  config_toml: String,
  blocked_content: String,
}

fn render_chart_artifacts(
  ctx: &StepExecutionContext<'_>, chart: &ChartReference, helm_envs: &[(String, String)],
) -> Result<RenderedChartArtifacts> {
  let output = ctx.run_privileged_command_capture(
    "helm",
    &[
      "template".to_string(),
      HELM_RELEASE_NAME.to_string(),
      chart.path.display().to_string(),
      "-n".to_string(),
      PLATFORM_NAMESPACE.to_string(),
      "-f".to_string(),
      PLATFORM_VALUES_DEST.to_string(),
    ],
    helm_envs,
  )?;
  let documents = Deserializer::from_str(&output)
    .map(YamlValue::deserialize)
    .collect::<std::result::Result<Vec<_>, _>>()
    .context("failed to parse `helm template` output")?;
  let mut config_toml = None;
  let mut blocked_content = None;

  for document in documents {
    let kind = document["kind"].as_str();
    let name = document["metadata"]["name"].as_str();

    match (kind, name) {
      (Some("Secret"), Some("ret2shell-config")) => {
        config_toml = document["stringData"]["config.toml"]
          .as_str()
          .map(str::to_string);
      }
      (Some("ConfigMap"), Some("ret2shell-blocked")) => {
        blocked_content = Some(
          document["data"]["blocked.txt"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        );
      }
      _ => {}
    }
  }

  Ok(RenderedChartArtifacts {
    config_toml: config_toml.ok_or_else(|| {
      anyhow!("the rendered ret2shell chart did not contain the generated platform config secret")
    })?,
    blocked_content: blocked_content.ok_or_else(|| {
      anyhow!("the rendered ret2shell chart did not contain the generated blocked configmap")
    })?,
  })
}

fn parse_yaml_value(contents: &str) -> Result<YamlValue> {
  if contents.trim().is_empty() {
    return Ok(YamlValue::Null);
  }

  serde_yaml::from_str(contents).context("failed to parse yaml content")
}

fn parse_release_chart_version(chart: &str) -> Option<String> {
  chart.strip_prefix("ret2shell-").map(str::to_string)
}

fn render_platform_values_yaml(summary: &PlatformPlanSummary) -> Result<String> {
  let platform = planned_service(summary, PlatformServiceId::Platform)?;
  let database = planned_service(summary, PlatformServiceId::Database)?;
  let cache = planned_service(summary, PlatformServiceId::Cache)?;
  let queue = planned_service(summary, PlatformServiceId::Queue)?;
  let registry = planned_service(summary, PlatformServiceId::Registry)?;
  let logs = planned_service(summary, PlatformServiceId::Logs)?;

  let mut lines = vec!["platform:".to_string()];
  lines.push("  exposure:".to_string());
  lines.push("    type: ingress".to_string());
  lines.push("  ingress:".to_string());
  lines.push(format!(
    "    className: {}",
    yaml_quote(&summary.ingress_class_name)
  ));
  lines.push("    hosts:".to_string());
  lines.push("      -".to_string());
  if !public_host_uses_catch_all_ingress(&summary.public_host) {
    lines.push(format!(
      "        host: {}",
      yaml_quote(&summary.public_host)
    ));
  }
  lines.push("        paths:".to_string());
  lines.push(format!(
    "          - path: {}",
    yaml_quote(PLATFORM_INGRESS_PATH)
  ));
  lines.push(format!(
    "            pathType: {PLATFORM_INGRESS_PATH_TYPE}"
  ));
  lines.push("  config:".to_string());
  lines.push("    auth:".to_string());
  lines.push(format!(
    "      signingKey: {}",
    yaml_quote(&summary.signing_key)
  ));
  lines.push("    server:".to_string());
  lines.push(format!(
    "      externalDomain: {}",
    yaml_quote(&summary.public_host)
  ));
  lines.push("      externalHttps: false".to_string());
  lines.push("  blocked:".to_string());
  push_yaml_string_field(&mut lines, "    ", "content", &summary.blocked_content);
  append_local_persistence(&mut lines, "  ", platform)?;

  lines.push(String::new());
  lines.push("postgresql:".to_string());
  match database.deployment {
    PlatformServiceDeploymentMode::Local => {
      lines.push("  mode: internal".to_string());
      lines.push("  auth:".to_string());
      lines.push(format!(
        "    password: {}",
        yaml_quote(&summary.internal_database_password)
      ));
      append_local_persistence(&mut lines, "  ", database)?;
    }
    PlatformServiceDeploymentMode::External => {
      lines.push("  mode: external".to_string());
      lines.push("  external:".to_string());
      lines.push(format!(
        "    host: {}",
        yaml_quote(required_external_value(database, "host")?)
      ));
      lines.push(format!(
        "    port: {}",
        parse_external_u16(database, "port")?
      ));
      lines.push(format!(
        "    database: {}",
        yaml_quote(required_external_value(database, "database")?)
      ));
      lines.push(format!(
        "    username: {}",
        yaml_quote(required_external_value(database, "username")?)
      ));
      lines.push(format!(
        "    password: {}",
        yaml_quote(required_external_value(database, "password")?)
      ));
      lines.push(format!(
        "    sslMode: {}",
        yaml_quote(required_external_value(database, "ssl_mode")?)
      ));
    }
    PlatformServiceDeploymentMode::Disabled => {
      bail!("postgresql cannot be disabled in the ret2shell helm deployment")
    }
  }

  lines.push(String::new());
  lines.push("valkey:".to_string());
  match cache.deployment {
    PlatformServiceDeploymentMode::Local => {
      lines.push("  mode: internal".to_string());
      lines.push("  auth:".to_string());
      lines.push("    enabled: true".to_string());
      lines.push(format!(
        "    password: {}",
        yaml_quote(&summary.internal_cache_password)
      ));
      append_local_persistence(&mut lines, "  ", cache)?;
    }
    PlatformServiceDeploymentMode::External => {
      lines.push("  mode: external".to_string());
      lines.push("  external:".to_string());
      lines.push(format!(
        "    url: {}",
        yaml_quote(required_external_value(cache, "url")?)
      ));
    }
    PlatformServiceDeploymentMode::Disabled => {
      bail!("valkey cannot be disabled in the ret2shell helm deployment")
    }
  }

  lines.push(String::new());
  lines.push("nats:".to_string());
  match queue.deployment {
    PlatformServiceDeploymentMode::Local => {
      lines.push("  mode: internal".to_string());
      lines.push("  auth:".to_string());
      lines.push("    enabled: true".to_string());
      lines.push(format!(
        "    token: {}",
        yaml_quote(&summary.internal_queue_token)
      ));
      append_local_persistence(&mut lines, "  ", queue)?;
    }
    PlatformServiceDeploymentMode::External => {
      lines.push("  mode: external".to_string());
      lines.push("  external:".to_string());
      lines.push(format!(
        "    host: {}",
        yaml_quote(required_external_value(queue, "host")?)
      ));
      lines.push(format!("    port: {}", parse_external_u16(queue, "port")?));
      if let Some(token) = optional_external_value(queue, "token") {
        lines.push(format!("    token: {}", yaml_quote(token)));
      }
      lines.push("    tls: false".to_string());
    }
    PlatformServiceDeploymentMode::Disabled => {
      bail!("nats cannot be disabled in the ret2shell helm deployment")
    }
  }

  lines.push(String::new());
  lines.push("registry:".to_string());
  match registry.deployment {
    PlatformServiceDeploymentMode::Disabled => {
      lines.push("  mode: disabled".to_string());
    }
    PlatformServiceDeploymentMode::Local => {
      lines.push("  mode: internal".to_string());
      lines.push("  externalAccess:".to_string());
      lines.push("    enabled: true".to_string());
      lines.push("    serviceType: NodePort".to_string());
      lines.push(format!("    nodePort: {INTERNAL_REGISTRY_NODE_PORT}"));
      lines.push(format!(
        "    host: {}",
        yaml_quote(&summary.internal_registry_host)
      ));
      lines.push("    insecure: true".to_string());
      append_local_persistence(&mut lines, "  ", registry)?;
    }
    PlatformServiceDeploymentMode::External => {
      lines.push("  mode: external".to_string());
      lines.push("  external:".to_string());
      lines.push(format!(
        "    server: {}",
        yaml_quote(required_external_value(registry, "server")?)
      ));
      lines.push(format!(
        "    external: {}",
        yaml_quote(required_external_value(registry, "external")?)
      ));
      lines.push(format!(
        "    insecure: {}",
        parse_external_bool(registry, "insecure")?
      ));
      if let Some(username) = optional_external_value(registry, "username") {
        lines.push(format!("    username: {}", yaml_quote(username)));
      }
      if let Some(password) = optional_external_value(registry, "password") {
        lines.push(format!("    password: {}", yaml_quote(password)));
      }
    }
  }

  lines.push(String::new());
  lines.push("victoriaLogs:".to_string());
  match logs.deployment {
    PlatformServiceDeploymentMode::Disabled => {
      lines.push("  mode: disabled".to_string());
    }
    PlatformServiceDeploymentMode::Local => {
      lines.push("  mode: internal".to_string());
      append_local_persistence(&mut lines, "  ", logs)?;
    }
    PlatformServiceDeploymentMode::External => {
      lines.push("  mode: external".to_string());
      lines.push("  external:".to_string());
      lines.push(format!(
        "    url: {}",
        yaml_quote(required_external_value(logs, "endpoint")?)
      ));
    }
  }

  lines.push(String::new());
  Ok(lines.join("\n"))
}

fn render_platform_storage_manifest(summary: &PlatformPlanSummary) -> Option<String> {
  let storage_classes = summary
    .services
    .iter()
    .filter(|service| {
      service.deployment == PlatformServiceDeploymentMode::Local
        && service.storage_mode == Some(PlatformStorageMode::LocalPath)
    })
    .filter_map(|service| service.storage_class_name.clone())
    .collect::<BTreeSet<_>>();

  if storage_classes.is_empty() {
    return None;
  }

  let mut lines = Vec::new();
  for (index, storage_class) in storage_classes.iter().enumerate() {
    if index > 0 {
      lines.push("---".to_string());
    }

    lines.push("apiVersion: storage.k8s.io/v1".to_string());
    lines.push("kind: StorageClass".to_string());
    lines.push("metadata:".to_string());
    lines.push(format!("  name: {}", yaml_quote(storage_class)));
    lines.push("  annotations:".to_string());
    lines.push("    storageclass.kubernetes.io/is-default-class: \"false\"".to_string());
    lines.push(format!("provisioner: {LOCAL_PATH_PROVISIONER}"));
    lines.push("reclaimPolicy: Retain".to_string());
    lines.push("allowVolumeExpansion: true".to_string());
    lines.push("volumeBindingMode: WaitForFirstConsumer".to_string());
  }
  lines.push(String::new());

  Some(lines.join("\n"))
}

fn planned_service(
  summary: &PlatformPlanSummary, service_id: PlatformServiceId,
) -> Result<&ResolvedPlatformServicePlan> {
  summary
    .services
    .iter()
    .find(|service| service.id == service_id)
    .ok_or_else(|| anyhow!("service plan `{}` is missing", service_id.as_config_value()))
}

fn append_local_persistence(
  lines: &mut Vec<String>, indent: &str, service: &ResolvedPlatformServicePlan,
) -> Result<()> {
  let storage_class = service.storage_class_name.as_deref().ok_or_else(|| {
    anyhow!(
      "storage class for service `{}` is required before rendering helm values",
      service.id.as_config_value()
    )
  })?;
  let size = service.persistence_size_gib.ok_or_else(|| {
    anyhow!(
      "storage size for service `{}` is required before rendering helm values",
      service.id.as_config_value()
    )
  })?;

  lines.push(format!("{indent}persistence:"));
  lines.push(format!(
    "{indent}  storageClass: {}",
    yaml_quote(storage_class)
  ));
  lines.push(format!("{indent}  size: {size}Gi"));

  Ok(())
}

fn push_yaml_string_field(lines: &mut Vec<String>, indent: &str, key: &str, value: &str) {
  if value.contains('\n') {
    lines.push(format!("{indent}{key}: |-"));
    for line in value.lines() {
      lines.push(format!("{indent}  {line}"));
    }
  } else {
    lines.push(format!("{indent}{key}: {}", yaml_quote(value)));
  }
}

fn required_external_value<'a>(
  service: &'a ResolvedPlatformServicePlan, key: &str,
) -> Result<&'a str> {
  service
    .external_values
    .get(key)
    .map(String::as_str)
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .ok_or_else(|| {
      anyhow!(
        "external value `{key}` for service `{}` is required before rendering helm values",
        service.id.as_config_value()
      )
    })
}

fn optional_external_value<'a>(
  service: &'a ResolvedPlatformServicePlan, key: &str,
) -> Option<&'a str> {
  service
    .external_values
    .get(key)
    .map(String::as_str)
    .map(str::trim)
    .filter(|value| !value.is_empty())
}

fn parse_external_u16(service: &ResolvedPlatformServicePlan, key: &str) -> Result<u16> {
  required_external_value(service, key)?
    .parse::<u16>()
    .with_context(|| {
      format!(
        "external value `{key}` for service `{}` must be a valid port number",
        service.id.as_config_value()
      )
    })
}

fn parse_external_bool(service: &ResolvedPlatformServicePlan, key: &str) -> Result<bool> {
  match required_external_value(service, key)?
    .to_ascii_lowercase()
    .as_str()
  {
    "true" | "1" | "yes" | "y" | "on" => Ok(true),
    "false" | "0" | "no" | "n" | "off" => Ok(false),
    _ => Err(anyhow!(
      "external value `{key}` for service `{}` must be a boolean",
      service.id.as_config_value()
    )),
  }
}

fn required_platform_secret(value: Option<&str>, label: &str) -> Result<String> {
  value
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .map(str::to_string)
    .ok_or_else(|| anyhow!("{label} is required before rendering helm values"))
}

fn required_internal_secret(
  services: &[ResolvedPlatformServicePlan], service_id: PlatformServiceId, value: Option<&str>,
  label: &str,
) -> Result<String> {
  let deployment = services
    .iter()
    .find(|service| service.id == service_id)
    .map(|service| service.deployment)
    .ok_or_else(|| anyhow!("service plan `{}` is missing", service_id.as_config_value()))?;

  if deployment == PlatformServiceDeploymentMode::Local {
    return required_platform_secret(value, label);
  }

  Ok(String::new())
}

fn normalize_public_host(raw: &str) -> Result<String> {
  let trimmed = raw.trim();
  let trimmed = trimmed
    .strip_prefix("https://")
    .or_else(|| trimmed.strip_prefix("http://"))
    .unwrap_or(trimmed);
  let trimmed = trimmed.split('/').next().unwrap_or("").trim();

  if trimmed.is_empty() {
    bail!("the public host cannot be empty");
  }
  if trimmed.matches(':').count() > 1 {
    bail!("the public host must be a domain name or IPv4 address");
  }

  Ok(
    trimmed
      .split_once(':')
      .map(|(host, _)| host)
      .unwrap_or(trimmed)
      .to_string(),
  )
}

fn derive_internal_registry_host(public_host: &str) -> String {
  format!("{public_host}:{INTERNAL_REGISTRY_NODE_PORT}")
}

fn public_host_uses_catch_all_ingress(public_host: &str) -> bool {
  public_host.parse::<Ipv4Addr>().is_ok()
}

fn platform_ingress_class(distribution: KubernetesDistribution) -> &'static str {
  match distribution {
    KubernetesDistribution::K3s => "traefik",
    KubernetesDistribution::Rke2 => "nginx",
  }
}

fn helm_envs(ctx: &StepPlanContext<'_>) -> Result<Vec<(String, String)>> {
  let distribution = ctx.kubernetes_distribution().ok_or_else(|| {
    anyhow!("kubernetes distribution is required before rendering helm deployment access")
  })?;

  let kubeconfig = match distribution {
    KubernetesDistribution::K3s => "/etc/rancher/k3s/k3s.yaml",
    KubernetesDistribution::Rke2 => "/etc/rancher/rke2/rke2.yaml",
  };

  Ok(vec![("KUBECONFIG".to_string(), kubeconfig.to_string())])
}

struct ClusterAccess {
  program: String,
  prefix_args: Vec<String>,
  envs: Vec<(String, String)>,
}

impl ClusterAccess {
  fn from_plan_context(ctx: &StepPlanContext<'_>) -> Result<Self> {
    let distribution = ctx.kubernetes_distribution().ok_or_else(|| {
      anyhow!("kubernetes distribution is required before rendering cluster access")
    })?;

    match distribution {
      KubernetesDistribution::K3s => {
        if !command_exists("k3s") {
          bail!("unable to locate the k3s binary required for cluster access");
        }

        Ok(Self {
          program: "k3s".to_string(),
          prefix_args: vec!["kubectl".to_string()],
          envs: Vec::new(),
        })
      }
      KubernetesDistribution::Rke2 => {
        let kubectl = find_existing_path(&[
          PathBuf::from("/var/lib/rancher/rke2/bin/kubectl"),
          PathBuf::from("/usr/local/bin/kubectl"),
        ])
        .ok_or_else(|| anyhow!("unable to locate the rke2 kubectl binary for cluster access"))?;

        Ok(Self {
          program: kubectl.display().to_string(),
          prefix_args: Vec::new(),
          envs: helm_envs(ctx)?,
        })
      }
    }
  }

  fn wait_for_nodes_ready(&self, ctx: &StepExecutionContext<'_>) -> Result<()> {
    let args = [
      "wait".to_string(),
      "--for=condition=Ready".to_string(),
      "node".to_string(),
      "--all".to_string(),
      "--timeout=5s".to_string(),
    ];
    let mut last_error = None;

    for _ in 0..60 {
      match self.run(ctx, &args) {
        Ok(()) => return Ok(()),
        Err(error) => last_error = Some(error),
      }

      thread::sleep(Duration::from_secs(5));
    }

    Err(
      last_error
        .unwrap_or_else(|| anyhow!("timed out waiting for kubernetes nodes to become ready")),
    )
  }

  fn apply_manifest(&self, ctx: &StepExecutionContext<'_>, path: &str) -> Result<()> {
    self.run(
      ctx,
      &["apply".to_string(), "-f".to_string(), path.to_string()],
    )
  }

  fn delete_manifest(&self, ctx: &StepExecutionContext<'_>, path: &str) -> Result<()> {
    self.run(
      ctx,
      &[
        "delete".to_string(),
        "--ignore-not-found".to_string(),
        "-f".to_string(),
        path.to_string(),
      ],
    )
  }

  fn storage_class_exists(&self, ctx: &StepExecutionContext<'_>, name: &str) -> Result<bool> {
    let output = self.capture(
      ctx,
      &[
        "get".to_string(),
        "storageclass".to_string(),
        name.to_string(),
        "--ignore-not-found".to_string(),
        "-o".to_string(),
        "name".to_string(),
      ],
    )?;

    Ok(!output.trim().is_empty())
  }

  fn capture_namespaced_object_template(
    &self, ctx: &StepExecutionContext<'_>, namespace: &str, resource: &str, name: &str,
    template: &str,
  ) -> Result<Option<String>> {
    let exists = self.capture(
      ctx,
      &[
        "-n".to_string(),
        namespace.to_string(),
        "get".to_string(),
        resource.to_string(),
        name.to_string(),
        "--ignore-not-found".to_string(),
        "-o".to_string(),
        "name".to_string(),
      ],
    )?;

    if exists.trim().is_empty() {
      return Ok(None);
    }

    self
      .capture(
        ctx,
        &[
          "-n".to_string(),
          namespace.to_string(),
          "get".to_string(),
          resource.to_string(),
          name.to_string(),
          "-o".to_string(),
          format!("go-template={template}"),
        ],
      )
      .map(Some)
  }

  fn restart_release_workloads(&self, ctx: &StepExecutionContext<'_>) -> Result<()> {
    self.run(
      ctx,
      &[
        "-n".to_string(),
        PLATFORM_NAMESPACE.to_string(),
        "rollout".to_string(),
        "restart".to_string(),
        "deployment,statefulset".to_string(),
        "-l".to_string(),
        format!("app.kubernetes.io/instance={HELM_RELEASE_NAME}"),
      ],
    )
  }

  fn capture(&self, ctx: &StepExecutionContext<'_>, args: &[String]) -> Result<String> {
    let mut command_args = self.prefix_args.clone();
    command_args.extend_from_slice(args);
    ctx.run_privileged_command_capture(&self.program, &command_args, &self.envs)
  }

  fn run(&self, ctx: &StepExecutionContext<'_>, args: &[String]) -> Result<()> {
    let mut command_args = self.prefix_args.clone();
    command_args.extend_from_slice(args);
    ctx.run_privileged_command(&self.program, &command_args, &self.envs)
  }
}

fn platform_service_summary_line(service: &ResolvedPlatformServicePlan) -> String {
  let mut parts = vec![platform_service_deployment_label(service.deployment)];

  if let Some(storage_mode) = service.storage_mode {
    parts.push(format!(
      "{} ({})",
      platform_storage_mode_label(storage_mode),
      service.storage_class_name.as_deref().unwrap_or("-")
    ));
  }

  if let Some(persistence_size_gib) = service.persistence_size_gib {
    parts.push(format!("{persistence_size_gib} GiB"));
  }

  for (label, value) in &service.external_summary {
    parts.push(format!("{label}={value}"));
  }

  parts.join(" / ")
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use serde::Deserialize;
  use serde_yaml::{Deserializer, Value};

  use super::*;

  #[test]
  fn normalize_public_host_strips_scheme_path_and_port() {
    let host = normalize_public_host("https://ctf.example.com:8443/ui").expect("host parses");

    assert_eq!(host, "ctf.example.com");
  }

  #[test]
  fn render_platform_values_yaml_maps_chart_settings() {
    let summary = sample_summary();
    let rendered = render_platform_values_yaml(&summary).expect("values render");
    let parsed: Value = serde_yaml::from_str(&rendered).expect("values parse as yaml");

    assert_eq!(
      parsed["platform"]["ingress"]["hosts"][0]["host"],
      Value::String("ctf.example.com".to_string())
    );
    assert_eq!(
      parsed["platform"]["ingress"]["className"],
      Value::String("traefik".to_string())
    );
    assert_eq!(
      parsed["platform"]["config"]["auth"]["signingKey"],
      Value::String("signing-key".to_string())
    );
    assert_eq!(
      parsed["postgresql"]["mode"],
      Value::String("external".to_string())
    );
    assert_eq!(
      parsed["postgresql"]["external"]["host"],
      Value::String("db.example.com".to_string())
    );
    assert_eq!(
      parsed["valkey"]["auth"]["password"],
      Value::String("cache-secret".to_string())
    );
    assert_eq!(
      parsed["registry"]["externalAccess"]["host"],
      Value::String("ctf.example.com:30310".to_string())
    );
    assert_eq!(
      parsed["victoriaLogs"]["mode"],
      Value::String("disabled".to_string())
    );
  }

  #[test]
  fn render_platform_values_yaml_omits_ingress_host_for_ipv4_public_host() {
    let mut summary = sample_summary();
    summary.public_host = "103.151.173.97".to_string();
    summary.internal_registry_host = "103.151.173.97:30310".to_string();

    let rendered = render_platform_values_yaml(&summary).expect("values render");
    let parsed: Value = serde_yaml::from_str(&rendered).expect("values parse as yaml");
    let ingress_rule = &parsed["platform"]["ingress"]["hosts"][0];

    assert!(ingress_rule["host"].is_null());
    assert_eq!(
      parsed["platform"]["config"]["server"]["externalDomain"],
      Value::String("103.151.173.97".to_string())
    );
  }

  #[test]
  fn render_platform_storage_manifest_only_emits_local_path_classes() {
    let summary = sample_summary();
    let rendered = render_platform_storage_manifest(&summary).expect("storage manifest exists");
    let documents = Deserializer::from_str(&rendered)
      .map(Value::deserialize)
      .collect::<std::result::Result<Vec<_>, _>>()
      .expect("storage manifest parses as yaml");
    let storage_classes = documents
      .into_iter()
      .map(|document| {
        document["metadata"]["name"]
          .as_str()
          .unwrap_or_default()
          .to_string()
      })
      .collect::<Vec<_>>();

    assert_eq!(
      storage_classes,
      vec![
        "ret2shell-storage-cache".to_string(),
        "ret2shell-storage-platform".to_string(),
        "ret2shell-storage-registry".to_string(),
      ]
    );
  }

  #[test]
  fn chart_cache_copy_is_skipped_when_source_is_already_in_system_cache() {
    let path = Path::new("/var/cache/ret2shell/ret2boot/charts/ret2shell/3.10.4/ret2shell-3.10.4.tgz");

    assert!(!chart_cache_copy_required(path, path));
  }

  #[test]
  fn chart_cache_copy_runs_when_source_and_target_differ() {
    assert!(chart_cache_copy_required(
      Path::new("/tmp/ret2shell-3.10.4.tgz"),
      Path::new("/var/cache/ret2shell/ret2boot/charts/ret2shell/3.10.4/ret2shell-3.10.4.tgz")
    ));
  }

  #[test]
  fn public_host_uses_catch_all_ingress_for_ipv4_only() {
    assert!(public_host_uses_catch_all_ingress("103.151.173.97"));
    assert!(!public_host_uses_catch_all_ingress("ctf.example.com"));
  }

  fn sample_summary() -> PlatformPlanSummary {
    PlatformPlanSummary {
      remaining_disk_gib: 900,
      requested_disk_gib: 730,
      allocated_local_disk_gib: 730,
      unallocated_local_disk_gib: 0,
      public_host: "ctf.example.com".to_string(),
      ingress_class_name: "traefik".to_string(),
      signing_key: "signing-key".to_string(),
      blocked_content: String::new(),
      internal_database_password: "database-secret".to_string(),
      internal_cache_password: "cache-secret".to_string(),
      internal_queue_token: "queue-secret".to_string(),
      internal_registry_host: "ctf.example.com:30310".to_string(),
      services: vec![
        service_plan(
          PlatformServiceId::Platform,
          PlatformServiceDeploymentMode::Local,
          Some(PlatformStorageMode::LocalPath),
          Some("ret2shell-storage-platform"),
          Some(420),
          Some(420),
          BTreeMap::new(),
        ),
        service_plan(
          PlatformServiceId::Database,
          PlatformServiceDeploymentMode::External,
          None,
          None,
          None,
          None,
          BTreeMap::from([
            ("host".to_string(), "db.example.com".to_string()),
            ("port".to_string(), "5432".to_string()),
            ("database".to_string(), "ret2shell".to_string()),
            ("username".to_string(), "ret2shell".to_string()),
            ("password".to_string(), "db-password".to_string()),
            ("ssl_mode".to_string(), "disable".to_string()),
          ]),
        ),
        service_plan(
          PlatformServiceId::Cache,
          PlatformServiceDeploymentMode::Local,
          Some(PlatformStorageMode::LocalPath),
          Some("ret2shell-storage-cache"),
          Some(10),
          Some(10),
          BTreeMap::new(),
        ),
        service_plan(
          PlatformServiceId::Queue,
          PlatformServiceDeploymentMode::External,
          None,
          None,
          None,
          None,
          BTreeMap::from([
            ("host".to_string(), "nats.example.com".to_string()),
            ("port".to_string(), "4222".to_string()),
            ("token".to_string(), "queue-token".to_string()),
          ]),
        ),
        service_plan(
          PlatformServiceId::Registry,
          PlatformServiceDeploymentMode::Local,
          Some(PlatformStorageMode::LocalPath),
          Some("ret2shell-storage-registry"),
          Some(300),
          Some(300),
          BTreeMap::new(),
        ),
        service_plan(
          PlatformServiceId::Logs,
          PlatformServiceDeploymentMode::Disabled,
          None,
          None,
          None,
          None,
          BTreeMap::new(),
        ),
      ],
    }
  }

  fn service_plan(
    id: PlatformServiceId, deployment: PlatformServiceDeploymentMode,
    storage_mode: Option<PlatformStorageMode>, storage_class_name: Option<&str>,
    local_disk_gib: Option<u32>, persistence_size_gib: Option<u32>,
    external_values: BTreeMap<String, String>,
  ) -> ResolvedPlatformServicePlan {
    ResolvedPlatformServicePlan {
      id,
      deployment,
      storage_mode,
      storage_class_name: storage_class_name.map(str::to_string),
      local_disk_gib,
      persistence_size_gib,
      external_summary: Vec::new(),
      external_values,
    }
  }
}
