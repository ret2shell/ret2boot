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
use serde_yaml::{Deserializer, Mapping, Value as YamlValue};
use toml::Value as TomlValue;

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  support::{
    command_exists, find_existing_path, install_directory, install_staged_file,
    managed_tls_asset_name, managed_tls_certificate_path, managed_tls_key_path,
    render_container_registry_config, stage_text_file, yaml_quote,
  },
};
use crate::{
  config::{
    ApplicationExposureMode, InstallStepId, InstallTargetRole, KubernetesDistribution,
    PlatformServiceDeploymentMode, PlatformServiceId, PlatformStorageMode, PlatformTlsMode,
  },
  install::collectors::{
    Collector, ConfirmCollector, InputCollector, SecretCollector, SingleSelectCollector,
  },
  ui, update,
};

const PLATFORM_VALUES_DEST: &str = "/etc/ret2shell/ret2boot-platform-values.yaml";
const PLATFORM_STORAGE_DEST: &str = "/etc/ret2shell/ret2boot-platform-storage.yaml";
const PLATFORM_NAMESPACE: &str = "ret2shell-platform";
const CHALLENGE_NAMESPACE: &str = "ret2shell-challenge";
const HELM_RELEASE_NAME: &str = "ret2shell";
const HELM_RELEASE_TIMEOUT: &str = "15m0s";
const HELM_UNINSTALL_TIMEOUT: &str = "10m0s";
const HELM_UNINSTALL_POLL_RETRIES: usize = 24;
const HELM_UNINSTALL_POLL_INTERVAL_SECONDS: u64 = 5;
const NODE_READY_WAIT_TIMEOUT: &str = "10s";
const NODE_READY_WAIT_RETRIES: usize = 60;
const RET2SHELL_ROOT_DIR: &str = "/srv/ret2shell";
const FRONTEND_HOST_DIR: &str = "/srv/ret2shell/frontend";
const BACKEND_ROOT_DIR: &str = "/srv/ret2shell/backend";
const BACKEND_CONFIG_DIR: &str = "/srv/ret2shell/backend/config";
const BACKEND_DEPLOYMENTS_DIR: &str = "/srv/ret2shell/backend/deployments";
const BACKEND_STORAGE_DIR: &str = "/srv/ret2shell/backend/storage";
const BACKEND_CHART_ROOT_DIR: &str = "/srv/ret2shell/backend/deployments/chart";
const BACKEND_CHART_DIR: &str = "/srv/ret2shell/backend/deployments/chart/ret2shell";
const BACKEND_INIT_MANIFEST_DEST: &str = "/srv/ret2shell/backend/deployments/0-init.yaml";
const BACKEND_VOLUMES_MANIFEST_DEST: &str = "/srv/ret2shell/backend/deployments/1-volumes.yaml";
const BACKEND_PLATFORM_VALUES_DEST: &str = "/srv/ret2shell/backend/deployments/7-platform.yaml";
const BACKEND_CACHE_DIR: &str = "/srv/ret2shell/backend/deployments/2-cache";
const BACKEND_DATABASE_DIR: &str = "/srv/ret2shell/backend/deployments/3-database";
const BACKEND_QUEUE_DIR: &str = "/srv/ret2shell/backend/deployments/4-queue";
const BACKEND_REGISTRY_DIR: &str = "/srv/ret2shell/backend/deployments/5-registry";
const BACKEND_LOGS_DIR: &str = "/srv/ret2shell/backend/deployments/6-logs";
const BACKEND_CONFIG_TOML_DEST: &str = "/srv/ret2shell/backend/config/config.toml";
const BACKEND_BLOCKED_DEST: &str = "/srv/ret2shell/backend/config/blocked.txt";
const BACKEND_LICENSE_DEST: &str = "/srv/ret2shell/backend/config/license";
const K3S_REGISTRIES_DEST: &str = "/etc/rancher/k3s/registries.yaml";
const RKE2_REGISTRIES_DEST: &str = "/etc/rancher/rke2/registries.yaml";
pub(crate) const PLATFORM_NODE_PORT: u16 = 30307;
const PLATFORM_INGRESS_PATH: &str = "/";
const PLATFORM_INGRESS_PATH_TYPE: &str = "Prefix";
const INTERNAL_REGISTRY_NODE_PORT: u16 = 30310;
const INTERNAL_INGRESS_HOST_SUFFIX: &str = "ret2boot.invalid";
const POSTGRESQL_STORAGE_ROOT_MODE: &str = "1777";
const POSTGRESQL_LEGACY_MOUNT_PATH: &str = "mountPath: /var/lib/postgresql/data";
const POSTGRESQL_PERSISTENT_MOUNT_PATH: &str = "mountPath: /var/lib/postgresql";
const PLATFORM_TEMPLATE_CONTAINERS_MARKER: &str = "      containers:\n";
const PLATFORM_TEMPLATE_VOLUME_MOUNTS_MARKER: &str = "          volumeMounts:\n";
const PLATFORM_TEMPLATE_RESOURCES_MARKER: &str =
  "          {{- with .Values.platform.resources }}\n";
const PLATFORM_TEMPLATE_VOLUMES_MARKER: &str = "      volumes:\n";
const PLATFORM_TEMPLATE_NODE_SELECTOR_MARKER: &str =
  "      {{- with .Values.platform.nodeSelector }}\n";
const PATCHED_PLATFORM_INIT_CONTAINERS: &str = r#"      initContainers:
        - name: frontend-sync
          image: {{ include "ret2shell.image" .Values.platform.image }}
          imagePullPolicy: {{ .Values.platform.image.pullPolicy }}
          command:
            - /bin/sh
            - -ec
            - |
              set -eu
              mkdir -p /host-frontend
              if [ ! -f /host-frontend/index.html ]; then
                cp -a /var/www/html/. /host-frontend/
              fi
          volumeMounts:
            - name: frontend
              mountPath: /host-frontend
      containers:
"#;
const PATCHED_PLATFORM_VOLUME_MOUNTS: &str = r#"          volumeMounts:
            - name: data
              mountPath: /var/lib/ret2shell
            - name: frontend
              mountPath: /var/www/html
            - name: backend-config
              mountPath: /etc/ret2shell
"#;
const PATCHED_PLATFORM_VOLUMES: &str = r#"      volumes:
        - name: frontend
          hostPath:
            path: /srv/ret2shell/frontend
            type: DirectoryOrCreate
        - name: backend-config
          hostPath:
            path: /srv/ret2shell/backend/config
            type: DirectoryOrCreate
        {{- if .Values.platform.persistence.existingClaim }}
        - name: data
          persistentVolumeClaim:
            claimName: {{ .Values.platform.persistence.existingClaim }}
        {{- else if not .Values.platform.persistence.enabled }}
        - name: data
          emptyDir: {}
        {{- end }}
"#;

#[derive(Debug)]
pub(crate) struct ResolvedPublicEndpoint {
  pub public_host: String,
  pub ingress_host: String,
}

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
  pub workload_changed: bool,
  pub values_changed: bool,
  pub config_changed: bool,
  pub blocked_changed: bool,
  pub storage_changed: bool,
}

impl PlatformSyncReport {
  pub(crate) fn has_changes(&self) -> bool {
    !self.release_exists
      || self.chart_changed
      || self.workload_changed
      || self.values_changed
      || self.config_changed
      || self.blocked_changed
      || self.storage_changed
  }
}

struct ChartReference {
  version: String,
  source_path: PathBuf,
  path: PathBuf,
  download_url: String,
  release_url: String,
}

struct ManifestResource {
  kind: String,
  name: String,
}

#[derive(Default)]
struct ReferenceConfigFiles {
  config_toml: Option<String>,
  blocked_content: Option<String>,
  license_content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HelmReleaseSummary {
  name: String,
  chart: String,
}

#[derive(Debug, Deserialize)]
struct ClusterNodeList {
  items: Vec<ClusterNodeItem>,
}

#[derive(Debug, Deserialize)]
struct ClusterNodeItem {
  metadata: ClusterNodeMetadata,
}

#[derive(Debug, Deserialize)]
struct ClusterNodeMetadata {
  name: String,
  #[serde(default)]
  labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct ClusterNodeIdentity {
  name: String,
  hostname_label: String,
  is_control_plane: bool,
}

#[derive(Default)]
struct ClusterReleaseState {
  release_exists: bool,
  chart_version: Option<String>,
  values: Option<YamlValue>,
  config_toml: Option<String>,
  blocked_content: Option<String>,
  platform_mount_layout: Option<YamlValue>,
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
    collect_platform_tls(ctx)?;
    collect_nodeport_security(ctx)?;
    ensure_generated_platform_credentials(ctx)?;

    Ok(())
  }

  fn describe(&self, ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    let summary = platform_plan_summary(ctx)?;
    let _ = render_platform_values_yaml(&summary)?;
    let _ = render_platform_storage_manifest(&summary, None);
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

    if summary.tls_enabled {
      match summary.application_exposure {
        ApplicationExposureMode::Ingress => details.push(
          t!(
            "install.steps.platform_plan.tls_ingress_secret",
            secret = summary
              .tls_secret_name
              .as_deref()
              .unwrap_or("ret2shell-tls")
          )
          .to_string(),
        ),
        ApplicationExposureMode::NodePortExternalNginx => {
          details.push(t!("install.steps.platform_plan.tls_external_gateway").to_string());
        }
      }
    }

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
    let cluster_access = ClusterAccess::from_plan_context(&ctx.as_plan_context())?;
    let storage_path = ctx
      .config()
      .install_step_metadata(self.id(), "storage_path")
      .map(str::to_string);

    uninstall_platform_release(ctx, &helm_envs)?;

    if Path::new(BACKEND_INIT_MANIFEST_DEST).is_file() {
      cluster_access.delete_manifest(ctx, BACKEND_INIT_MANIFEST_DEST)?;
    }

    if let Some(storage_path) = storage_path.filter(|path| PathBuf::from(path).is_file()) {
      cluster_access.delete_manifest(ctx, &storage_path)?;
    }

    ctx.run_privileged_command(
      "rm",
      &[
        "-f".to_string(),
        BACKEND_INIT_MANIFEST_DEST.to_string(),
        BACKEND_VOLUMES_MANIFEST_DEST.to_string(),
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
      let changed = config.remove_install_step_metadata(self.id(), "chart_source_path") || changed;
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
  application_exposure: ApplicationExposureMode,
  public_host: String,
  ingress_host: String,
  ingress_class_name: String,
  tls_enabled: bool,
  tls_secret_name: Option<String>,
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
  let public_host = ctx
    .config()
    .install
    .questionnaire
    .platform
    .public_host
    .as_deref()
    .ok_or_else(|| anyhow!("a public host is required before planning the platform deployment"))?;
  let application_exposure = ctx.application_exposure().ok_or_else(|| {
    anyhow!("application exposure mode is required before planning the platform deployment")
  })?;
  let endpoint = resolve_public_endpoint(public_host, application_exposure)?;
  let ingress_class_name =
    platform_ingress_class(ctx.kubernetes_distribution().ok_or_else(|| {
      anyhow!("kubernetes distribution is required before planning the platform deployment")
    })?)
    .to_string();
  let tls_enabled =
    ctx.platform_tls_mode().unwrap_or(PlatformTlsMode::Disabled) != PlatformTlsMode::Disabled;
  let tls_secret_name = if tls_enabled && application_exposure == ApplicationExposureMode::Ingress {
    Some(
      ctx
        .platform_tls_secret_name()
        .ok_or_else(|| anyhow!("a TLS secret name is required when TLS is enabled"))?
        .to_string(),
    )
  } else {
    None
  };
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
    application_exposure,
    public_host: endpoint.public_host.clone(),
    ingress_host: endpoint.ingress_host.clone(),
    ingress_class_name,
    tls_enabled,
    tls_secret_name,
    signing_key,
    blocked_content,
    internal_database_password,
    internal_cache_password,
    internal_queue_token,
    internal_registry_host: derive_internal_registry_host(&endpoint.public_host),
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
  let exposure = ctx
    .as_plan_context()
    .application_exposure()
    .ok_or_else(|| {
      anyhow!("application exposure mode is required before collecting the platform public host")
    })?;
  let default_host = ctx
    .config()
    .install
    .questionnaire
    .platform
    .public_host
    .clone()
    .or_else(|| match exposure {
      ApplicationExposureMode::NodePortExternalNginx => ctx
        .preflight_state()
        .public_network_ip()
        .map(str::to_string),
      ApplicationExposureMode::Ingress => None,
    })
    .unwrap_or_else(|| match exposure {
      ApplicationExposureMode::NodePortExternalNginx => "ret2shell.local".to_string(),
      ApplicationExposureMode::Ingress => "ctf.example.com".to_string(),
    });
  let raw_public_host = InputCollector::new(public_host_prompt(exposure))
    .with_default(default_host)
    .collect()?;
  let input_host = canonical_public_host_input(&raw_public_host)?.to_string();
  let endpoint = resolve_public_endpoint(&input_host, exposure)?;

  if exposure == ApplicationExposureMode::NodePortExternalNginx
    && public_host_is_ipv4(&input_host)
    && endpoint.ingress_host != endpoint.public_host
  {
    println!(
      "{}",
      ui::note(t!(
        "install.platform.public_host.bare_ip_note",
        input = input_host.as_str(),
        host = endpoint.ingress_host.as_str()
      ))
    );
  }

  ctx.persist_change(
    "install.questionnaire.platform.public_host",
    &input_host,
    |config| config.set_platform_public_host(input_host.clone()),
  )?;

  Ok(())
}

fn collect_platform_tls(ctx: &mut StepQuestionContext<'_>) -> Result<()> {
  let exposure = ctx
    .as_plan_context()
    .application_exposure()
    .ok_or_else(|| {
      anyhow!("application exposure mode is required before collecting TLS settings")
    })?;
  let public_host = ctx
    .config()
    .install
    .questionnaire
    .platform
    .public_host
    .clone()
    .ok_or_else(|| anyhow!("platform public host is required before collecting TLS settings"))?;
  let existing_mode = ctx
    .as_plan_context()
    .platform_tls_mode()
    .unwrap_or(match exposure {
      ApplicationExposureMode::Ingress => PlatformTlsMode::AcmeDns,
      ApplicationExposureMode::NodePortExternalNginx => PlatformTlsMode::Disabled,
    });
  let enable_tls_default = existing_mode != PlatformTlsMode::Disabled;
  let enable_tls =
    ConfirmCollector::new(t!("install.platform.tls.enable_prompt"), enable_tls_default)
      .collect()?;

  if !enable_tls {
    ctx.persist_change(
      "install.questionnaire.platform.tls.mode",
      "disabled",
      |config| config.set_platform_tls_mode(PlatformTlsMode::Disabled),
    )?;
    return Ok(());
  }

  let tls_modes = supported_tls_modes(exposure, &public_host);
  let default_tls_mode = tls_modes
    .iter()
    .position(|mode| *mode == existing_mode)
    .unwrap_or(0);
  let tls_mode = if tls_modes.len() == 1 {
    println!();
    println!(
      "{}",
      ui::note(t!(
        "install.platform.tls.mode.auto_selected",
        mode = tls_mode_label(tls_modes[0])
      ))
    );
    tls_modes[0]
  } else {
    let tls_mode_labels = tls_modes
      .iter()
      .copied()
      .map(tls_mode_label)
      .collect::<Vec<_>>();
    tls_modes[SingleSelectCollector::new(t!("install.platform.tls.mode.prompt"), tls_mode_labels)
      .with_default(default_tls_mode)
      .collect_index()?]
  };

  ctx.persist_change(
    "install.questionnaire.platform.tls.mode",
    tls_mode.as_config_value(),
    |config| config.set_platform_tls_mode(tls_mode),
  )?;

  if exposure == ApplicationExposureMode::Ingress {
    let default_secret_name = ctx
      .as_plan_context()
      .platform_tls_secret_name()
      .unwrap_or("ret2shell-tls")
      .to_string();
    let secret_name = InputCollector::new(t!("install.platform.tls.secret_name.prompt"))
      .with_default(default_secret_name)
      .collect()?
      .trim()
      .to_string();
    ctx.persist_change(
      "install.questionnaire.platform.tls.secret_name",
      &secret_name,
      |config| config.set_platform_tls_secret_name(secret_name.clone()),
    )?;
  }

  let default_domains = if ctx.as_plan_context().platform_tls_domains().is_empty() {
    public_host.clone()
  } else {
    ctx.as_plan_context().platform_tls_domains().join(",")
  };
  let domains = InputCollector::new(t!("install.platform.tls.domains.prompt"))
    .with_default(default_domains)
    .collect()?;
  let domains = domains
    .split(',')
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .map(str::to_string)
    .collect::<Vec<_>>();
  if domains.is_empty() {
    bail!("{}", t!("install.platform.tls.domains.required"));
  }
  ctx.persist_change(
    "install.questionnaire.platform.tls.domains",
    &domains.join(","),
    |config| config.set_platform_tls_domains(domains.clone()),
  )?;

  match tls_mode {
    PlatformTlsMode::Disabled => {}
    PlatformTlsMode::AcmeDns => {
      if !supports_acme_dns_for_host(exposure, &public_host) {
        bail!("{}", t!("install.platform.tls.acme_dns.public_host_error"));
      }

      let acme_email = InputCollector::new(t!("install.platform.tls.acme_email.prompt"))
        .with_default(
          ctx
            .as_plan_context()
            .platform_tls_acme_email()
            .unwrap_or("admin@example.com")
            .to_string(),
        )
        .collect()?
        .trim()
        .to_string();
      ctx.persist_change(
        "install.questionnaire.platform.tls.acme_email",
        &acme_email,
        |config| config.set_platform_tls_acme_email(acme_email.clone()),
      )?;

      let dns_provider = InputCollector::new(t!("install.platform.tls.acme_dns_provider.prompt"))
        .with_default(
          ctx
            .as_plan_context()
            .platform_tls_acme_dns_provider()
            .unwrap_or("dns_dp")
            .to_string(),
        )
        .collect()?
        .trim()
        .to_string();
      ctx.persist_change(
        "install.questionnaire.platform.tls.acme_dns_provider",
        &dns_provider,
        |config| config.set_platform_tls_acme_dns_provider(dns_provider.clone()),
      )?;

      let credentials_prompt = t!("install.platform.tls.acme_dns_credentials.prompt");
      let credentials = match ctx.as_plan_context().platform_tls_acme_dns_credentials() {
        Some(existing_value) => SecretCollector::new(credentials_prompt)
          .with_existing_value(existing_value.to_string())
          .collect()?,
        None => SecretCollector::new(credentials_prompt).collect()?,
      };
      ctx.persist_change(
        "install.questionnaire.platform.tls.acme_dns_credentials",
        "[redacted]",
        |config| config.set_platform_tls_acme_dns_credentials(credentials.clone()),
      )?;
    }
    PlatformTlsMode::ProvidedFiles => {
      let certificate_path = collect_existing_host_file_path(
        ctx,
        t!("install.platform.tls.certificate_path.prompt").to_string(),
        ctx
          .as_plan_context()
          .platform_tls_certificate_path()
          .unwrap_or("/etc/ssl/certs/fullchain.pem")
          .to_string(),
        t!("install.platform.tls.certificate_path.missing").to_string(),
      )?;
      ctx.persist_change(
        "install.questionnaire.platform.tls.certificate_path",
        &certificate_path,
        |config| config.set_platform_tls_certificate_path(certificate_path.clone()),
      )?;

      let key_path = collect_existing_host_file_path(
        ctx,
        t!("install.platform.tls.key_path.prompt").to_string(),
        ctx
          .as_plan_context()
          .platform_tls_key_path()
          .unwrap_or("/etc/ssl/private/privkey.pem")
          .to_string(),
        t!("install.platform.tls.key_path.missing").to_string(),
      )?;
      ctx.persist_change(
        "install.questionnaire.platform.tls.key_path",
        &key_path,
        |config| config.set_platform_tls_key_path(key_path.clone()),
      )?;
    }
  }

  Ok(())
}

fn collect_existing_host_file_path(
  ctx: &StepQuestionContext<'_>, prompt: String, default: String, missing_message: String,
) -> Result<String> {
  loop {
    let path = InputCollector::new(prompt.clone())
      .with_default(default.clone())
      .collect()?
      .trim()
      .to_string();

    if host_regular_file_exists(ctx, &path) {
      return Ok(path);
    }

    println!("{}", ui::warning(format!("{missing_message} ({path})")));
  }
}

fn host_regular_file_exists(ctx: &StepQuestionContext<'_>, path: &str) -> bool {
  ctx
    .run_privileged_command("test", &["-f".to_string(), path.to_string()], &[])
    .is_ok()
}

fn collect_nodeport_security(ctx: &mut StepQuestionContext<'_>) -> Result<()> {
  let exposure = ctx
    .as_plan_context()
    .application_exposure()
    .ok_or_else(|| {
      anyhow!("application exposure mode is required before collecting nodeport security settings")
    })?;

  if exposure != ApplicationExposureMode::NodePortExternalNginx {
    let _ = ctx.persist_change(
      "install.questionnaire.platform.nodeport_security.guard_enabled",
      "false",
      |config| config.set_platform_nodeport_guard_enabled(false),
    )?;
    return Ok(());
  }

  let guard_enabled = ConfirmCollector::new(
    t!("install.platform.nodeport_security.guard_prompt"),
    ctx
      .as_plan_context()
      .platform_nodeport_guard_enabled()
      .unwrap_or(true),
  )
  .collect()?;
  ctx.persist_change(
    "install.questionnaire.platform.nodeport_security.guard_enabled",
    if guard_enabled { "true" } else { "false" },
    |config| config.set_platform_nodeport_guard_enabled(guard_enabled),
  )?;

  if !guard_enabled {
    return Ok(());
  }

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
  let node_hostname = detect_node_hostname_label(ctx, &cluster_access)?;

  install_directory(ctx, "/etc/ret2shell")?;
  ensure_platform_host_layout(ctx, &summary)?;
  if sync_runtime_registry_config(ctx, &summary)? {
    restart_cluster_runtime_service(ctx)?;
  }
  cluster_access.wait_for_nodes_ready(ctx)?;

  let desired_values = render_platform_values_yaml(&summary)?;
  let _ = sync_managed_text_file(ctx, PLATFORM_VALUES_DEST, &desired_values, "600")?;
  let storage_manifest = render_platform_storage_manifest(&summary, Some(&node_hostname));
  let storage_changed =
    sync_storage_manifest(ctx, step, &cluster_access, storage_manifest.clone())?;

  let rendered_artifacts = render_chart_artifacts(ctx, &chart, &helm_envs)?;
  let reference_files = load_reference_config_files()?;
  let desired_host_config_toml = merge_runtime_config_with_reference(
    &rendered_artifacts.config_toml,
    reference_files.config_toml.as_deref(),
  )?;
  let desired_host_blocked = reference_files
    .blocked_content
    .clone()
    .unwrap_or_else(|| rendered_artifacts.blocked_content.clone());
  let host_config_changed = sync_managed_text_file(
    ctx,
    BACKEND_CONFIG_TOML_DEST,
    &desired_host_config_toml,
    "600",
  )?;
  let host_blocked_changed =
    sync_managed_text_file(ctx, BACKEND_BLOCKED_DEST, &desired_host_blocked, "600")?;
  let host_license_changed = sync_optional_managed_text_file(
    ctx,
    BACKEND_LICENSE_DEST,
    reference_files.license_content.as_deref(),
    "644",
  )?;
  sync_deployment_exports(ctx, &desired_values, storage_manifest.as_deref())?;
  cluster_access.apply_manifest(ctx, BACKEND_INIT_MANIFEST_DEST)?;
  ensure_platform_tls_secret(ctx, &cluster_access, &summary)?;
  let cluster_state = query_cluster_release_state(ctx, &cluster_access, &helm_envs)?;
  let report = PlatformSyncReport {
    release_exists: cluster_state.release_exists,
    chart_changed: cluster_state.chart_version.as_deref() != Some(chart.version.as_str()),
    workload_changed: cluster_state.platform_mount_layout.as_ref()
      != Some(&rendered_artifacts.platform_mount_layout)
      || host_license_changed,
    values_changed: cluster_state.values != Some(parse_yaml_value(&desired_values)?),
    config_changed: host_config_changed
      || cluster_state.config_toml.as_deref() != Some(rendered_artifacts.config_toml.as_str()),
    blocked_changed: host_blocked_changed
      || cluster_state.blocked_content.as_deref()
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

fn ensure_platform_tls_secret(
  ctx: &StepExecutionContext<'_>, cluster_access: &ClusterAccess, summary: &PlatformPlanSummary,
) -> Result<()> {
  if summary.application_exposure != ApplicationExposureMode::Ingress || !summary.tls_enabled {
    return Ok(());
  }

  let secret_name = summary
    .tls_secret_name
    .as_deref()
    .ok_or_else(|| anyhow!("a TLS secret name is required for ingress TLS"))?;
  let (certificate_path, key_path) = resolved_managed_tls_material_paths(&ctx.as_plan_context())?;

  if !Path::new(&certificate_path).is_file() {
    bail!("the managed TLS certificate file `{certificate_path}` is missing");
  }
  if !Path::new(&key_path).is_file() {
    bail!("the managed TLS key file `{key_path}` is missing");
  }

  let secret_manifest = cluster_access.capture(
    ctx,
    &[
      "-n".to_string(),
      PLATFORM_NAMESPACE.to_string(),
      "create".to_string(),
      "secret".to_string(),
      "tls".to_string(),
      secret_name.to_string(),
      format!("--cert={certificate_path}"),
      format!("--key={key_path}"),
      "--dry-run=client".to_string(),
      "-o".to_string(),
      "yaml".to_string(),
    ],
  )?;
  let staged_secret = stage_text_file("platform-tls-secret", "yaml", secret_manifest)?;
  let apply_result = cluster_access.apply_manifest(ctx, &staged_secret.display().to_string());
  let _ = fs::remove_file(&staged_secret);
  apply_result
}

fn resolve_chart_reference(
  ctx: &StepExecutionContext<'_>, step: &PlatformDeploymentStep, mode: PlatformSyncMode,
) -> Result<ChartReference> {
  match mode {
    PlatformSyncMode::InstallLatest | PlatformSyncMode::UpdateLatest => {
      let chart = update::download_ret2shell_chart()?;
      let chart = copy_chart_to_system_cache(
        ctx,
        ChartReference {
          version: chart.version,
          source_path: chart.path.clone(),
          path: chart.path,
          download_url: chart.download_url,
          release_url: chart.release_url,
        },
      )?;

      prepare_patched_chart(ctx, chart)
    }
    PlatformSyncMode::SyncRecorded => {
      let chart_version = ctx
        .config()
        .install_step_metadata(step.id(), "chart_version")
        .map(str::to_string)
        .ok_or_else(|| {
          anyhow!("the installed ret2shell chart version is unknown; run `ret2boot update` first")
        })?;
      let configured_chart_source_path = ctx
        .config()
        .install_step_metadata(step.id(), "chart_source_path")
        .map(PathBuf::from)
        .or_else(|| {
          ctx
            .config()
            .install_step_metadata(step.id(), "chart_path")
            .map(PathBuf::from)
            .filter(|path| path.is_file())
        })
        .ok_or_else(|| {
          anyhow!("the cached ret2shell chart path is missing; run `ret2boot update` first")
        })?;
      let chart_source_path = if configured_chart_source_path.is_file() {
        configured_chart_source_path
      } else {
        let system_chart_path = system_chart_cache_path(&chart_version);

        if system_chart_path.is_file() {
          system_chart_path
        } else {
          bail!(
            "the cached ret2shell chart `{}` is missing; run `ret2boot update` first",
            configured_chart_source_path.display()
          );
        }
      };

      prepare_patched_chart(
        ctx,
        ChartReference {
          version: chart_version,
          source_path: chart_source_path.clone(),
          path: chart_source_path,
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
        },
      )
    }
  }
}

fn copy_chart_to_system_cache(
  ctx: &StepExecutionContext<'_>, chart: ChartReference,
) -> Result<ChartReference> {
  let system_chart_path = system_chart_cache_path(&chart.version);
  if !chart_cache_copy_required(&chart.source_path, &system_chart_path) {
    return Ok(ChartReference {
      source_path: system_chart_path.clone(),
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
      chart.source_path.display().to_string(),
      system_chart_path.display().to_string(),
    ],
    &[],
  )?;

  Ok(ChartReference {
    source_path: system_chart_path.clone(),
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

fn prepare_patched_chart(
  ctx: &StepExecutionContext<'_>, chart: ChartReference,
) -> Result<ChartReference> {
  install_directory(ctx, RET2SHELL_ROOT_DIR)?;
  install_directory(ctx, BACKEND_ROOT_DIR)?;
  install_directory(ctx, BACKEND_DEPLOYMENTS_DIR)?;
  install_directory(ctx, BACKEND_CHART_ROOT_DIR)?;

  ctx.run_privileged_command(
    "rm",
    &["-rf".to_string(), BACKEND_CHART_DIR.to_string()],
    &[],
  )?;
  ctx.run_privileged_command(
    "tar",
    &[
      "-xzf".to_string(),
      chart.source_path.display().to_string(),
      "-C".to_string(),
      BACKEND_CHART_ROOT_DIR.to_string(),
    ],
    &[],
  )?;

  let platform_template_path = format!("{BACKEND_CHART_DIR}/templates/platform.yaml");
  let original_template = read_privileged_text_file(ctx, &platform_template_path)?
    .ok_or_else(|| anyhow!("the extracted ret2shell chart is missing `templates/platform.yaml`"))?;
  let patched_template = patch_platform_chart_template(&original_template)?;
  let _ = sync_managed_text_file(ctx, &platform_template_path, &patched_template, "644")?;
  patch_postgresql_chart_templates(ctx)?;

  Ok(ChartReference {
    path: PathBuf::from(BACKEND_CHART_DIR),
    ..chart
  })
}

fn patch_platform_chart_template(contents: &str) -> Result<String> {
  let patched = replace_first_block(
    contents,
    PLATFORM_TEMPLATE_VOLUME_MOUNTS_MARKER,
    PLATFORM_TEMPLATE_RESOURCES_MARKER,
    PATCHED_PLATFORM_VOLUME_MOUNTS,
  )?;
  let patched = replace_first_block(
    &patched,
    PLATFORM_TEMPLATE_VOLUMES_MARKER,
    PLATFORM_TEMPLATE_NODE_SELECTOR_MARKER,
    PATCHED_PLATFORM_VOLUMES,
  )?;

  replace_first_block(
    &patched,
    PLATFORM_TEMPLATE_CONTAINERS_MARKER,
    PLATFORM_TEMPLATE_CONTAINERS_MARKER,
    PATCHED_PLATFORM_INIT_CONTAINERS,
  )
}

fn patch_postgresql_chart_templates(ctx: &StepExecutionContext<'_>) -> Result<()> {
  for template_path in
    find_chart_template_paths(Path::new(&format!("{BACKEND_CHART_DIR}/templates")))?
  {
    let display_path = template_path.display().to_string();
    let Some(original_template) = read_privileged_text_file(ctx, &display_path)? else {
      continue;
    };
    let patched_template = patch_postgresql_chart_template(&original_template);
    if patched_template != original_template {
      let _ = sync_managed_text_file(ctx, &display_path, &patched_template, "644")?;
    }
  }

  Ok(())
}

fn patch_postgresql_chart_template(contents: &str) -> String {
  contents.replace(
    POSTGRESQL_LEGACY_MOUNT_PATH,
    POSTGRESQL_PERSISTENT_MOUNT_PATH,
  )
}

fn find_chart_template_paths(root: &Path) -> Result<Vec<PathBuf>> {
  if !root.is_dir() {
    return Ok(Vec::new());
  }

  let mut paths = Vec::new();
  collect_chart_template_paths(root, &mut paths)?;
  Ok(paths)
}

fn collect_chart_template_paths(root: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
  for entry in fs::read_dir(root).with_context(|| {
    format!(
      "failed to read chart template directory `{}`",
      root.display()
    )
  })? {
    let entry = entry.with_context(|| {
      format!(
        "failed to inspect chart template directory `{}`",
        root.display()
      )
    })?;
    let path = entry.path();

    if path.is_dir() {
      collect_chart_template_paths(&path, paths)?;
      continue;
    }

    if matches!(
      path.extension().and_then(|extension| extension.to_str()),
      Some("yaml" | "yml" | "tpl")
    ) {
      paths.push(path);
    }
  }

  Ok(())
}

fn replace_first_block(
  contents: &str, start_marker: &str, end_marker: &str, replacement: &str,
) -> Result<String> {
  let (start, start_marker_len) = find_block_marker(contents, start_marker)
    .ok_or_else(|| anyhow!("unable to find the expected chart template marker `{start_marker}`"))?;
  let end = if start_marker == end_marker {
    start + start_marker_len
  } else {
    find_block_marker(&contents[start..], end_marker)
      .map(|(offset, _)| start + offset)
      .ok_or_else(|| anyhow!("unable to find the expected chart template marker `{end_marker}`"))?
  };

  Ok(format!(
    "{}{}{}",
    &contents[..start],
    replacement,
    &contents[end..]
  ))
}

fn find_block_marker(contents: &str, marker: &str) -> Option<(usize, usize)> {
  if let Some(index) = contents.find(marker) {
    return Some((index, marker.len()));
  }

  let trimmed = marker.trim_start();
  contents.find(trimmed).map(|index| {
    let line_start = contents[..index]
      .rfind('\n')
      .map(|newline| newline + 1)
      .unwrap_or(0);
    (line_start, index - line_start + trimmed.len())
  })
}

fn detect_node_hostname_label(
  ctx: &StepExecutionContext<'_>, cluster_access: &ClusterAccess,
) -> Result<String> {
  let shell_hostname = ctx
    .run_privileged_command_capture("hostname", &[], &[])?
    .trim()
    .to_string();

  if shell_hostname.is_empty() {
    bail!("failed to determine the local shell hostname for persistent volume planning");
  }

  let nodes_output = cluster_access.capture(
    ctx,
    &[
      "get".to_string(),
      "nodes".to_string(),
      "-o".to_string(),
      "json".to_string(),
    ],
  )?;
  let nodes: ClusterNodeList =
    serde_json::from_str(&nodes_output).context("failed to parse `kubectl get nodes -o json`")?;
  let identities = nodes
    .items
    .into_iter()
    .map(|node| {
      let hostname_label = node
        .metadata
        .labels
        .get("kubernetes.io/hostname")
        .cloned()
        .unwrap_or_else(|| node.metadata.name.clone());
      let is_control_plane = node
        .metadata
        .labels
        .contains_key("node-role.kubernetes.io/control-plane")
        || node
          .metadata
          .labels
          .contains_key("node-role.kubernetes.io/master");

      ClusterNodeIdentity {
        name: node.metadata.name,
        hostname_label,
        is_control_plane,
      }
    })
    .collect::<Vec<_>>();

  resolve_local_node_hostname_label(&shell_hostname, &identities)
}

fn resolve_local_node_hostname_label(
  shell_hostname: &str, nodes: &[ClusterNodeIdentity],
) -> Result<String> {
  if nodes.is_empty() {
    bail!("the kubernetes cluster returned no nodes while preparing local persistent volumes");
  }

  if nodes.len() == 1 {
    return Ok(nodes[0].hostname_label.clone());
  }

  let shell_short = short_hostname(shell_hostname);
  let shell_matches = nodes
    .iter()
    .filter(|node| node_matches_shell_hostname(node, shell_hostname, shell_short))
    .collect::<Vec<_>>();

  if shell_matches.len() == 1 {
    return Ok(shell_matches[0].hostname_label.clone());
  }

  let control_plane_nodes = nodes
    .iter()
    .filter(|node| node.is_control_plane)
    .collect::<Vec<_>>();
  if control_plane_nodes.len() == 1 {
    return Ok(control_plane_nodes[0].hostname_label.clone());
  }

  let available = nodes
    .iter()
    .map(|node| format!("{} ({})", node.name, node.hostname_label))
    .collect::<Vec<_>>()
    .join(", ");

  bail!(
    "unable to determine the local kubernetes node hostname label from shell hostname `{shell_hostname}`; available nodes: {available}"
  )
}

fn node_matches_shell_hostname(
  node: &ClusterNodeIdentity, shell_hostname: &str, shell_short: &str,
) -> bool {
  node.name == shell_hostname
    || node.hostname_label == shell_hostname
    || short_hostname(&node.name) == shell_short
    || short_hostname(&node.hostname_label) == shell_short
}

fn short_hostname(hostname: &str) -> &str {
  hostname.split('.').next().unwrap_or(hostname)
}

fn ensure_platform_host_layout(
  ctx: &StepExecutionContext<'_>, summary: &PlatformPlanSummary,
) -> Result<()> {
  for path in [
    RET2SHELL_ROOT_DIR,
    FRONTEND_HOST_DIR,
    BACKEND_ROOT_DIR,
    BACKEND_CONFIG_DIR,
    BACKEND_DEPLOYMENTS_DIR,
    BACKEND_CHART_ROOT_DIR,
    BACKEND_STORAGE_DIR,
    BACKEND_CACHE_DIR,
    BACKEND_DATABASE_DIR,
    BACKEND_QUEUE_DIR,
    BACKEND_REGISTRY_DIR,
    BACKEND_LOGS_DIR,
  ] {
    install_directory(ctx, path)?;
  }

  for service in &summary.services {
    if service.deployment == PlatformServiceDeploymentMode::Local
      && service.storage_mode == Some(PlatformStorageMode::LocalPath)
    {
      install_directory(ctx, storage_host_path(service.id))?;
      if let Some(subdirectory) = storage_host_subdirectory(service.id) {
        install_directory(ctx, subdirectory)?;
      }
      if service.id == PlatformServiceId::Database {
        prepare_postgresql_storage_host_layout(ctx)?;
      }
    }
  }

  Ok(())
}

fn prepare_postgresql_storage_host_layout(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let root = storage_host_path(PlatformServiceId::Database);

  // The official postgres entrypoint creates PGDATA itself and only adjusts
  // ownership automatically when the container starts as root. Our workload
  // runs as the image's default `postgres` user, so the mounted parent path
  // must already be writable for that unprivileged startup path.
  ctx.run_privileged_command(
    "install",
    &[
      "-d".to_string(),
      "-m".to_string(),
      POSTGRESQL_STORAGE_ROOT_MODE.to_string(),
      root.to_string(),
    ],
    &[],
  )?;
  ctx.run_privileged_command(
    "chmod",
    &[POSTGRESQL_STORAGE_ROOT_MODE.to_string(), root.to_string()],
    &[],
  )?;

  Ok(())
}

fn sync_managed_text_file(
  ctx: &StepExecutionContext<'_>, dest: &str, contents: &str, mode: &str,
) -> Result<bool> {
  if read_privileged_text_file(ctx, dest)?.as_deref() == Some(contents) {
    return Ok(false);
  }

  let staged = stage_text_file("ret2boot-managed", "txt", contents.to_string())?;
  let install_result = ctx.run_privileged_command(
    "install",
    &[
      "-D".to_string(),
      "-m".to_string(),
      mode.to_string(),
      staged.display().to_string(),
      dest.to_string(),
    ],
    &[],
  );
  let _ = fs::remove_file(&staged);
  install_result?;

  Ok(true)
}

fn sync_optional_managed_text_file(
  ctx: &StepExecutionContext<'_>, dest: &str, contents: Option<&str>, mode: &str,
) -> Result<bool> {
  match contents {
    Some(contents) => sync_managed_text_file(ctx, dest, contents, mode),
    None => {
      if Path::new(dest).is_file() {
        ctx.run_privileged_command("rm", &["-f".to_string(), dest.to_string()], &[])?;
        Ok(true)
      } else {
        Ok(false)
      }
    }
  }
}

fn sync_runtime_registry_config(
  ctx: &StepExecutionContext<'_>, summary: &PlatformPlanSummary,
) -> Result<bool> {
  let destination = match ctx
    .as_plan_context()
    .kubernetes_distribution()
    .ok_or_else(|| anyhow!("kubernetes distribution is required before syncing registry mirrors"))?
  {
    KubernetesDistribution::K3s => K3S_REGISTRIES_DEST,
    KubernetesDistribution::Rke2 => RKE2_REGISTRIES_DEST,
  };
  let registry = planned_service(summary, PlatformServiceId::Registry)?;
  let internal_registry_host = if registry.deployment == PlatformServiceDeploymentMode::Local {
    Some(summary.internal_registry_host.as_str())
  } else {
    None
  };
  let Some(contents) = render_container_registry_config(
    ctx
      .as_plan_context()
      .kubernetes_enable_china_registry_mirrors()
      .unwrap_or(false),
    internal_registry_host,
  ) else {
    return Ok(false);
  };

  sync_managed_text_file(ctx, destination, &contents, "600")
}

fn restart_cluster_runtime_service(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let service_name = match ctx
    .as_plan_context()
    .kubernetes_distribution()
    .ok_or_else(|| {
      anyhow!("kubernetes distribution is required before restarting the cluster runtime")
    })? {
    KubernetesDistribution::K3s => "k3s",
    KubernetesDistribution::Rke2 => "rke2-server",
  };

  ctx.run_privileged_command(
    "systemctl",
    &["restart".to_string(), service_name.to_string()],
    &[],
  )
}

fn uninstall_platform_release(
  ctx: &StepExecutionContext<'_>, helm_envs: &[(String, String)],
) -> Result<()> {
  let uninstall_args = vec![
    "uninstall".to_string(),
    HELM_RELEASE_NAME.to_string(),
    "-n".to_string(),
    PLATFORM_NAMESPACE.to_string(),
    "--ignore-not-found".to_string(),
    "--wait".to_string(),
    "--timeout".to_string(),
    HELM_UNINSTALL_TIMEOUT.to_string(),
  ];

  match ctx.run_privileged_command("helm", &uninstall_args, helm_envs) {
    Ok(()) => Ok(()),
    Err(error) if helm_uninstall_timed_out(&error) => {
      println!(
        "{}",
        ui::note(
          "helm uninstall timed out while waiting for platform resources to terminate; checking whether the release has already been removed"
        )
      );

      for _ in 0..HELM_UNINSTALL_POLL_RETRIES {
        if current_helm_release(ctx, helm_envs)?.is_none() {
          println!(
            "{}",
            ui::note(
              "helm release is already gone; continuing with installer-managed resource cleanup"
            )
          );
          return Ok(());
        }

        thread::sleep(Duration::from_secs(HELM_UNINSTALL_POLL_INTERVAL_SECONDS));
      }

      Err(error)
    }
    Err(error) => Err(error),
  }
}

fn helm_uninstall_timed_out(error: &anyhow::Error) -> bool {
  error.to_string().contains("context deadline exceeded")
}

fn sync_deployment_exports(
  ctx: &StepExecutionContext<'_>, desired_values: &str, storage_manifest: Option<&str>,
) -> Result<()> {
  let _ = sync_managed_text_file(
    ctx,
    BACKEND_INIT_MANIFEST_DEST,
    &render_platform_init_manifest(),
    "644",
  )?;

  if let Some(storage_manifest) = storage_manifest {
    let _ = sync_managed_text_file(ctx, BACKEND_VOLUMES_MANIFEST_DEST, storage_manifest, "644")?;
  }

  let _ = sync_managed_text_file(
    ctx,
    BACKEND_PLATFORM_VALUES_DEST,
    &extract_chart_section(desired_values, "platform")?,
    "644",
  )?;

  for service in [
    PlatformServiceId::Cache,
    PlatformServiceId::Database,
    PlatformServiceId::Queue,
    PlatformServiceId::Registry,
    PlatformServiceId::Logs,
  ] {
    let destination = format!(
      "{}/chart-values.yaml",
      deployment_service_directory(service).expect("service deployment directory exists")
    );
    let _ = sync_managed_text_file(
      ctx,
      &destination,
      &extract_chart_section(desired_values, chart_section_key(service))?,
      "644",
    )?;
  }

  Ok(())
}

fn load_reference_config_files() -> Result<ReferenceConfigFiles> {
  Ok(ReferenceConfigFiles {
    config_toml: read_first_existing_text(&["config.toml", "Raw_Config_files/config/config.toml"])?,
    blocked_content: read_first_existing_text(&[
      "blocked.txt",
      "Raw_Config_files/config/blocked.txt",
    ])?,
    license_content: read_first_existing_text(&["license", "Raw_Config_files/config/license"])?,
  })
}

fn read_first_existing_text(paths: &[&str]) -> Result<Option<String>> {
  for path in paths {
    let candidate = Path::new(path);
    if !candidate.is_file() {
      continue;
    }

    let contents = fs::read_to_string(candidate).with_context(|| {
      format!(
        "failed to read reference config file `{}`",
        candidate.display()
      )
    })?;
    return Ok(Some(contents));
  }

  Ok(None)
}

fn merge_runtime_config_with_reference(generated: &str, reference: Option<&str>) -> Result<String> {
  let Some(reference) = reference else {
    return Ok(generated.to_string());
  };

  let mut generated_value: TomlValue =
    toml::from_str(generated).context("failed to parse generated runtime config TOML")?;
  let reference_value: TomlValue =
    toml::from_str(reference).context("failed to parse reference runtime config TOML")?;

  for path in [
    &["cluster", "node_selector"][..],
    &["cluster", "enable_capture"][..],
    &["cluster", "capture_directory"][..],
    &["cluster", "traffic"][..],
    &["cluster", "lifecycle"][..],
    &["media", "anti_theft"][..],
    &["media", "limit"][..],
    &["email"][..],
    &["server", "name"][..],
    &["server", "footer_info"][..],
    &["server", "footer_url"][..],
    &["server", "subject_info"][..],
    &["server", "subject_url"][..],
    &["server", "record"][..],
    &["server", "hide_maker"][..],
    &["server", "highlight_banner"][..],
    &["server", "zen_game"][..],
    &["server", "rate_limit"][..],
  ] {
    copy_toml_path(&reference_value, &mut generated_value, path);
  }

  let mut rendered =
    toml::to_string_pretty(&generated_value).context("failed to serialize runtime config TOML")?;
  if !rendered.ends_with('\n') {
    rendered.push('\n');
  }

  Ok(rendered)
}

fn copy_toml_path(source: &TomlValue, destination: &mut TomlValue, path: &[&str]) -> bool {
  let Some(value) = get_toml_path(source, path).cloned() else {
    return false;
  };

  set_toml_path(destination, path, value)
}

fn get_toml_path<'a>(value: &'a TomlValue, path: &[&str]) -> Option<&'a TomlValue> {
  let mut current = value;
  for segment in path {
    current = current.get(*segment)?;
  }

  Some(current)
}

fn set_toml_path(value: &mut TomlValue, path: &[&str], replacement: TomlValue) -> bool {
  if path.is_empty() {
    *value = replacement;
    return true;
  }

  let Some(table) = value.as_table_mut() else {
    return false;
  };

  if path.len() == 1 {
    table.insert(path[0].to_string(), replacement);
    return true;
  }

  let entry = table
    .entry(path[0].to_string())
    .or_insert_with(|| TomlValue::Table(Default::default()));

  set_toml_path(entry, &path[1..], replacement)
}

fn render_platform_init_manifest() -> String {
  [
    "apiVersion: v1".to_string(),
    "kind: Namespace".to_string(),
    "metadata:".to_string(),
    format!("  name: {}", yaml_quote(CHALLENGE_NAMESPACE)),
    "---".to_string(),
    "apiVersion: v1".to_string(),
    "kind: Namespace".to_string(),
    "metadata:".to_string(),
    format!("  name: {}", yaml_quote(PLATFORM_NAMESPACE)),
    "---".to_string(),
    "apiVersion: v1".to_string(),
    "kind: ServiceAccount".to_string(),
    "metadata:".to_string(),
    "  name: 'ret2shell-service'".to_string(),
    format!("  namespace: {}", yaml_quote(PLATFORM_NAMESPACE)),
    "automountServiceAccountToken: true".to_string(),
    "---".to_string(),
    "apiVersion: rbac.authorization.k8s.io/v1".to_string(),
    "kind: ClusterRoleBinding".to_string(),
    "metadata:".to_string(),
    "  name: 'ret2shell-service-global'".to_string(),
    "subjects:".to_string(),
    "  - kind: ServiceAccount".to_string(),
    "    name: 'ret2shell-service'".to_string(),
    format!("    namespace: {}", yaml_quote(PLATFORM_NAMESPACE)),
    "roleRef:".to_string(),
    "  apiGroup: rbac.authorization.k8s.io".to_string(),
    "  kind: ClusterRole".to_string(),
    "  name: 'cluster-admin'".to_string(),
    String::new(),
  ]
  .join("\n")
}

fn extract_chart_section(full_values: &str, key: &str) -> Result<String> {
  let parsed = parse_yaml_value(full_values)?;
  let root = parsed
    .as_mapping()
    .ok_or_else(|| anyhow!("rendered helm values must be a YAML mapping"))?;
  let section = root
    .get(YamlValue::String(key.to_string()))
    .cloned()
    .ok_or_else(|| anyhow!("rendered helm values are missing section `{key}`"))?;
  let mut mapping = Mapping::new();
  mapping.insert(YamlValue::String(key.to_string()), section);
  let mut rendered =
    serde_yaml::to_string(&YamlValue::Mapping(mapping)).context("failed to serialize YAML")?;
  if !rendered.ends_with('\n') {
    rendered.push('\n');
  }

  Ok(rendered)
}

fn deployment_service_directory(service: PlatformServiceId) -> Option<&'static str> {
  match service {
    PlatformServiceId::Platform => None,
    PlatformServiceId::Cache => Some(BACKEND_CACHE_DIR),
    PlatformServiceId::Database => Some(BACKEND_DATABASE_DIR),
    PlatformServiceId::Queue => Some(BACKEND_QUEUE_DIR),
    PlatformServiceId::Registry => Some(BACKEND_REGISTRY_DIR),
    PlatformServiceId::Logs => Some(BACKEND_LOGS_DIR),
  }
}

fn chart_section_key(service: PlatformServiceId) -> &'static str {
  match service {
    PlatformServiceId::Platform => "platform",
    PlatformServiceId::Database => "postgresql",
    PlatformServiceId::Cache => "valkey",
    PlatformServiceId::Queue => "nats",
    PlatformServiceId::Registry => "registry",
    PlatformServiceId::Logs => "victoriaLogs",
  }
}

fn storage_host_path(service: PlatformServiceId) -> &'static str {
  match service {
    PlatformServiceId::Platform => "/srv/ret2shell/backend/storage/platform-pv1",
    PlatformServiceId::Database => "/srv/ret2shell/backend/storage/database-pv1",
    PlatformServiceId::Cache => "/srv/ret2shell/backend/storage/cache-pv1",
    PlatformServiceId::Queue => "/srv/ret2shell/backend/storage/queue-pv1",
    PlatformServiceId::Registry => "/srv/ret2shell/backend/storage/registry-pv1",
    PlatformServiceId::Logs => "/srv/ret2shell/backend/storage/logs-pv1",
  }
}

fn storage_host_subdirectory(service: PlatformServiceId) -> Option<&'static str> {
  match service {
    PlatformServiceId::Database => None,
    _ => None,
  }
}

fn storage_persistent_volume_name(service: PlatformServiceId) -> String {
  format!("ret2shell-storage-{}-pv1", service.as_config_value())
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
        "chart_source_path",
        chart.source_path.display().to_string(),
      ) || changed;
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
  let desired_resources = desired_storage_manifest
    .as_deref()
    .map(extract_manifest_resources)
    .transpose()?
    .unwrap_or_default();
  let mut missing_resources = false;
  for resource in &desired_resources {
    if !cluster_access.cluster_scoped_resource_exists(
      ctx,
      manifest_resource_type(resource.kind.as_str())?,
      resource.name.as_str(),
    )? {
      missing_resources = true;
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

      if manifest_changed || file_changed || missing_resources {
        cluster_access.apply_manifest(ctx, PLATFORM_STORAGE_DEST)?;
      }

      ctx.persist_change(
        "install.execution.platform.storage",
        PLATFORM_STORAGE_DEST,
        |config| config.set_install_step_metadata(step.id(), "storage_path", PLATFORM_STORAGE_DEST),
      )?;

      Ok(manifest_changed || file_changed || missing_resources)
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

fn extract_manifest_resources(manifest: &str) -> Result<Vec<ManifestResource>> {
  let documents = Deserializer::from_str(manifest)
    .map(YamlValue::deserialize)
    .collect::<std::result::Result<Vec<_>, _>>()
    .context("failed to parse the rendered storage manifest")?;

  Ok(
    documents
      .into_iter()
      .filter_map(|document| {
        Some(ManifestResource {
          kind: document["kind"].as_str()?.to_string(),
          name: document["metadata"]["name"].as_str()?.to_string(),
        })
      })
      .collect(),
  )
}

fn manifest_resource_type(kind: &str) -> Result<&'static str> {
  match kind {
    "StorageClass" => Ok("storageclass"),
    "PersistentVolume" => Ok("persistentvolume"),
    _ => bail!("unsupported manifest resource kind `{kind}` in platform storage manifest"),
  }
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
    platform_mount_layout: cluster_access
      .capture_namespaced_object_yaml(ctx, PLATFORM_NAMESPACE, "statefulset", "ret2shell-platform")?
      .map(|document| extract_platform_mount_layout(&document)),
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
  platform_mount_layout: YamlValue,
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
  let mut platform_mount_layout = None;

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
      (Some("StatefulSet"), Some("ret2shell-platform")) => {
        platform_mount_layout = Some(extract_platform_mount_layout(&document));
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
    platform_mount_layout: platform_mount_layout.ok_or_else(|| {
      anyhow!("the rendered ret2shell chart did not contain the platform StatefulSet")
    })?,
  })
}

fn parse_yaml_value(contents: &str) -> Result<YamlValue> {
  if contents.trim().is_empty() {
    return Ok(YamlValue::Null);
  }

  serde_yaml::from_str(contents).context("failed to parse yaml content")
}

fn extract_platform_mount_layout(document: &YamlValue) -> YamlValue {
  let template_spec = &document["spec"]["template"]["spec"];
  let platform_volume_mounts = template_spec["containers"]
    .as_sequence()
    .and_then(|containers| {
      containers
        .iter()
        .find(|container| container["name"].as_str() == Some("platform"))
    })
    .map(|container| {
      YamlValue::Sequence(
        container["volumeMounts"]
          .as_sequence()
          .into_iter()
          .flatten()
          .map(|mount| {
            let mut normalized = Mapping::new();
            normalized.insert(YamlValue::String("name".to_string()), mount["name"].clone());
            normalized.insert(
              YamlValue::String("mountPath".to_string()),
              mount["mountPath"].clone(),
            );
            if !mount["subPath"].is_null() {
              normalized.insert(
                YamlValue::String("subPath".to_string()),
                mount["subPath"].clone(),
              );
            }
            if !mount["readOnly"].is_null() {
              normalized.insert(
                YamlValue::String("readOnly".to_string()),
                mount["readOnly"].clone(),
              );
            }

            YamlValue::Mapping(normalized)
          })
          .collect(),
      )
    })
    .unwrap_or_else(|| YamlValue::Sequence(Vec::new()));
  let normalized_init_containers = YamlValue::Sequence(
    template_spec["initContainers"]
      .as_sequence()
      .into_iter()
      .flatten()
      .map(|container| {
        let mut normalized = Mapping::new();
        normalized.insert(
          YamlValue::String("name".to_string()),
          container["name"].clone(),
        );
        normalized.insert(
          YamlValue::String("command".to_string()),
          container["command"].clone(),
        );
        normalized.insert(
          YamlValue::String("volumeMounts".to_string()),
          YamlValue::Sequence(
            container["volumeMounts"]
              .as_sequence()
              .into_iter()
              .flatten()
              .map(|mount| {
                let mut normalized_mount = Mapping::new();
                normalized_mount
                  .insert(YamlValue::String("name".to_string()), mount["name"].clone());
                normalized_mount.insert(
                  YamlValue::String("mountPath".to_string()),
                  mount["mountPath"].clone(),
                );
                if !mount["subPath"].is_null() {
                  normalized_mount.insert(
                    YamlValue::String("subPath".to_string()),
                    mount["subPath"].clone(),
                  );
                }
                if !mount["readOnly"].is_null() {
                  normalized_mount.insert(
                    YamlValue::String("readOnly".to_string()),
                    mount["readOnly"].clone(),
                  );
                }

                YamlValue::Mapping(normalized_mount)
              })
              .collect(),
          ),
        );

        YamlValue::Mapping(normalized)
      })
      .collect(),
  );
  let normalized_volumes = YamlValue::Sequence(
    template_spec["volumes"]
      .as_sequence()
      .into_iter()
      .flatten()
      .map(|volume| {
        let mut normalized = Mapping::new();
        normalized.insert(
          YamlValue::String("name".to_string()),
          volume["name"].clone(),
        );
        if !volume["hostPath"].is_null() {
          let mut host_path = Mapping::new();
          host_path.insert(
            YamlValue::String("path".to_string()),
            volume["hostPath"]["path"].clone(),
          );
          host_path.insert(
            YamlValue::String("type".to_string()),
            volume["hostPath"]["type"].clone(),
          );
          normalized.insert(
            YamlValue::String("hostPath".to_string()),
            YamlValue::Mapping(host_path),
          );
        }
        if !volume["secret"].is_null() {
          let mut secret = Mapping::new();
          secret.insert(
            YamlValue::String("secretName".to_string()),
            volume["secret"]["secretName"].clone(),
          );
          normalized.insert(
            YamlValue::String("secret".to_string()),
            YamlValue::Mapping(secret),
          );
        }
        if !volume["configMap"].is_null() {
          let mut config_map = Mapping::new();
          config_map.insert(
            YamlValue::String("name".to_string()),
            volume["configMap"]["name"].clone(),
          );
          normalized.insert(
            YamlValue::String("configMap".to_string()),
            YamlValue::Mapping(config_map),
          );
        }
        if !volume["persistentVolumeClaim"].is_null() {
          let mut claim = Mapping::new();
          claim.insert(
            YamlValue::String("claimName".to_string()),
            volume["persistentVolumeClaim"]["claimName"].clone(),
          );
          normalized.insert(
            YamlValue::String("persistentVolumeClaim".to_string()),
            YamlValue::Mapping(claim),
          );
        }
        if !volume["emptyDir"].is_null() {
          normalized.insert(
            YamlValue::String("emptyDir".to_string()),
            YamlValue::Mapping(Mapping::new()),
          );
        }

        YamlValue::Mapping(normalized)
      })
      .collect(),
  );

  let mut layout = Mapping::new();
  layout.insert(
    YamlValue::String("initContainers".to_string()),
    normalized_init_containers,
  );
  layout.insert(YamlValue::String("volumes".to_string()), normalized_volumes);
  layout.insert(
    YamlValue::String("platformVolumeMounts".to_string()),
    platform_volume_mounts,
  );

  YamlValue::Mapping(layout)
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
  let external_https = summary.tls_enabled;
  let external_origin = derive_external_origin(&summary.public_host, external_https);

  let mut lines = vec!["platform:".to_string()];
  lines.push("  exposure:".to_string());
  match summary.application_exposure {
    ApplicationExposureMode::Ingress => {
      lines.push("    type: ingress".to_string());
      lines.push("  ingress:".to_string());
      lines.push(format!(
        "    className: {}",
        yaml_quote(&summary.ingress_class_name)
      ));
      lines.push("    annotations:".to_string());
      lines.push(format!(
        "      kubernetes.io/ingress.class: {}",
        yaml_quote(&summary.ingress_class_name)
      ));
      if summary.tls_enabled {
        lines.push(format!(
          "      nginx.ingress.kubernetes.io/ssl-redirect: {}",
          yaml_quote("true")
        ));
      }
      lines.push("    hosts:".to_string());
      lines.push(format!(
        "      - host: {}",
        yaml_quote(&summary.ingress_host)
      ));
      lines.push("        paths:".to_string());
      lines.push(format!(
        "          - path: {}",
        yaml_quote(PLATFORM_INGRESS_PATH)
      ));
      lines.push(format!(
        "            pathType: {PLATFORM_INGRESS_PATH_TYPE}"
      ));
      if let Some(tls_secret_name) = &summary.tls_secret_name {
        lines.push("    tls:".to_string());
        lines.push(format!(
          "      - secretName: {}",
          yaml_quote(tls_secret_name)
        ));
        lines.push("        hosts:".to_string());
        lines.push(format!("          - {}", yaml_quote(&summary.ingress_host)));
      }
    }
    ApplicationExposureMode::NodePortExternalNginx => {
      lines.push("    type: nodePort".to_string());
      lines.push("  service:".to_string());
      lines.push(format!("    nodePort: {PLATFORM_NODE_PORT}"));
    }
  }
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
  lines.push(format!("      externalHttps: {external_https}"));
  lines.push(format!(
    "      corsOrigins: {}",
    yaml_quote(&external_origin)
  ));
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

fn render_platform_storage_manifest(
  summary: &PlatformPlanSummary, node_hostname: Option<&str>,
) -> Option<String> {
  let local_services = summary
    .services
    .iter()
    .filter(|service| {
      service.deployment == PlatformServiceDeploymentMode::Local
        && service.storage_mode == Some(PlatformStorageMode::LocalPath)
    })
    .collect::<Vec<_>>();

  if local_services.is_empty() {
    return None;
  }

  let storage_classes = local_services
    .iter()
    .filter_map(|service| service.storage_class_name.clone())
    .collect::<BTreeSet<_>>();
  let node_hostname = node_hostname.unwrap_or("ret2shell-node-master");
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
    lines.push("provisioner: kubernetes.io/no-provisioner".to_string());
    lines.push("reclaimPolicy: Retain".to_string());
    lines.push("allowVolumeExpansion: true".to_string());
    lines.push("volumeBindingMode: WaitForFirstConsumer".to_string());
  }
  for service in local_services {
    if !lines.is_empty() {
      lines.push("---".to_string());
    }

    let storage_class = service
      .storage_class_name
      .as_deref()
      .expect("local service has storage class");
    let size_gib = service
      .persistence_size_gib
      .expect("local service has persistence size");
    lines.push("apiVersion: v1".to_string());
    lines.push("kind: PersistentVolume".to_string());
    lines.push("metadata:".to_string());
    lines.push(format!(
      "  name: {}",
      yaml_quote(&storage_persistent_volume_name(service.id))
    ));
    lines.push("spec:".to_string());
    lines.push("  capacity:".to_string());
    lines.push(format!("    storage: {size_gib}Gi"));
    lines.push("  accessModes:".to_string());
    lines.push("    - ReadWriteOnce".to_string());
    lines.push("  persistentVolumeReclaimPolicy: Retain".to_string());
    lines.push(format!("  storageClassName: {}", yaml_quote(storage_class)));
    lines.push("  local:".to_string());
    lines.push(format!(
      "    path: {}",
      yaml_quote(storage_host_path(service.id))
    ));
    lines.push("  nodeAffinity:".to_string());
    lines.push("    required:".to_string());
    lines.push("      nodeSelectorTerms:".to_string());
    lines.push("        - matchExpressions:".to_string());
    lines.push("            - key: kubernetes.io/hostname".to_string());
    lines.push("              operator: In".to_string());
    lines.push("              values:".to_string());
    lines.push(format!("                - {}", yaml_quote(node_hostname)));
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

pub(crate) fn resolve_public_endpoint(
  raw: &str, exposure: ApplicationExposureMode,
) -> Result<ResolvedPublicEndpoint> {
  let host = canonical_public_host_input(raw)?;
  validate_public_host_for_exposure(host, exposure)?;

  if public_host_is_ipv4(host) {
    return Ok(ResolvedPublicEndpoint {
      public_host: host.to_string(),
      ingress_host: match exposure {
        ApplicationExposureMode::Ingress => host.to_string(),
        ApplicationExposureMode::NodePortExternalNginx => {
          derive_internal_ingress_host_from_ipv4(host)
        }
      },
    });
  }

  Ok(ResolvedPublicEndpoint {
    public_host: host.to_string(),
    ingress_host: host.to_string(),
  })
}

fn validate_public_host_for_exposure(host: &str, exposure: ApplicationExposureMode) -> Result<()> {
  match exposure {
    ApplicationExposureMode::Ingress => {
      if public_host_is_ipv4(host) {
        bail!(
          "{}",
          t!("install.platform.public_host.ingress_requires_hostname")
        );
      }

      Ok(())
    }
    ApplicationExposureMode::NodePortExternalNginx => Ok(()),
  }
}

fn canonical_public_host_input(raw: &str) -> Result<&str> {
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
      .unwrap_or(trimmed),
  )
}

fn supported_tls_modes(
  exposure: ApplicationExposureMode, public_host: &str,
) -> Vec<PlatformTlsMode> {
  if supports_acme_dns_for_host(exposure, public_host) {
    vec![PlatformTlsMode::AcmeDns, PlatformTlsMode::ProvidedFiles]
  } else {
    vec![PlatformTlsMode::ProvidedFiles]
  }
}

fn supports_acme_dns_for_host(exposure: ApplicationExposureMode, public_host: &str) -> bool {
  matches!(
    exposure,
    ApplicationExposureMode::NodePortExternalNginx | ApplicationExposureMode::Ingress
  ) && !public_host_is_ipv4(public_host)
    && !public_host.ends_with(".nip.io")
    && !public_host.ends_with(".sslip.io")
}

fn tls_mode_label(mode: PlatformTlsMode) -> String {
  match mode {
    PlatformTlsMode::Disabled => t!("install.platform.tls.mode.disabled").to_string(),
    PlatformTlsMode::AcmeDns => t!("install.platform.tls.mode.acme_dns").to_string(),
    PlatformTlsMode::ProvidedFiles => t!("install.platform.tls.mode.provided_files").to_string(),
  }
}

fn derive_internal_registry_host(public_host: &str) -> String {
  format!("{public_host}:{INTERNAL_REGISTRY_NODE_PORT}")
}

fn resolved_managed_tls_material_paths(ctx: &StepPlanContext<'_>) -> Result<(String, String)> {
  let exposure = ctx.application_exposure().ok_or_else(|| {
    anyhow!("application exposure mode is required before resolving managed TLS paths")
  })?;
  let asset_name = managed_tls_asset_name(exposure, ctx.platform_tls_secret_name())?;

  Ok((
    managed_tls_certificate_path(&asset_name),
    managed_tls_key_path(&asset_name),
  ))
}

fn derive_external_origin(public_host: &str, external_https: bool) -> String {
  let scheme = if external_https { "https" } else { "http" };

  format!("{scheme}://{public_host}")
}

fn public_host_is_ipv4(public_host: &str) -> bool {
  public_host.parse::<Ipv4Addr>().is_ok()
}

fn derive_internal_ingress_host_from_ipv4(public_host: &str) -> String {
  let dashed = public_host.replace('.', "-");

  format!("ret2shell-{dashed}.{INTERNAL_INGRESS_HOST_SUFFIX}")
}

fn public_host_prompt(exposure: ApplicationExposureMode) -> String {
  match exposure {
    ApplicationExposureMode::NodePortExternalNginx => {
      t!("install.platform.public_host.prompt.nodeport").to_string()
    }
    ApplicationExposureMode::Ingress => {
      t!("install.platform.public_host.prompt.ingress").to_string()
    }
  }
}

fn platform_ingress_class(distribution: KubernetesDistribution) -> &'static str {
  match distribution {
    KubernetesDistribution::K3s => "nginx",
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
      format!("--timeout={NODE_READY_WAIT_TIMEOUT}"),
    ];
    let mut last_error = None;

    for _ in 0..NODE_READY_WAIT_RETRIES {
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

  fn cluster_scoped_resource_exists(
    &self, ctx: &StepExecutionContext<'_>, resource: &str, name: &str,
  ) -> Result<bool> {
    let output = self.capture(
      ctx,
      &[
        "get".to_string(),
        resource.to_string(),
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

  fn capture_namespaced_object_yaml(
    &self, ctx: &StepExecutionContext<'_>, namespace: &str, resource: &str, name: &str,
  ) -> Result<Option<YamlValue>> {
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

    let yaml = self.capture(
      ctx,
      &[
        "-n".to_string(),
        namespace.to_string(),
        "get".to_string(),
        resource.to_string(),
        name.to_string(),
        "-o".to_string(),
        "yaml".to_string(),
      ],
    )?;

    parse_yaml_value(&yaml).map(Some)
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
  fn resolve_public_endpoint_strips_scheme_path_and_port_for_dns_hosts() {
    let endpoint = resolve_public_endpoint(
      "https://ctf.example.com:8443/ui",
      ApplicationExposureMode::Ingress,
    )
    .expect("host parses");

    assert_eq!(endpoint.public_host, "ctf.example.com");
    assert_eq!(endpoint.ingress_host, "ctf.example.com");
  }

  #[test]
  fn render_platform_values_yaml_maps_chart_settings() {
    let summary = sample_summary();
    let rendered = render_platform_values_yaml(&summary).expect("values render");
    let parsed: Value = serde_yaml::from_str(&rendered).expect("values parse as yaml");

    assert_eq!(
      parsed["platform"]["exposure"]["type"],
      Value::String("ingress".to_string())
    );
    assert_eq!(
      parsed["platform"]["ingress"]["hosts"][0]["host"],
      Value::String("ctf.example.com".to_string())
    );
    assert_eq!(
      parsed["platform"]["ingress"]["className"],
      Value::String("nginx".to_string())
    );
    assert_eq!(
      parsed["platform"]["ingress"]["tls"][0]["secretName"],
      Value::String("ret2shell-tls".to_string())
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
  fn render_platform_values_yaml_maps_local_lab_profile_to_nodeport() {
    let mut summary = sample_summary();
    summary.application_exposure = ApplicationExposureMode::NodePortExternalNginx;
    summary.tls_enabled = false;
    summary.public_host = "192.168.23.132".to_string();
    summary.ingress_host = "ret2shell-192-168-23-132.ret2boot.invalid".to_string();
    summary.internal_registry_host = "192.168.23.132:30310".to_string();
    let rendered = render_platform_values_yaml(&summary).expect("values render");
    let parsed: Value = serde_yaml::from_str(&rendered).expect("values parse as yaml");

    assert_eq!(
      parsed["platform"]["exposure"]["type"],
      Value::String("nodePort".to_string())
    );
    assert_eq!(
      parsed["platform"]["service"]["nodePort"],
      Value::Number(30307.into())
    );
    assert_eq!(
      parsed["platform"]["config"]["server"]["externalDomain"],
      Value::String("192.168.23.132".to_string())
    );
    assert_eq!(
      parsed["platform"]["config"]["server"]["externalHttps"],
      Value::Bool(false)
    );
    assert_eq!(
      parsed["platform"]["config"]["server"]["corsOrigins"],
      Value::String("http://192.168.23.132".to_string())
    );
  }

  #[test]
  fn render_platform_values_yaml_maps_ingress_profile_to_https_origin() {
    let summary = sample_summary();
    let rendered = render_platform_values_yaml(&summary).expect("values render");
    let parsed: Value = serde_yaml::from_str(&rendered).expect("values parse as yaml");

    assert_eq!(
      parsed["platform"]["config"]["server"]["externalHttps"],
      Value::Bool(true)
    );
    assert_eq!(
      parsed["platform"]["config"]["server"]["corsOrigins"],
      Value::String("https://ctf.example.com".to_string())
    );
  }

  #[test]
  fn resolve_public_endpoint_rejects_bare_ipv4_for_ingress() {
    resolve_public_endpoint("103.151.173.97", ApplicationExposureMode::Ingress)
      .expect_err("ingress should require a hostname");
  }

  #[test]
  fn resolve_public_endpoint_keeps_bare_ipv4_for_nodeport_nginx_mode() {
    let endpoint = resolve_public_endpoint(
      "103.151.173.97",
      ApplicationExposureMode::NodePortExternalNginx,
    )
    .expect("bare IPv4 input should be supported through external nginx");

    assert_eq!(endpoint.public_host, "103.151.173.97");
    assert_eq!(
      endpoint.ingress_host,
      "ret2shell-103-151-173-97.ret2boot.invalid"
    );
  }

  #[test]
  fn render_platform_storage_manifest_emits_local_pvs_and_storage_classes() {
    let summary = sample_summary();
    let rendered = render_platform_storage_manifest(&summary, Some("ret2shell-node-master"))
      .expect("storage manifest exists");
    let documents = Deserializer::from_str(&rendered)
      .map(Value::deserialize)
      .collect::<std::result::Result<Vec<_>, _>>()
      .expect("storage manifest parses as yaml");
    let storage_classes = documents
      .iter()
      .filter(|document| document["kind"].as_str() == Some("StorageClass"))
      .map(|document| {
        document["metadata"]["name"]
          .as_str()
          .unwrap_or_default()
          .to_string()
      })
      .collect::<Vec<_>>();
    let persistent_volumes = documents
      .iter()
      .filter(|document| document["kind"].as_str() == Some("PersistentVolume"))
      .into_iter()
      .map(|document| {
        (
          document["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
          document["spec"]["local"]["path"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        )
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
    assert_eq!(
      persistent_volumes,
      vec![
        (
          "ret2shell-storage-platform-pv1".to_string(),
          "/srv/ret2shell/backend/storage/platform-pv1".to_string()
        ),
        (
          "ret2shell-storage-cache-pv1".to_string(),
          "/srv/ret2shell/backend/storage/cache-pv1".to_string()
        ),
        (
          "ret2shell-storage-registry-pv1".to_string(),
          "/srv/ret2shell/backend/storage/registry-pv1".to_string()
        ),
      ]
    );
  }

  #[test]
  fn database_storage_host_layout_uses_volume_root_for_official_postgres() {
    assert_eq!(
      storage_host_path(PlatformServiceId::Database),
      "/srv/ret2shell/backend/storage/database-pv1"
    );
    assert_eq!(storage_host_subdirectory(PlatformServiceId::Database), None);
    assert_eq!(storage_host_subdirectory(PlatformServiceId::Cache), None);
  }

  #[test]
  fn database_storage_root_mode_allows_official_postgres_to_create_pgdata() {
    assert_eq!(POSTGRESQL_STORAGE_ROOT_MODE, "1777");
  }

  #[test]
  fn patch_postgresql_chart_template_moves_volume_to_pgdata_parent() {
    let original = "\
volumeMounts:\n\
  - name: data\n\
    mountPath: /var/lib/postgresql/data\n";

    let patched = patch_postgresql_chart_template(original);

    assert!(patched.contains("mountPath: /var/lib/postgresql\n"));
    assert!(!patched.contains("mountPath: /var/lib/postgresql/data"));
  }

  #[test]
  fn patch_platform_chart_template_keeps_platform_container_after_inserting_init_containers() {
    let original = "\
spec:\n\
      containers:\n\
        - name: platform\n\
          image: example\n\
          volumeMounts:\n\
            - name: data\n\
              mountPath: /var/lib/ret2shell\n\
          {{- with .Values.platform.resources }}\n\
          resources:\n\
            {}\n\
          {{- end }}\n\
      volumes:\n\
        - name: config\n\
          secret:\n\
            secretName: ret2shell-config\n\
      {{- with .Values.platform.nodeSelector }}\n\
      nodeSelector:\n\
        {}\n\
      {{- end }}\n";
    let patched = patch_platform_chart_template(original).expect("template patch succeeds");

    assert!(patched.contains("initContainers:"));
    assert!(patched.contains("- name: platform"));
    assert!(patched.contains("mountPath: /var/www/html"));
    assert!(patched.contains("path: /srv/ret2shell/frontend"));
    assert!(patched.contains("mountPath: /etc/ret2shell"));
    assert!(!patched.contains("find /host-frontend -mindepth 1 -maxdepth 1 -exec rm -rf {} +"));
    assert!(!patched.contains("name: config-sync"));
    assert!(!patched.contains("name: generated-config"));
  }

  #[test]
  fn chart_cache_copy_is_skipped_when_source_is_already_in_system_cache() {
    let path =
      Path::new("/var/cache/ret2shell/ret2boot/charts/ret2shell/3.10.4/ret2shell-3.10.4.tgz");

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
  fn public_host_is_ipv4_detects_bare_ipv4_only() {
    assert!(public_host_is_ipv4("103.151.173.97"));
    assert!(!public_host_is_ipv4("ctf.example.com"));
  }

  #[test]
  fn derive_internal_ingress_host_from_ipv4_formats_dns_safe_host() {
    assert_eq!(
      derive_internal_ingress_host_from_ipv4("103.151.173.97"),
      "ret2shell-103-151-173-97.ret2boot.invalid"
    );
  }

  #[test]
  fn supported_tls_modes_only_offer_provided_files_for_bare_ipv4_nodeport() {
    assert_eq!(
      supported_tls_modes(
        ApplicationExposureMode::NodePortExternalNginx,
        "103.151.173.97"
      ),
      vec![PlatformTlsMode::ProvidedFiles]
    );
  }

  #[test]
  fn resolve_local_node_hostname_label_prefers_exact_hostname_label_match() {
    let nodes = vec![
      ClusterNodeIdentity {
        name: "k3s-master".to_string(),
        hostname_label: "ret2shell-node-master".to_string(),
        is_control_plane: true,
      },
      ClusterNodeIdentity {
        name: "k3s-worker".to_string(),
        hostname_label: "ret2shell-node-worker".to_string(),
        is_control_plane: false,
      },
    ];

    assert_eq!(
      resolve_local_node_hostname_label("ret2shell-node-master", &nodes)
        .expect("local label resolves"),
      "ret2shell-node-master"
    );
  }

  #[test]
  fn resolve_local_node_hostname_label_matches_short_hostname() {
    let nodes = vec![ClusterNodeIdentity {
      name: "k3s-master.internal.example".to_string(),
      hostname_label: "ret2shell-node-master.internal.example".to_string(),
      is_control_plane: true,
    }];

    assert_eq!(
      resolve_local_node_hostname_label("ret2shell-node-master", &nodes)
        .expect("short hostname resolves"),
      "ret2shell-node-master.internal.example"
    );
  }

  #[test]
  fn resolve_local_node_hostname_label_falls_back_to_single_control_plane_node() {
    let nodes = vec![
      ClusterNodeIdentity {
        name: "worker-a".to_string(),
        hostname_label: "worker-a".to_string(),
        is_control_plane: false,
      },
      ClusterNodeIdentity {
        name: "cp-a".to_string(),
        hostname_label: "cp-label".to_string(),
        is_control_plane: true,
      },
    ];

    assert_eq!(
      resolve_local_node_hostname_label("unknown-host", &nodes)
        .expect("single control-plane node resolves"),
      "cp-label"
    );
  }

  #[test]
  fn merge_runtime_config_with_reference_overrides_selected_runtime_fields() {
    let generated = r#"
[cluster]
try_default = true
auto_infer = false
kube_config_path = ""
node_selector = ""
capture_directory = "/var/lib/ret2shell/captures"

[email]
enabled = false

[logging]
directory = "/var/lib/ret2shell/log"
level = "info"

[server]
external_domain = "generated.example"
external_https = false
cors_origins = "*"

[server.rate_limit]
burst_limit = 32
burst_restore_rate = 500

[media]
anti_theft = true
limit = 100
"#;
    let reference = r#"
[cluster]
auto_infer = true
kube_config_path = "/etc/rancher/k3s.yaml"
node_selector = "challenge"
capture_directory = "/var/cache/ret2shell/captures"

[email]
enabled = true
host = "smtp.example.com"

[logging]
directory = "/var/lib/ret2shell/logs"
level = "info,tower_http=off,sqlx=warn"

[server]
external_domain = "ctf.example.com"
external_https = true
cors_origins = "https://ctf.example.com"

[server.rate_limit]
burst_limit = 128
burst_restore_rate = 500

[media]
anti_theft = false
limit = 5
"#;

    let merged =
      merge_runtime_config_with_reference(generated, Some(reference)).expect("merge succeeds");
    let parsed: TomlValue = toml::from_str(&merged).expect("merged config parses");

    assert_eq!(parsed["cluster"]["auto_infer"], TomlValue::Boolean(false));
    assert_eq!(
      parsed["cluster"]["kube_config_path"],
      TomlValue::String(String::new())
    );
    assert_eq!(
      parsed["cluster"]["node_selector"],
      TomlValue::String("challenge".to_string())
    );
    assert_eq!(
      parsed["cluster"]["capture_directory"],
      TomlValue::String("/var/cache/ret2shell/captures".to_string())
    );
    assert_eq!(
      parsed["logging"]["directory"],
      TomlValue::String("/var/lib/ret2shell/log".to_string())
    );
    assert_eq!(
      parsed["server"]["external_https"],
      TomlValue::Boolean(false)
    );
    assert_eq!(
      parsed["server"]["cors_origins"],
      TomlValue::String("*".to_string())
    );
    assert_eq!(parsed["email"]["enabled"], TomlValue::Boolean(true));
    assert_eq!(parsed["media"]["anti_theft"], TomlValue::Boolean(false));
    assert_eq!(parsed["media"]["limit"], TomlValue::Integer(5));
  }

  #[test]
  fn render_runtime_registry_config_marks_registry_as_insecure_http() {
    let rendered = render_container_registry_config(true, Some("192.168.23.132:30310"))
      .expect("registry config should render");

    assert!(rendered.contains("'192.168.23.132:30310'"));
    assert!(rendered.contains("'http://192.168.23.132:30310'"));
    assert!(rendered.contains("insecure_skip_verify: true"));
    assert!(rendered.contains("'https://docker.1ms.run'"));
  }

  fn sample_summary() -> PlatformPlanSummary {
    PlatformPlanSummary {
      remaining_disk_gib: 900,
      requested_disk_gib: 730,
      allocated_local_disk_gib: 730,
      unallocated_local_disk_gib: 0,
      application_exposure: ApplicationExposureMode::Ingress,
      public_host: "ctf.example.com".to_string(),
      ingress_host: "ctf.example.com".to_string(),
      ingress_class_name: "nginx".to_string(),
      tls_enabled: true,
      tls_secret_name: Some("ret2shell-tls".to_string()),
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
