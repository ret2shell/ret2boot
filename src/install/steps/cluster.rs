use std::{fs, path::PathBuf};

use anyhow::{Result, anyhow, bail};
use rust_i18n::t;
use tracing::info;

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  support::{
    find_existing_path, install_staged_file, render_container_registry_config, run_staged_script,
    stage_remote_script, stage_text_file, unique_temp_path, yaml_quote,
  },
};
use crate::{
  config::{
    ApplicationExposureMode, InstallStepId, InstallTargetRole, KubernetesDistribution,
    KubernetesInstallSource,
  },
  install::collectors::{Collector, InputCollector, SingleSelectCollector},
  ui,
};

pub const CLUSTER_CIDR: &str = "10.42.0.0/16";
pub const NODE_CIDR_MASK_SIZE: u8 = 20;
pub const NODE_MAX_PODS: u16 = 3072;
const K3S_CONFIG_DEST: &str = "/etc/rancher/k3s/config.yaml";
const K3S_KUBELET_CONFIG_DEST: &str = "/etc/rancher/k3s/kubelet.config";
const K3S_REGISTRIES_DEST: &str = "/etc/rancher/k3s/registries.yaml";
const RKE2_CONFIG_DEST: &str = "/etc/rancher/rke2/config.yaml";
const RKE2_KUBELET_CONFIG_DEST: &str = "/etc/rancher/rke2/kubelet.conf";
const RKE2_REGISTRIES_DEST: &str = "/etc/rancher/rke2/registries.yaml";
const K3S_MANIFEST_DIR: &str = "/var/lib/rancher/k3s/server/manifests";
const RKE2_MANIFEST_DIR: &str = "/var/lib/rancher/rke2/server/manifests";
const K3S_TRAEFIK_CONFIG_DEST: &str =
  "/var/lib/rancher/k3s/server/manifests/ret2boot-traefik-config.yaml";
const RKE2_TRAEFIK_CONFIG_DEST: &str =
  "/var/lib/rancher/rke2/server/manifests/ret2boot-traefik-config.yaml";
const RKE2_INGRESS_NGINX_CONFIG_DEST: &str =
  "/var/lib/rancher/rke2/server/manifests/ret2boot-ingress-nginx-config.yaml";
const GATEWAY_HTTP_PORT_CANDIDATES: [u16; 6] = [10080, 11080, 12080, 13080, 14080, 15080];
const GATEWAY_HTTPS_PORT_CANDIDATES: [u16; 6] = [10443, 11443, 12443, 13443, 14443, 15443];

pub struct ClusterBootstrapStep;

impl ClusterBootstrapStep {
  pub(crate) fn reconcile_existing(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let spec = ClusterInstallSpec::from_plan_context(&ctx.as_plan_context())?;

    match spec.distribution {
      KubernetesDistribution::K3s => install_k3s(ctx, &spec),
      KubernetesDistribution::Rke2 => install_rke2(ctx, &spec),
    }
  }
}

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

    let disable_traefik = distribution == KubernetesDistribution::K3s;
    ctx.persist_change(
      "install.questionnaire.kubernetes.bootstrap.disable_traefik",
      if disable_traefik { "true" } else { "false" },
      |config| config.set_install_kubernetes_disable_traefik(disable_traefik),
    )?;

    let enable_china_registry_mirrors = source == KubernetesInstallSource::ChinaMirror;
    ctx.persist_change(
      "install.questionnaire.kubernetes.mirrors.enable_china_registry_mirrors",
      if enable_china_registry_mirrors {
        "true"
      } else {
        "false"
      },
      |config| {
        config.set_install_kubernetes_enable_china_registry_mirrors(enable_china_registry_mirrors)
      },
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
      let server_url = server_prompt.collect()?.trim().to_string();

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
      let token = token_prompt.collect()?.trim().to_string();

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

    if spec.distribution == KubernetesDistribution::K3s && spec.disable_traefik {
      details.push("k3s builtin traefik will be disabled during cluster bootstrap".to_string());
    }

    if ctx
      .kubernetes_enable_china_registry_mirrors()
      .unwrap_or(false)
    {
      details.push("container registry mirrors will be configured for docker.io, ghcr.io, gcr.io, quay.io, and registry.k8s.io".to_string());
    }

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

pub(crate) struct ClusterInstallSpec {
  pub(crate) role: InstallTargetRole,
  pub(crate) distribution: KubernetesDistribution,
  pub(crate) source: KubernetesInstallSource,
  pub(crate) disable_traefik: bool,
  pub(crate) application_exposure: Option<ApplicationExposureMode>,
  pub(crate) worker_server_url: Option<String>,
  pub(crate) worker_token: Option<String>,
}

impl ClusterInstallSpec {
  pub(crate) fn from_plan_context(ctx: &StepPlanContext<'_>) -> Result<Self> {
    let role = ctx
      .node_role()
      .ok_or_else(|| anyhow!("node role is required before planning cluster installation"))?;
    let distribution = ctx.kubernetes_distribution().ok_or_else(|| {
      anyhow!("kubernetes distribution is required before planning cluster installation")
    })?;
    let source = ctx.kubernetes_source().ok_or_else(|| {
      anyhow!("kubernetes source is required before planning cluster installation")
    })?;
    let disable_traefik = ctx
      .kubernetes_disable_traefik()
      .unwrap_or(distribution == KubernetesDistribution::K3s);
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
      disable_traefik,
      application_exposure,
      worker_server_url,
      worker_token,
    })
  }
}

pub(crate) fn install_k3s(
  ctx: &mut StepExecutionContext<'_>, spec: &ClusterInstallSpec,
) -> Result<()> {
  sync_cluster_registry_config(
    ctx,
    K3S_REGISTRIES_DEST,
    ctx
      .as_plan_context()
      .kubernetes_enable_china_registry_mirrors()
      .unwrap_or(false),
  )?;

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

  let result = run_staged_script(ctx, &script_path, &envs);
  let _ = fs::remove_file(&script_path);
  staged.cleanup();
  result
}

pub(crate) fn uninstall_k3s(
  ctx: &StepExecutionContext<'_>, spec: &ClusterInstallSpec,
) -> Result<()> {
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

pub(crate) fn install_rke2(
  ctx: &mut StepExecutionContext<'_>, spec: &ClusterInstallSpec,
) -> Result<()> {
  sync_cluster_registry_config(
    ctx,
    RKE2_REGISTRIES_DEST,
    ctx
      .as_plan_context()
      .kubernetes_enable_china_registry_mirrors()
      .unwrap_or(false),
  )?;

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

  let install_result = run_staged_script(ctx, &script_path, &envs);
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

pub(crate) fn uninstall_rke2(ctx: &StepExecutionContext<'_>) -> Result<()> {
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

pub(crate) fn rollback_k3s(
  ctx: &StepExecutionContext<'_>, spec: &ClusterInstallSpec,
) -> Result<()> {
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

pub(crate) fn rollback_rke2(ctx: &StepExecutionContext<'_>) -> Result<()> {
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

fn choose_available_gateway_ports() -> Result<(u16, u16)> {
  let listening = super::support::listening_tcp_ports();

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

fn stage_k3s_config(spec: &ClusterInstallSpec) -> Result<StagedClusterConfig> {
  let config = unique_temp_path("k3s-config", "yaml");
  let kubelet_config = unique_temp_path("k3s-kubelet", "yaml");

  fs::write(&config, render_k3s_config(spec))?;
  fs::write(&kubelet_config, render_kubelet_config())?;

  Ok(StagedClusterConfig {
    config,
    kubelet_config,
  })
}

fn stage_rke2_config(spec: &ClusterInstallSpec) -> Result<StagedClusterConfig> {
  let config = unique_temp_path("rke2-config", "yaml");
  let kubelet_config = unique_temp_path("rke2-kubelet", "yaml");

  fs::write(&config, render_rke2_config(spec))?;
  fs::write(&kubelet_config, render_kubelet_config())?;

  Ok(StagedClusterConfig {
    config,
    kubelet_config,
  })
}

fn cleanup_k3s_configs(ctx: &StepExecutionContext<'_>) -> Result<()> {
  ctx.run_privileged_command(
    "rm",
    &[
      "-f".to_string(),
      K3S_CONFIG_DEST.to_string(),
      K3S_KUBELET_CONFIG_DEST.to_string(),
      K3S_REGISTRIES_DEST.to_string(),
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
      RKE2_REGISTRIES_DEST.to_string(),
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

fn render_k3s_config(spec: &ClusterInstallSpec) -> String {
  let mut lines = Vec::new();

  if spec.role == InstallTargetRole::ControlPlane {
    lines.push(format!("cluster-cidr: {CLUSTER_CIDR}"));
    if spec.disable_traefik {
      lines.push("disable:".to_string());
      lines.push("  - traefik".to_string());
    }
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

fn sync_cluster_registry_config(
  ctx: &StepExecutionContext<'_>, destination: &str, enable_china_registry_mirrors: bool,
) -> Result<()> {
  let Some(contents) = render_container_registry_config(enable_china_registry_mirrors, None) else {
    return Ok(());
  };

  let staged = stage_text_file("cluster-registries", "yaml", contents)?;
  let install_result = install_staged_file(ctx, &staged, destination);
  let _ = fs::remove_file(&staged);
  install_result
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
