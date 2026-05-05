use std::{fs, path::Path, thread, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use rust_i18n::t;
use serde::Deserialize;
use tracing::info;

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  SystemPackageManager,
  platform::{PLATFORM_NODE_PORT, resolve_public_endpoint},
  support::{
    command_exists, detect_nginx_binary_path, ensure_symlink, find_existing_path,
    install_directory, install_staged_file, managed_tls_asset_name, managed_tls_certificate_path,
    managed_tls_directory, managed_tls_key_path, nginx_service_exists, stage_text_file,
  },
};
use crate::{
  config::{
    ApplicationExposureMode, InstallStepId, InstallTargetRole, KubernetesDistribution,
    PlatformTlsMode,
  },
  install::collectors::{Collector, InputCollector, SingleSelectCollector},
  ui,
};

const NGINX_MAIN_CONF: &str = "/etc/nginx/nginx.conf";
const NGINX_LOG_DIR: &str = "/var/log/nginx/ret2shell";
const NGINX_SITE_AVAILABLE: &str = "/etc/nginx/sites-available/ret2shell.conf";
const NGINX_SITE_ENABLED: &str = "/etc/nginx/sites-enabled/ret2shell.conf";
const NGINX_STREAM_AVAILABLE: &str = "/etc/nginx/ret2boot-stream-available/ret2shell.conf";
const NGINX_STREAM_ENABLED: &str = "/etc/nginx/ret2boot-stream-enabled/ret2shell.conf";
const NGINX_STREAM_MODULE_RET2BOOT_CONF: &str = "/etc/nginx/modules-enabled/ret2boot-stream.conf";
const NGINX_SITE_INCLUDE_MARKER: &str = "include /etc/nginx/sites-enabled/*.conf;";
const NGINX_SITE_INCLUDE_MARKER_DEFAULT: &str = "include /etc/nginx/sites-enabled/*;";
const NGINX_STREAM_INCLUDE_MARKER: &str = "include /etc/nginx/ret2boot-stream-enabled/*.conf;";
const INGRESS_RELEASE_NAME: &str = "ingress-nginx";
const INGRESS_NAMESPACE: &str = "ingress-nginx";
const KUBERNETES_API_PORT: u16 = 6443;
const KUBELET_SECURE_PORT: u16 = 10250;
const INGRESS_GATEWAY_HTTP_PORT: u16 = 10080;
const INGRESS_GATEWAY_HTTPS_PORT: u16 = 10443;
const INTERNAL_REGISTRY_NODE_PORT: u16 = 30310;
const CLUSTER_READY_WAIT_TIMEOUT: &str = "10s";
const CLUSTER_READY_WAIT_RETRIES: usize = 120;
const CLUSTER_READY_RETRY_INTERVAL_SECONDS: u64 = 5;
pub struct ApplicationGatewayStep;

struct ManagedTlsMaterial {
  certificate_path: String,
  key_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum NodePortGuardTarget {
  Interface(String),
}

#[derive(Debug, Deserialize)]
struct ClusterPodList {
  items: Vec<ClusterPodItem>,
}

#[derive(Debug, Deserialize)]
struct ClusterPodItem {
  metadata: ClusterPodMetadata,
  status: ClusterPodStatus,
}

#[derive(Debug, Deserialize)]
struct ClusterPodMetadata {
  #[serde(default)]
  namespace: String,
  name: String,
}

#[derive(Debug, Deserialize)]
struct ClusterPodStatus {
  phase: Option<String>,
  #[serde(default)]
  conditions: Vec<ClusterPodCondition>,
}

#[derive(Debug, Deserialize)]
struct ClusterPodCondition {
  #[serde(rename = "type")]
  condition_type: String,
  status: String,
}

struct GatewayClusterAccess {
  program: String,
  prefix_args: Vec<String>,
  envs: Vec<(String, String)>,
}

impl AtomicInstallStep for ApplicationGatewayStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::ApplicationGateway
  }

  fn should_include(&self, ctx: &StepPlanContext<'_>) -> bool {
    ctx.node_role() == Some(InstallTargetRole::ControlPlane)
  }

  fn collect(&self, ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    let exposure_options = [
      ApplicationExposureMode::NodePortExternalNginx,
      ApplicationExposureMode::Ingress,
    ];
    let default = match ctx
      .config()
      .install
      .questionnaire
      .kubernetes
      .application_exposure
    {
      Some(ApplicationExposureMode::NodePortExternalNginx) => 0,
      Some(ApplicationExposureMode::Ingress) => 1,
      None => 0,
    };
    let options = exposure_options
      .iter()
      .copied()
      .map(application_exposure_label)
      .collect::<Vec<_>>();
    let exposure =
      exposure_options[SingleSelectCollector::new(t!("install.exposure.entry_prompt"), options)
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
        details.push(t!("install.steps.gateway.ingress_host_network").to_string());
        if ctx.platform_nodeport_guard_enabled().unwrap_or(false) {
          details.push(t!("install.steps.gateway.nodeport_guard").to_string());
        }
        if ctx.platform_tls_mode().unwrap_or(PlatformTlsMode::Disabled) != PlatformTlsMode::Disabled
        {
          details.push(t!("install.steps.gateway.ingress_tls_prepare").to_string());
        }
      }
      ApplicationExposureMode::NodePortExternalNginx => {
        details.push(t!("install.steps.gateway.nodeport_detail").to_string());
        details.push(t!("install.steps.gateway.port_reservation").to_string());
        if ctx.platform_nodeport_guard_enabled().unwrap_or(false) {
          details.push(t!("install.steps.gateway.nodeport_guard").to_string());
        }
        if ctx.platform_tls_mode().unwrap_or(PlatformTlsMode::Disabled) != PlatformTlsMode::Disabled
        {
          details.push(t!("install.steps.gateway.nodeport_tls_termination").to_string());
        }

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

    let tls_material = ensure_managed_tls_assets(ctx)?;

    if exposure == ApplicationExposureMode::Ingress {
      info!("installing or updating ingress-nginx gateway");
      let installed_by_ret2boot = install_ingress_nginx(ctx)?;
      ensure_gateway_guard_rules(ctx)?;
      ctx.persist_change(
        "install.execution.gateway.ingress",
        INGRESS_RELEASE_NAME,
        |config| {
          let changed =
            config.set_install_step_metadata(self.id(), "ingress_release", INGRESS_RELEASE_NAME);
          config.set_install_step_metadata(
            self.id(),
            "ingress_installed_by_ret2boot",
            if installed_by_ret2boot {
              "true"
            } else {
              "false"
            },
          ) || changed
        },
      )?;
      return Ok(());
    }

    let package_manager = ctx
      .preflight_state()
      .package_manager()
      .or_else(SystemPackageManager::detect)
      .ok_or_else(|| anyhow!("no supported package manager is available for nginx installation"))?;
    let nginx_existed = detect_nginx_binary_path().is_some();
    let installed_by_ret2boot = ctx
      .config()
      .install_step_metadata(self.id(), "installed_by_ret2boot")
      .is_some_and(|value| value == "true");

    if !nginx_existed {
      info!(
        package_manager = package_manager.label(),
        "installing nginx for external gateway"
      );
      package_manager.install_nginx(ctx)?;
    }

    let public_host = ctx
      .config()
      .install
      .questionnaire
      .platform
      .public_host
      .as_deref()
      .ok_or_else(|| anyhow!("platform public host is required before installing gateway"))?;
    let endpoint = resolve_public_endpoint(public_host, exposure)?;
    let backend_host = "127.0.0.1".to_string();
    let backend_http_port = PLATFORM_NODE_PORT;

    info!("writing external nginx gateway configuration");
    install_external_nginx_gateway(
      ctx,
      backend_host.as_str(),
      backend_http_port,
      &endpoint.public_host,
      tls_material.as_ref(),
    )?;
    ensure_gateway_guard_rules(ctx)?;
    let nginx_binary = detect_nginx_binary_path();
    let nginx_command = nginx_binary
      .as_ref()
      .map(|path| path.display().to_string())
      .unwrap_or_else(|| "nginx".to_string());
    info!("enabling and validating nginx service");
    ctx.run_privileged_command(
      "systemctl",
      &[
        "enable".to_string(),
        "--now".to_string(),
        "nginx.service".to_string(),
      ],
      &[],
    )?;
    ctx.run_privileged_command(&nginx_command, &["-t".to_string()], &[])?;
    ctx.run_privileged_command(
      "systemctl",
      &["reload".to_string(), "nginx.service".to_string()],
      &[],
    )?;

    let package_manager_label = package_manager.label().to_string();
    let binary_path = nginx_binary
      .map(|path| path.display().to_string())
      .unwrap_or_else(|| nginx_command.clone());

    ctx.persist_change("install.execution.gateway.nginx", &binary_path, |config| {
      let changed = config.set_install_step_metadata(
        self.id(),
        "package_manager",
        package_manager_label.clone(),
      );
      let changed =
        config.set_install_step_metadata(self.id(), "binary_path", binary_path.clone()) || changed;
      let changed =
        config.set_install_step_metadata(self.id(), "upstream_host", backend_host.clone())
          || changed;
      config.set_install_step_metadata(
        self.id(),
        "installed_by_ret2boot",
        if installed_by_ret2boot || !nginx_existed {
          "true"
        } else {
          "false"
        },
      ) || changed
    })?;

    Ok(())
  }

  fn uninstall(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let mode = ctx
      .config()
      .install_step_metadata(self.id(), "mode")
      .unwrap_or(ApplicationExposureMode::Ingress.as_config_value());

    if mode == ApplicationExposureMode::Ingress.as_config_value() {
      let _ = remove_gateway_guard_rules(ctx);
      if ctx
        .config()
        .install_step_metadata(self.id(), "ingress_installed_by_ret2boot")
        .is_some_and(|value| value == "true")
      {
        let _ = uninstall_ingress_nginx(ctx);
      }

      ctx.persist_change("install.execution.gateway.cleanup", "done", |config| {
        let changed = config.remove_install_step_metadata(self.id(), "ingress_release");
        config.remove_install_step_metadata(self.id(), "ingress_installed_by_ret2boot") || changed
      })?;
      return Ok(());
    }

    if mode != ApplicationExposureMode::NodePortExternalNginx.as_config_value() {
      return Ok(());
    }

    cleanup_external_nginx_gateway(ctx)?;
    let _ = remove_gateway_guard_rules(ctx);

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
        .and_then(SystemPackageManager::from_label)
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
      let changed = config.remove_install_step_metadata(self.id(), "upstream_host") || changed;
      let changed = config.remove_install_step_metadata(self.id(), "ingress_release") || changed;
      let changed =
        config.remove_install_step_metadata(self.id(), "ingress_installed_by_ret2boot") || changed;
      config.remove_install_step_metadata(self.id(), "installed_by_ret2boot") || changed
    })?;

    Ok(())
  }
}

fn install_external_nginx_gateway(
  ctx: &StepExecutionContext<'_>, backend_host: &str, http_port: u16, server_name: &str,
  tls_material: Option<&ManagedTlsMaterial>,
) -> Result<()> {
  install_directory(ctx, "/etc/nginx/sites-available")?;
  install_directory(ctx, "/etc/nginx/sites-enabled")?;
  install_directory(ctx, NGINX_LOG_DIR)?;

  ensure_nginx_site_include(ctx)?;

  let site_path = stage_text_file(
    "nginx-ret2boot-site",
    "conf",
    render_nginx_site(backend_host, http_port, server_name, tls_material)?,
  )?;
  install_staged_file(ctx, &site_path, NGINX_SITE_AVAILABLE)?;
  let _ = fs::remove_file(&site_path);
  ensure_symlink(ctx, NGINX_SITE_AVAILABLE, NGINX_SITE_ENABLED)?;

  let _ = ctx.run_privileged_command(
    "rm",
    &[
      "-f".to_string(),
      NGINX_STREAM_ENABLED.to_string(),
      NGINX_STREAM_AVAILABLE.to_string(),
      NGINX_STREAM_MODULE_RET2BOOT_CONF.to_string(),
    ],
    &[],
  );
  let _ = remove_nginx_stream_include(ctx);
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

fn install_ingress_nginx(ctx: &StepExecutionContext<'_>) -> Result<bool> {
  let release_existed = helm_release_exists(ctx, INGRESS_NAMESPACE, INGRESS_RELEASE_NAME)?;

  info!("adding ingress-nginx helm repository");
  ctx.run_privileged_command(
    "helm",
    &[
      "repo".to_string(),
      "add".to_string(),
      INGRESS_RELEASE_NAME.to_string(),
      "https://kubernetes.github.io/ingress-nginx".to_string(),
      "--force-update".to_string(),
    ],
    &[],
  )?;
  info!("updating helm repositories before ingress-nginx install");
  ctx.run_privileged_command("helm", &["repo".to_string(), "update".to_string()], &[])?;
  info!(
    release = INGRESS_RELEASE_NAME,
    namespace = INGRESS_NAMESPACE,
    "running helm upgrade for ingress-nginx"
  );
  ctx.run_privileged_command(
    "helm",
    &[
      "upgrade".to_string(),
      "--install".to_string(),
      INGRESS_RELEASE_NAME.to_string(),
      format!("{INGRESS_RELEASE_NAME}/{INGRESS_RELEASE_NAME}"),
      "-n".to_string(),
      INGRESS_NAMESPACE.to_string(),
      "--create-namespace".to_string(),
      "--wait".to_string(),
      "--timeout".to_string(),
      "15m0s".to_string(),
      "--set".to_string(),
      "controller.kind=DaemonSet".to_string(),
      "--set".to_string(),
      "controller.hostNetwork=true".to_string(),
      "--set".to_string(),
      "controller.service.type=ClusterIP".to_string(),
      "--set".to_string(),
      "controller.ingressClass=nginx".to_string(),
      "--set".to_string(),
      "controller.ingressClassResource.name=nginx".to_string(),
    ],
    &[],
  )?;

  Ok(!release_existed)
}

fn uninstall_ingress_nginx(ctx: &StepExecutionContext<'_>) -> Result<()> {
  ctx.run_privileged_command(
    "helm",
    &[
      "uninstall".to_string(),
      INGRESS_RELEASE_NAME.to_string(),
      "-n".to_string(),
      INGRESS_NAMESPACE.to_string(),
      "--ignore-not-found".to_string(),
    ],
    &[],
  )
}

fn helm_release_exists(
  ctx: &StepExecutionContext<'_>, namespace: &str, release_name: &str,
) -> Result<bool> {
  match ctx.run_privileged_command(
    "helm",
    &[
      "status".to_string(),
      release_name.to_string(),
      "-n".to_string(),
      namespace.to_string(),
    ],
    &[],
  ) {
    Ok(()) => Ok(true),
    Err(error) if error.to_string().contains("release: not found") => Ok(false),
    Err(error) => Err(error),
  }
}

fn ensure_managed_tls_assets(ctx: &StepExecutionContext<'_>) -> Result<Option<ManagedTlsMaterial>> {
  let plan_context = ctx.as_plan_context();
  let tls_mode = plan_context
    .platform_tls_mode()
    .unwrap_or(PlatformTlsMode::Disabled);
  if tls_mode == PlatformTlsMode::Disabled {
    return Ok(None);
  }

  let exposure = plan_context
    .application_exposure()
    .ok_or_else(|| anyhow!("application exposure mode is required before preparing TLS assets"))?;
  let asset_name = managed_tls_asset_name(exposure, plan_context.platform_tls_secret_name())?;
  let certificate_path = managed_tls_certificate_path(&asset_name);
  let key_path = managed_tls_key_path(&asset_name);
  let tls_directory = managed_tls_directory(&asset_name);
  install_directory(ctx, "/etc/ret2shell")?;
  install_directory(ctx, "/etc/ret2shell/tls")?;
  install_directory(ctx, &tls_directory)?;

  match tls_mode {
    PlatformTlsMode::Disabled => {}
    PlatformTlsMode::AcmeDns | PlatformTlsMode::ProvidedFiles => {
      let source_certificate_path =
        plan_context
          .platform_tls_certificate_path()
          .ok_or_else(|| {
            anyhow!("a TLS certificate path is required when TLS mode is provided-files")
          })?;
      let source_key_path = plan_context
        .platform_tls_key_path()
        .ok_or_else(|| anyhow!("a TLS key path is required when TLS mode is provided-files"))?;

      ensure_host_file_exists(ctx, source_certificate_path, "TLS certificate")?;
      ensure_host_file_exists(ctx, source_key_path, "TLS private key")?;

      ctx.run_privileged_command(
        "install",
        &[
          "-m".to_string(),
          "600".to_string(),
          source_certificate_path.to_string(),
          certificate_path.clone(),
        ],
        &[],
      )?;
      ctx.run_privileged_command(
        "install",
        &[
          "-m".to_string(),
          "600".to_string(),
          source_key_path.to_string(),
          key_path.clone(),
        ],
        &[],
      )?;
    }
  }

  Ok(Some(ManagedTlsMaterial {
    certificate_path,
    key_path,
  }))
}

fn ensure_host_file_exists(ctx: &StepExecutionContext<'_>, path: &str, label: &str) -> Result<()> {
  if ctx
    .run_privileged_command("test", &["-f".to_string(), path.to_string()], &[])
    .is_ok()
  {
    return Ok(());
  }

  bail!("{label} file `{path}` is missing on the target host or is not a regular file")
}

fn ensure_gateway_guard_rules(ctx: &mut StepExecutionContext<'_>) -> Result<()> {
  let guard_enabled = ctx
    .as_plan_context()
    .platform_nodeport_guard_enabled()
    .unwrap_or(false);
  if !guard_enabled {
    info!("gateway guard is disabled; skipping iptables raw rules");
    return Ok(());
  }

  let exposure = ctx
    .as_plan_context()
    .application_exposure()
    .ok_or_else(|| {
      anyhow!("application exposure mode is required before building gateway guard rules")
    })?;
  let protected_ports = protected_gateway_ports(exposure);
  let cluster_target = resolve_nodeport_guard_target_for_install(ctx)?;
  info!(
    protected_ports,
    target = ?cluster_target,
    "installing gateway guard iptables raw rules"
  );
  purge_gateway_guard_rules(ctx, &cluster_target, &protected_ports);
  if let Some(legacy_ports) = legacy_protected_gateway_ports(exposure) {
    purge_gateway_guard_rules(ctx, &cluster_target, &legacy_ports);
  }
  insert_gateway_guard_rules(ctx, &cluster_target, &protected_ports)?;

  persist_iptables_rules(ctx)
}

fn remove_gateway_guard_rules(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let exposure = match ctx.as_plan_context().application_exposure() {
    Some(exposure) => exposure,
    None => return Ok(()),
  };
  let protected_ports = protected_gateway_ports(exposure);
  let cluster_target = match resolve_nodeport_guard_target_for_cleanup(ctx) {
    Ok(target) => target,
    Err(_) => return Ok(()),
  };
  purge_gateway_guard_rules(ctx, &cluster_target, &protected_ports);
  if let Some(legacy_ports) = legacy_protected_gateway_ports(exposure) {
    purge_gateway_guard_rules(ctx, &cluster_target, &legacy_ports);
  }

  let _ = persist_iptables_rules(ctx);
  Ok(())
}

fn purge_gateway_guard_rules(
  ctx: &StepExecutionContext<'_>, cluster_target: &NodePortGuardTarget, protected_ports: &str,
) {
  for args in gateway_guard_rules("-D", cluster_target, protected_ports) {
    while ctx.run_privileged_command("iptables", &args, &[]).is_ok() {}
  }
}

fn insert_gateway_guard_rules(
  ctx: &StepExecutionContext<'_>, cluster_target: &NodePortGuardTarget, protected_ports: &str,
) -> Result<()> {
  for args in gateway_guard_rules("-I", cluster_target, protected_ports)
    .into_iter()
    .rev()
  {
    let mut insert_args = args;
    insert_args.insert(4, "1".to_string());
    ctx.run_privileged_command("iptables", &insert_args, &[])?;
  }

  Ok(())
}

fn persist_iptables_rules(ctx: &StepExecutionContext<'_>) -> Result<()> {
  if command_exists("netfilter-persistent") {
    info!("saving iptables rules with netfilter-persistent");
    return ctx.run_privileged_command("netfilter-persistent", &["save".to_string()], &[]);
  }

  let package_manager = ctx
    .preflight_state()
    .package_manager()
    .or_else(SystemPackageManager::detect);
  if matches!(package_manager, Some(SystemPackageManager::Apt)) {
    info!("installing iptables-persistent so gateway guard rules survive reboot");
    ctx.run_privileged_command(
      "apt-get",
      &[
        "-o".to_string(),
        "DPkg::Lock::Timeout=300".to_string(),
        "install".to_string(),
        "-y".to_string(),
        "iptables-persistent".to_string(),
      ],
      &[("DEBIAN_FRONTEND".to_string(), "noninteractive".to_string())],
    )?;
    return ctx.run_privileged_command("netfilter-persistent", &["save".to_string()], &[]);
  }

  Ok(())
}

fn cleanup_external_nginx_gateway(ctx: &StepExecutionContext<'_>) -> Result<()> {
  ctx.run_privileged_command(
    "rm",
    &[
      "-f".to_string(),
      NGINX_SITE_ENABLED.to_string(),
      NGINX_SITE_AVAILABLE.to_string(),
      NGINX_STREAM_ENABLED.to_string(),
      NGINX_STREAM_AVAILABLE.to_string(),
      NGINX_STREAM_MODULE_RET2BOOT_CONF.to_string(),
    ],
    &[],
  )?;
  let _ = remove_nginx_site_include(ctx);
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

fn render_nginx_site(
  backend_host: &str, http_port: u16, server_name: &str, tls_material: Option<&ManagedTlsMaterial>,
) -> Result<String> {
  let location_block =
    "    location /assets/ {\n        expires 1y;\n        add_header Cache-Control \"public, immutable\";\n    }\n\n    location / {\n        client_max_body_size 1024M;\n        proxy_pass http://backend;\n        proxy_set_header Host $host;\n        proxy_http_version 1.1;\n        proxy_set_header Upgrade $http_upgrade;\n        proxy_set_header Connection \"Upgrade\";\n        proxy_set_header Range $http_range;\n        proxy_set_header If-Range $http_if_range;\n        proxy_set_header X-Real-IP $remote_addr;\n        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n        proxy_set_header X-Forwarded-Host $host;\n        proxy_set_header X-Forwarded-Proto $scheme;\n        proxy_set_header X-Forwarded-Port $server_port;\n        proxy_set_header X-Forwarded-Server $host;\n        proxy_set_header Origin $http_origin;\n        proxy_set_header Referer $http_referer;\n        proxy_redirect off;\n    }\n\n    location ~ ^/v2(/.*)?$ {\n        rewrite ^/v2(.*)$ /api/cluster/registry/v2$1 break;\n        proxy_pass http://backend;\n        proxy_set_header Host $host;\n        proxy_set_header Range $http_range;\n        proxy_set_header If-Range $http_if_range;\n        proxy_set_header X-Real-IP $remote_addr;\n        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n        proxy_set_header X-Forwarded-Host $host;\n        proxy_set_header X-Forwarded-Proto $scheme;\n        proxy_set_header X-Forwarded-Port $server_port;\n        proxy_set_header X-Forwarded-Server $host;\n        proxy_set_header Origin $http_origin;\n        proxy_set_header Referer $http_referer;\n        proxy_redirect off;\n    }\n"
      .to_string();

  let mut lines = vec![
    "upstream backend {".to_string(),
    format!("    server {backend_host}:{http_port};"),
    "}".to_string(),
    String::new(),
  ];

  if let Some(tls_material) = tls_material {
    lines.extend([
      "server {".to_string(),
      "    listen 80;".to_string(),
      "    listen [::]:80;".to_string(),
      format!("    server_name {server_name};"),
      "    return 301 https://$host$request_uri;".to_string(),
      "}".to_string(),
      String::new(),
      "server {".to_string(),
      "    listen 443 ssl http2;".to_string(),
      "    listen [::]:443 ssl http2;".to_string(),
      format!("    server_name {server_name};"),
      format!("    ssl_certificate {};", tls_material.certificate_path),
      format!("    ssl_certificate_key {};", tls_material.key_path),
      "    ssl_protocols TLSv1.2 TLSv1.3;".to_string(),
      "    ssl_ciphers HIGH:!aNULL:!MD5;".to_string(),
      "    ssl_prefer_server_ciphers on;".to_string(),
      "    gzip_static on;".to_string(),
      format!("    access_log {NGINX_LOG_DIR}/access.log;"),
      format!("    error_log {NGINX_LOG_DIR}/error.log;"),
      "    proxy_set_header Host $host;".to_string(),
      "    client_max_body_size 256m;".to_string(),
      "    root /srv/ret2shell/frontend;".to_string(),
      location_block.trim_end().to_string(),
      "}".to_string(),
    ]);
  } else {
    lines.extend([
      "server {".to_string(),
      "    listen 80;".to_string(),
      "    listen [::]:80;".to_string(),
      format!("    server_name {server_name};"),
      "    gzip_static on;".to_string(),
      format!("    access_log {NGINX_LOG_DIR}/access.log;"),
      format!("    error_log {NGINX_LOG_DIR}/error.log;"),
      "    proxy_set_header Host $host;".to_string(),
      "    client_max_body_size 256m;".to_string(),
      "    root /srv/ret2shell/frontend;".to_string(),
      location_block.trim_end().to_string(),
      "}".to_string(),
    ]);
  }

  lines.push(String::new());
  Ok(lines.join("\n"))
}

fn ensure_nginx_site_include(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let contents = fs::read_to_string(NGINX_MAIN_CONF)
    .with_context(|| format!("failed to read `{NGINX_MAIN_CONF}`"))?;

  if contents.contains(NGINX_SITE_INCLUDE_MARKER_DEFAULT)
    && contents.contains(NGINX_SITE_INCLUDE_MARKER)
  {
    let updated = remove_custom_site_include_line(&contents);
    let staged = stage_text_file("nginx-main", "conf", updated)?;
    install_staged_file(ctx, &staged, NGINX_MAIN_CONF)?;
    let _ = fs::remove_file(&staged);
    return Ok(());
  }

  if contents.contains(NGINX_SITE_INCLUDE_MARKER)
    || contents.contains(NGINX_SITE_INCLUDE_MARKER_DEFAULT)
  {
    return Ok(());
  }

  let http_index = contents
    .find("http {")
    .ok_or_else(|| anyhow!("unable to locate the http block in `{NGINX_MAIN_CONF}`"))?;
  let updated = format!(
    "{}{}\n    {}\n{}",
    &contents[..http_index],
    "http {",
    NGINX_SITE_INCLUDE_MARKER,
    &contents[http_index..]
      .strip_prefix("http {")
      .expect("http block exists")
  );
  let staged = stage_text_file("nginx-main", "conf", updated)?;
  install_staged_file(ctx, &staged, NGINX_MAIN_CONF)?;
  let _ = fs::remove_file(&staged);
  Ok(())
}

fn remove_nginx_site_include(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let contents = fs::read_to_string(NGINX_MAIN_CONF)
    .with_context(|| format!("failed to read `{NGINX_MAIN_CONF}`"))?;

  if !contents.contains(NGINX_SITE_INCLUDE_MARKER) {
    return Ok(());
  }

  let updated = remove_custom_site_include_line(&contents);
  let staged = stage_text_file("nginx-main", "conf", updated)?;
  install_staged_file(ctx, &staged, NGINX_MAIN_CONF)?;
  let _ = fs::remove_file(&staged);
  Ok(())
}

fn remove_custom_site_include_line(contents: &str) -> String {
  contents
    .replace(&format!("    {NGINX_SITE_INCLUDE_MARKER}\n"), "")
    .replace(&format!("{NGINX_SITE_INCLUDE_MARKER}\n"), "")
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

fn application_exposure_label(exposure: ApplicationExposureMode) -> String {
  match exposure {
    ApplicationExposureMode::Ingress => t!("install.exposure.entry_options.ingress").to_string(),
    ApplicationExposureMode::NodePortExternalNginx => {
      t!("install.exposure.entry_options.nodeport_external_nginx").to_string()
    }
  }
}

fn wait_for_cluster_network_ready(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let cluster_access = GatewayClusterAccess::from_execution_context(ctx)?;

  cluster_access.wait_for_nodes_ready(ctx)?;
  cluster_access.wait_for_kube_system_pods_ready(ctx)
}

impl GatewayClusterAccess {
  fn from_execution_context(ctx: &StepExecutionContext<'_>) -> Result<Self> {
    let plan_context = ctx.as_plan_context();
    let distribution = plan_context.kubernetes_distribution().ok_or_else(|| {
      anyhow!("kubernetes distribution is required before detecting the cluster bridge interface")
    })?;

    match distribution {
      KubernetesDistribution::K3s => {
        if !command_exists("k3s") {
          bail!("unable to locate the k3s binary required for cluster bridge detection");
        }

        Ok(Self {
          program: "k3s".to_string(),
          prefix_args: vec!["kubectl".to_string()],
          envs: Vec::new(),
        })
      }
      KubernetesDistribution::Rke2 => {
        let kubectl = find_existing_path(&[
          std::path::PathBuf::from("/var/lib/rancher/rke2/bin/kubectl"),
          std::path::PathBuf::from("/usr/local/bin/kubectl"),
        ])
        .ok_or_else(|| {
          anyhow!("unable to locate the rke2 kubectl binary for cluster bridge detection")
        })?;

        Ok(Self {
          program: kubectl.display().to_string(),
          prefix_args: Vec::new(),
          envs: vec![(
            "KUBECONFIG".to_string(),
            "/etc/rancher/rke2/rke2.yaml".to_string(),
          )],
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
      format!("--timeout={CLUSTER_READY_WAIT_TIMEOUT}"),
    ];
    let mut last_error = None;

    info!(
      attempts = CLUSTER_READY_WAIT_RETRIES,
      "waiting for kubernetes nodes before detecting cluster bridge interface"
    );

    for attempt in 1..=CLUSTER_READY_WAIT_RETRIES {
      match self.run(ctx, &args) {
        Ok(()) => {
          info!("kubernetes nodes are ready");
          return Ok(());
        }
        Err(error) => {
          info!(
            attempt,
            attempts = CLUSTER_READY_WAIT_RETRIES,
            error = %error,
            "kubernetes nodes are not ready yet"
          );
          last_error = Some(error);
        }
      }

      thread::sleep(Duration::from_secs(CLUSTER_READY_RETRY_INTERVAL_SECONDS));
    }

    Err(last_error.unwrap_or_else(|| {
      anyhow!("timed out waiting for kubernetes nodes before cluster bridge detection")
    }))
  }

  fn wait_for_kube_system_pods_ready(&self, ctx: &StepExecutionContext<'_>) -> Result<()> {
    let mut last_error = None;
    let mut last_pending = Vec::new();

    info!(
      attempts = CLUSTER_READY_WAIT_RETRIES,
      "waiting for kube-system pods before detecting cluster bridge interface"
    );

    for attempt in 1..=CLUSTER_READY_WAIT_RETRIES {
      match self.kube_system_pod_status(ctx) {
        Ok(status) if status.pending.is_empty() => {
          info!(pod_count = status.ready_count, "kube-system pods are ready");
          return Ok(());
        }
        Ok(status) => {
          last_pending = status.pending;
          info!(
            attempt,
            attempts = CLUSTER_READY_WAIT_RETRIES,
            pending = %last_pending.join(", "),
            "kube-system pods are not ready yet"
          );
          last_error = None;
        }
        Err(error) => {
          info!(
            attempt,
            attempts = CLUSTER_READY_WAIT_RETRIES,
            error = %error,
            "unable to read kube-system pod status yet"
          );
          last_pending.clear();
          last_error = Some(error);
        }
      }

      thread::sleep(Duration::from_secs(CLUSTER_READY_RETRY_INTERVAL_SECONDS));
    }

    Err(last_error.unwrap_or_else(|| match last_pending.is_empty() {
      true => anyhow!("timed out waiting for kube-system pods before cluster bridge detection"),
      false => anyhow!(
        "timed out waiting for kube-system pods before cluster bridge detection; pending pods: {}",
        last_pending.join(", ")
      ),
    }))
  }

  fn kube_system_pod_status(&self, ctx: &StepExecutionContext<'_>) -> Result<KubeSystemPodStatus> {
    let output = self.capture(
      ctx,
      &[
        "get".to_string(),
        "pods".to_string(),
        "-n".to_string(),
        "kube-system".to_string(),
        "-o".to_string(),
        "json".to_string(),
        "--request-timeout=10s".to_string(),
      ],
    )?;
    let pods: ClusterPodList =
      serde_json::from_str(&output).context("failed to parse `kubectl get pods` output")?;
    let ready_count = pods.items.len();
    let pending = if pods.items.is_empty() {
      vec!["kube-system/<none> (Pending)".to_string()]
    } else {
      pods
        .items
        .iter()
        .filter(|pod| !pod_is_ready(pod))
        .map(pod_status_summary)
        .collect::<Vec<_>>()
    };

    Ok(KubeSystemPodStatus {
      ready_count,
      pending,
    })
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

struct KubeSystemPodStatus {
  ready_count: usize,
  pending: Vec<String>,
}

fn pod_is_ready(pod: &ClusterPodItem) -> bool {
  if pod.status.phase.as_deref() == Some("Succeeded") {
    return true;
  }

  pod.status.phase.as_deref() == Some("Running")
    && pod
      .status
      .conditions
      .iter()
      .any(|condition| condition.condition_type == "Ready" && condition.status == "True")
}

fn pod_status_summary(pod: &ClusterPodItem) -> String {
  let namespace = if pod.metadata.namespace.is_empty() {
    "kube-system"
  } else {
    pod.metadata.namespace.as_str()
  };
  let phase = pod.status.phase.as_deref().unwrap_or("Unknown");

  format!("{namespace}/{} ({phase})", pod.metadata.name)
}

fn resolve_nodeport_guard_target_for_install(
  ctx: &mut StepExecutionContext<'_>,
) -> Result<NodePortGuardTarget> {
  wait_for_cluster_network_ready(ctx)?;

  if let Some(interface) =
    stored_cluster_bridge_interface(ctx).filter(|interface| network_interface_exists(interface))
  {
    info!(
      interface,
      "reusing recorded cluster bridge interface for gateway guard"
    );
    return Ok(NodePortGuardTarget::Interface(interface));
  }

  let interfaces = discover_network_interfaces()?;
  if interfaces.iter().any(|name| name == "cni0") {
    let interface = "cni0".to_string();
    persist_cluster_bridge_interface(ctx, &interface)?;
    info!(
      interface,
      interfaces = %interfaces.join(", "),
      "detected cluster bridge interface for gateway guard"
    );
    return Ok(NodePortGuardTarget::Interface(interface));
  }

  let interface = prompt_cluster_bridge_interface(ctx, &interfaces)?;
  Ok(NodePortGuardTarget::Interface(interface))
}

fn resolve_nodeport_guard_target_for_cleanup(
  ctx: &StepExecutionContext<'_>,
) -> Result<NodePortGuardTarget> {
  if let Some(interface) = stored_cluster_bridge_interface(ctx) {
    return Ok(NodePortGuardTarget::Interface(interface));
  }

  let interfaces = discover_network_interfaces()?;
  if interfaces.iter().any(|name| name == "cni0") {
    return Ok(NodePortGuardTarget::Interface("cni0".to_string()));
  }

  bail!(
    "unable to determine the cluster bridge interface for gateway guard cleanup; detected interfaces: {}",
    interfaces.join(", ")
  )
}

fn discover_network_interfaces() -> Result<Vec<String>> {
  let mut interfaces = fs::read_dir("/sys/class/net")
    .context("failed to inspect `/sys/class/net` while detecting the cluster bridge interface")?
    .filter_map(|entry| entry.ok())
    .filter_map(|entry| entry.file_name().into_string().ok())
    .collect::<Vec<_>>();
  interfaces.sort();
  Ok(interfaces)
}

fn stored_cluster_bridge_interface(ctx: &StepExecutionContext<'_>) -> Option<String> {
  ctx
    .config()
    .install_step_metadata(InstallStepId::ApplicationGateway, "cluster_interface")
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .map(str::to_string)
}

fn prompt_cluster_bridge_interface(
  ctx: &mut StepExecutionContext<'_>, interfaces: &[String],
) -> Result<String> {
  let detected_interfaces = interfaces.join(", ");
  println!(
    "{}",
    ui::note(t!(
      "install.platform.nodeport_security.interface_detect_failed",
      interfaces = detected_interfaces.as_str()
    ))
  );

  let default_interface = choose_cluster_bridge_interface(interfaces)
    .or_else(|| {
      interfaces
        .iter()
        .find(|name| name.as_str() != "lo")
        .cloned()
    })
    .unwrap_or_else(|| "cni0".to_string());

  loop {
    let interface = InputCollector::new(t!("install.platform.nodeport_security.interface_prompt"))
      .with_default(default_interface.clone())
      .collect()?
      .trim()
      .to_string();

    if network_interface_exists(&interface) {
      persist_cluster_bridge_interface(ctx, &interface)?;
      return Ok(interface);
    }

    println!(
      "{}",
      ui::warning(t!(
        "install.platform.nodeport_security.interface_invalid",
        interface = interface.as_str(),
        interfaces = detected_interfaces.as_str()
      ))
    );
  }
}

fn persist_cluster_bridge_interface(
  ctx: &mut StepExecutionContext<'_>, interface: &str,
) -> Result<()> {
  ctx.persist_change(
    "install.execution.gateway.cluster_interface",
    interface,
    |config| {
      config.set_install_step_metadata(
        InstallStepId::ApplicationGateway,
        "cluster_interface",
        interface.to_string(),
      )
    },
  )?;

  Ok(())
}

fn choose_cluster_bridge_interface(interfaces: &[String]) -> Option<String> {
  for preferred in ["cni0", "flannel.1", "cbr0", "weave", "kube-bridge"] {
    if interfaces.iter().any(|name| name == preferred) {
      return Some(preferred.to_string());
    }
  }

  for prefix in ["cni", "flannel", "cbr", "weave", "kube"] {
    if let Some(name) = interfaces.iter().find(|name| name.starts_with(prefix)) {
      return Some(name.clone());
    }
  }

  None
}

fn network_interface_exists(interface: &str) -> bool {
  Path::new("/sys/class/net").join(interface).exists()
}

fn protected_gateway_ports(exposure: ApplicationExposureMode) -> String {
  let ports = match exposure {
    ApplicationExposureMode::NodePortExternalNginx => [
      KUBERNETES_API_PORT,
      KUBELET_SECURE_PORT,
      PLATFORM_NODE_PORT,
      INTERNAL_REGISTRY_NODE_PORT,
    ],
    ApplicationExposureMode::Ingress => [
      KUBERNETES_API_PORT,
      KUBELET_SECURE_PORT,
      INGRESS_GATEWAY_HTTP_PORT,
      INGRESS_GATEWAY_HTTPS_PORT,
    ],
  };

  ports
    .into_iter()
    .map(|port| port.to_string())
    .collect::<Vec<_>>()
    .join(",")
}

fn legacy_protected_gateway_ports(exposure: ApplicationExposureMode) -> Option<String> {
  let ports = match exposure {
    ApplicationExposureMode::NodePortExternalNginx => [
      KUBELET_SECURE_PORT,
      PLATFORM_NODE_PORT,
      INTERNAL_REGISTRY_NODE_PORT,
    ],
    ApplicationExposureMode::Ingress => [
      KUBELET_SECURE_PORT,
      INGRESS_GATEWAY_HTTP_PORT,
      INGRESS_GATEWAY_HTTPS_PORT,
    ],
  };

  Some(
    ports
      .into_iter()
      .map(|port| port.to_string())
      .collect::<Vec<_>>()
      .join(","),
  )
}

fn gateway_guard_rules(
  operation: &str, cluster_target: &NodePortGuardTarget, protected_ports: &str,
) -> [Vec<String>; 3] {
  [
    gateway_guard_loopback_rule_args(operation, protected_ports, "ACCEPT"),
    gateway_guard_rule_args(operation, cluster_target, protected_ports, "ACCEPT"),
    gateway_guard_any_rule_args(operation, protected_ports, "DROP"),
  ]
}

fn gateway_guard_rule_args(
  operation: &str, target: &NodePortGuardTarget, protected_ports: &str, verdict: &str,
) -> Vec<String> {
  let mut args = vec![
    "-t".to_string(),
    "raw".to_string(),
    operation.to_string(),
    "PREROUTING".to_string(),
  ];

  match target {
    NodePortGuardTarget::Interface(interface) => {
      args.push("-i".to_string());
      args.push(interface.clone());
    }
  }

  args.extend([
    "-p".to_string(),
    "tcp".to_string(),
    "-m".to_string(),
    "multiport".to_string(),
    "--dports".to_string(),
    protected_ports.to_string(),
    "-j".to_string(),
    verdict.to_string(),
  ]);
  args
}

fn gateway_guard_loopback_rule_args(
  operation: &str, protected_ports: &str, verdict: &str,
) -> Vec<String> {
  vec![
    "-t".to_string(),
    "raw".to_string(),
    operation.to_string(),
    "PREROUTING".to_string(),
    "-i".to_string(),
    "lo".to_string(),
    "-p".to_string(),
    "tcp".to_string(),
    "-m".to_string(),
    "multiport".to_string(),
    "--dports".to_string(),
    protected_ports.to_string(),
    "-j".to_string(),
    verdict.to_string(),
  ]
}

fn gateway_guard_any_rule_args(
  operation: &str, protected_ports: &str, verdict: &str,
) -> Vec<String> {
  vec![
    "-t".to_string(),
    "raw".to_string(),
    operation.to_string(),
    "PREROUTING".to_string(),
    "-p".to_string(),
    "tcp".to_string(),
    "-m".to_string(),
    "multiport".to_string(),
    "--dports".to_string(),
    protected_ports.to_string(),
    "-j".to_string(),
    verdict.to_string(),
  ]
}

#[cfg(test)]
mod tests {
  use super::{
    INGRESS_GATEWAY_HTTP_PORT, INGRESS_GATEWAY_HTTPS_PORT, INTERNAL_REGISTRY_NODE_PORT,
    KUBELET_SECURE_PORT, KUBERNETES_API_PORT, ManagedTlsMaterial, NodePortGuardTarget,
    choose_cluster_bridge_interface, gateway_guard_any_rule_args, gateway_guard_loopback_rule_args,
    gateway_guard_rule_args, gateway_guard_rules, remove_custom_site_include_line,
    render_nginx_site,
  };
  use crate::install::steps::platform::PLATFORM_NODE_PORT;

  #[test]
  fn renders_nginx_site_with_original_host_forwarding() {
    let rendered = render_nginx_site("192.168.23.132", 10080, "192.168.23.132", None)
      .expect("template should render");

    assert!(rendered.contains("server 192.168.23.132:10080;"));
    assert!(rendered.contains("server_name 192.168.23.132;"));
    assert!(rendered.contains("proxy_set_header Host $host;"));
    assert!(rendered.contains("proxy_set_header X-Forwarded-Host $host;"));
    assert!(!rendered.contains("{{"));
  }

  #[test]
  fn renders_https_nginx_site_when_tls_material_is_present() {
    let tls_material = ManagedTlsMaterial {
      certificate_path: "/etc/ret2shell/tls/ret2shell-tls/fullchain.pem".to_string(),
      key_path: "/etc/ret2shell/tls/ret2shell-tls/privkey.pem".to_string(),
    };
    let rendered = render_nginx_site("127.0.0.1", 30307, "ctf.example.com", Some(&tls_material))
      .expect("template should render");

    assert!(rendered.contains("listen 443 ssl http2;"));
    assert!(rendered.contains("return 301 https://$host$request_uri;"));
    assert!(rendered.contains("ssl_certificate /etc/ret2shell/tls/ret2shell-tls/fullchain.pem;"));
    assert!(rendered.contains("server 127.0.0.1:30307;"));
  }

  #[test]
  fn removes_custom_site_include_without_touching_default_include() {
    let contents = "http {\n    include /etc/nginx/sites-enabled/*;\n    include /etc/nginx/sites-enabled/*.conf;\n}\n";
    let updated = remove_custom_site_include_line(contents);

    assert!(updated.contains("include /etc/nginx/sites-enabled/*;"));
    assert!(!updated.contains("include /etc/nginx/sites-enabled/*.conf;"));
  }

  #[test]
  fn choose_cluster_bridge_interface_prefers_cni0() {
    let interfaces = vec![
      "eth0".to_string(),
      "cni0".to_string(),
      "flannel.1".to_string(),
    ];

    assert_eq!(
      choose_cluster_bridge_interface(&interfaces),
      Some("cni0".to_string())
    );
  }

  #[test]
  fn choose_cluster_bridge_interface_falls_back_to_matching_prefix() {
    let interfaces = vec![
      "eth0".to_string(),
      "flannel.1".to_string(),
      "lo".to_string(),
    ];

    assert_eq!(
      choose_cluster_bridge_interface(&interfaces),
      Some("flannel.1".to_string())
    );
  }

  #[test]
  fn nodeport_guard_rule_uses_interface_when_detected() {
    let args = gateway_guard_rule_args(
      "-C",
      &NodePortGuardTarget::Interface("cni0".to_string()),
      &format!(
        "{KUBERNETES_API_PORT},{KUBELET_SECURE_PORT},{PLATFORM_NODE_PORT},{INTERNAL_REGISTRY_NODE_PORT}"
      ),
      "ACCEPT",
    );

    assert!(args.windows(2).any(|pair| pair == ["-i", "cni0"]));
  }

  #[test]
  fn gateway_guard_loopback_rule_includes_requested_ingress_ports() {
    let protected_ports = format!(
      "{KUBERNETES_API_PORT},{KUBELET_SECURE_PORT},{INGRESS_GATEWAY_HTTP_PORT},{INGRESS_GATEWAY_HTTPS_PORT}"
    );
    let args = gateway_guard_loopback_rule_args("-C", &protected_ports, "ACCEPT");

    assert!(args.windows(2).any(|pair| pair == ["-i", "lo"]));
    assert!(
      args
        .windows(2)
        .any(|pair| pair == ["--dports", protected_ports.as_str()])
    );
  }

  #[test]
  fn gateway_guard_drop_rule_targets_all_sources() {
    let protected_ports = format!(
      "{KUBERNETES_API_PORT},{KUBELET_SECURE_PORT},{PLATFORM_NODE_PORT},{INTERNAL_REGISTRY_NODE_PORT}"
    );
    let args = gateway_guard_any_rule_args("-C", &protected_ports, "DROP");

    assert!(!args.iter().any(|value| value == "-i"));
    assert!(!args.iter().any(|value| value == "-s"));
    assert!(
      args
        .windows(2)
        .any(|pair| pair == ["--dports", protected_ports.as_str()])
    );
  }

  #[test]
  fn gateway_guard_rules_are_ordered_as_accept_accept_drop() {
    let protected_ports = format!(
      "{KUBERNETES_API_PORT},{KUBELET_SECURE_PORT},{PLATFORM_NODE_PORT},{INTERNAL_REGISTRY_NODE_PORT}"
    );
    let rules = gateway_guard_rules(
      "-I",
      &NodePortGuardTarget::Interface("cni0".to_string()),
      &protected_ports,
    );

    assert!(rules[0].windows(2).any(|pair| pair == ["-i", "lo"]));
    assert!(rules[1].windows(2).any(|pair| pair == ["-i", "cni0"]));
    assert_eq!(rules[2].last().map(String::as_str), Some("DROP"));
  }
}
