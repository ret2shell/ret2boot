use std::{fs, path::PathBuf};

use anyhow::{Context, Result, anyhow};
use rust_i18n::t;

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  SystemPackageManager,
  cluster::cluster_gateway_port_metadata,
  support::{
    detect_nginx_binary_path, ensure_symlink, install_directory, install_staged_file,
    nginx_service_exists, stage_text_file,
  },
};
use crate::{
  config::{ApplicationExposureMode, InstallStepId, InstallTargetRole},
  install::collectors::SingleSelectCollector,
  ui,
};

const NGINX_BINARY_DEST: &str = "/usr/sbin/nginx";
const NGINX_MAIN_CONF: &str = "/etc/nginx/nginx.conf";
const NGINX_SITE_AVAILABLE: &str = "/etc/nginx/sites-available/ret2boot.conf";
const NGINX_SITE_ENABLED: &str = "/etc/nginx/sites-enabled/ret2boot.conf";
const NGINX_SITE_INCLUDE: &str = "/etc/nginx/conf.d/ret2boot-sites-enabled.conf";
const NGINX_STREAM_AVAILABLE: &str = "/etc/nginx/ret2boot-stream-available/ret2boot.conf";
const NGINX_STREAM_ENABLED: &str = "/etc/nginx/ret2boot-stream-enabled/ret2boot.conf";
const NGINX_STREAM_INCLUDE_MARKER: &str = "include /etc/nginx/ret2boot-stream-enabled/*.conf;";

pub struct ApplicationGatewayStep;

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
      .collect::<Vec<_>>();

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

    let gateway_http_port = cluster_gateway_port_metadata(ctx, "gateway_http_port", 10080);
    let gateway_https_port = cluster_gateway_port_metadata(ctx, "gateway_https_port", 10443);

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
      config.remove_install_step_metadata(self.id(), "installed_by_ret2boot") || changed
    })?;

    Ok(())
  }
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

fn ensure_nginx_stream_include(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let contents = fs::read_to_string(NGINX_MAIN_CONF)
    .with_context(|| format!("failed to read `{NGINX_MAIN_CONF}`"))?;

  if contents.contains(NGINX_STREAM_INCLUDE_MARKER) {
    return Ok(());
  }

  let http_index = contents
    .find("http {")
    .ok_or_else(|| anyhow!("unable to locate the http block in `{NGINX_MAIN_CONF}`"))?;
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

fn application_exposure_label(exposure: ApplicationExposureMode) -> String {
  match exposure {
    ApplicationExposureMode::Ingress => t!("install.exposure.options.ingress").to_string(),
    ApplicationExposureMode::NodePortExternalNginx => {
      t!("install.exposure.options.nodeport_external_nginx").to_string()
    }
  }
}
