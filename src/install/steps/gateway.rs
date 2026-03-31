use std::{
  fs,
  path::{Path, PathBuf},
  process::Command,
};

use anyhow::{Context, Result, anyhow};
use rust_i18n::t;
use tracing::warn;

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  SystemPackageManager,
  platform::{PLATFORM_NODE_PORT, resolve_public_endpoint},
  support::{
    detect_nginx_binary_path, ensure_symlink, file_contains, install_directory,
    install_staged_file, nginx_service_exists, stage_text_file,
  },
};
use crate::{
  config::{ApplicationExposureMode, DeploymentProfile, InstallStepId, InstallTargetRole},
  install::collectors::SingleSelectCollector,
  resources, ui,
};

const NGINX_BINARY_DEST: &str = "/usr/sbin/nginx";
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
const NGINX_MODULES_INCLUDE_MARKER: &str = "include /etc/nginx/modules-enabled/*.conf;";

pub struct ApplicationGatewayStep;

impl AtomicInstallStep for ApplicationGatewayStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::ApplicationGateway
  }

  fn should_include(&self, ctx: &StepPlanContext<'_>) -> bool {
    ctx.node_role() == Some(InstallTargetRole::ControlPlane)
  }

  fn collect(&self, ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    let default_profile = ctx
      .config()
      .install
      .questionnaire
      .platform
      .deployment_profile
      .unwrap_or(DeploymentProfile::LocalLab)
      .default_index();
    let profile_options = DeploymentProfile::ALL
      .iter()
      .copied()
      .map(deployment_profile_label)
      .collect::<Vec<_>>();
    let profile = DeploymentProfile::ALL[SingleSelectCollector::new(
      t!("install.deployment_profile.prompt"),
      profile_options,
    )
    .with_default(default_profile)
    .collect_index()?];

    ctx.persist_change(
      "install.questionnaire.platform.deployment_profile",
      profile.as_config_value(),
      |config| config.set_platform_deployment_profile(profile),
    )?;

    println!();
    println!("{}", ui::note(deployment_profile_notice(profile)));

    let supported_exposures = supported_application_exposure_modes(profile);
    let exposure = if supported_exposures.len() == 1 {
      let exposure = supported_exposures[0];
      println!();
      println!(
        "{}",
        ui::note(t!(
          "install.exposure.profile_locked",
          exposure = application_exposure_label(exposure)
        ))
      );
      exposure
    } else {
      let default = ctx
        .config()
        .install
        .questionnaire
        .kubernetes
        .application_exposure
        .filter(|exposure| supported_exposures.contains(exposure))
        .unwrap_or(profile.recommended_exposure())
        .default_index();
      let options = supported_exposures
        .iter()
        .copied()
        .map(application_exposure_label)
        .collect::<Vec<_>>();

      supported_exposures[SingleSelectCollector::new(t!("install.exposure.prompt"), options)
        .with_default(default)
        .collect_index()?]
    };

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
    let profile = ctx.deployment_profile().ok_or_else(|| {
      anyhow!("deployment profile is required before planning gateway setup")
    })?;
    let exposure = ctx.application_exposure().ok_or_else(|| {
      anyhow!("application exposure mode is required before planning gateway setup")
    })?;
    validate_profile_exposure(profile, exposure)?;

    let mut details = vec![
      t!(
        "install.steps.gateway.selected_profile",
        profile = deployment_profile_label(profile)
      )
      .to_string(),
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
    let profile = ctx.as_plan_context().deployment_profile().ok_or_else(|| {
      anyhow!("deployment profile is required before installing gateway")
    })?;
    let exposure = ctx
      .as_plan_context()
      .application_exposure()
      .ok_or_else(|| anyhow!("application exposure mode is required before installing gateway"))?;
    validate_profile_exposure(profile, exposure)?;

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
    let installed_by_ret2boot = ctx
      .config()
      .install_step_metadata(self.id(), "installed_by_ret2boot")
      .is_some_and(|value| value == "true");

    if !nginx_existed {
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
    let endpoint = resolve_public_endpoint(public_host, exposure, profile)?;
    let backend_host = endpoint.public_host.clone();
    let backend_http_port = PLATFORM_NODE_PORT;
    let https_gateway_enabled = false;

    install_external_nginx_gateway(
      ctx,
      backend_host.as_str(),
      backend_http_port,
      &endpoint.public_host,
      &endpoint.ingress_host,
      https_gateway_enabled,
    )?;
    let nginx_binary =
      detect_nginx_binary_path().unwrap_or_else(|| PathBuf::from(NGINX_BINARY_DEST));
    ctx.run_privileged_command(
      "systemctl",
      &[
        "enable".to_string(),
        "--now".to_string(),
        "nginx.service".to_string(),
      ],
      &[],
    )?;

    if !https_gateway_enabled {
      println!("{}", ui::warning(t!("install.exposure.nodeport_https_degraded")));
    }

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
      let changed =
        config.set_install_step_metadata(self.id(), "upstream_host", endpoint.ingress_host.clone())
          || changed;
      let changed = config.set_install_step_metadata(
        self.id(),
        "https_gateway_enabled",
        if https_gateway_enabled { "true" } else { "false" },
      ) || changed;
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
      let changed = config.remove_install_step_metadata(self.id(), "upstream_host") || changed;
      let changed =
        config.remove_install_step_metadata(self.id(), "https_gateway_enabled") || changed;
      config.remove_install_step_metadata(self.id(), "installed_by_ret2boot") || changed
    })?;

    Ok(())
  }
}

fn install_external_nginx_gateway(
  ctx: &StepExecutionContext<'_>, backend_host: &str, http_port: u16, server_name: &str,
  upstream_host: &str, enable_https_stream: bool,
) -> Result<()> {
  install_directory(ctx, "/etc/nginx/sites-available")?;
  install_directory(ctx, "/etc/nginx/sites-enabled")?;
  install_directory(ctx, NGINX_LOG_DIR)?;

  ensure_nginx_site_include(ctx)?;

    let site_path = stage_text_file(
      "nginx-ret2boot-site",
      "conf",
      render_nginx_http_site(backend_host, http_port, upstream_host, server_name)?,
    )?;
  install_staged_file(ctx, &site_path, NGINX_SITE_AVAILABLE)?;
  let _ = fs::remove_file(&site_path);
  ensure_symlink(ctx, NGINX_SITE_AVAILABLE, NGINX_SITE_ENABLED)?;

  if !enable_https_stream {
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
    return Ok(());
  }

  install_directory(ctx, "/etc/nginx/ret2boot-stream-available")?;
  install_directory(ctx, "/etc/nginx/ret2boot-stream-enabled")?;

  let stream_path = stage_text_file(
    "nginx-ret2boot-stream",
    "conf",
    render_nginx_stream_site(http_port)?,
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

fn render_nginx_http_site(
  backend_host: &str, http_port: u16, upstream_host: &str, server_name: &str,
) -> Result<String> {
  resources::load_utf8("templates/nginx/ret2shell.conf.tmpl").map(|template| {
    template
      .replace("{{BACKEND_HOST}}", backend_host)
      .replace("{{BACKEND_HTTP_PORT}}", &http_port.to_string())
      .replace("{{UPSTREAM_HOST}}", upstream_host)
      .replace("{{SERVER_NAME}}", server_name)
  })
}

fn render_nginx_stream_site(https_port: u16) -> Result<String> {
  resources::load_utf8("templates/nginx/ret2shell-stream.conf.tmpl")
    .map(|template| template.replace("{{BACKEND_HTTPS_PORT}}", &https_port.to_string()))
}

fn best_effort_enable_nginx_stream(
  ctx: &StepExecutionContext<'_>, package_manager: SystemPackageManager, nginx_binary: &Path,
) {
  if nginx_supports_stream(nginx_binary) {
    return;
  }

  if let Err(error) = package_manager.ensure_nginx_stream_module(ctx) {
    warn!(error = %error, "failed to install optional nginx stream module");
  }

  if nginx_supports_stream(nginx_binary) {
    return;
  }

  if let Err(error) = ensure_nginx_dynamic_stream_module_loaded(ctx) {
    warn!(error = %error, "failed to enable optional nginx stream module");
  }
}

fn nginx_supports_stream(nginx_binary: &Path) -> bool {
  let Some(arguments) = nginx_compile_arguments(nginx_binary) else {
    return false;
  };

  if nginx_has_built_in_stream(&arguments) {
    return true;
  }

  nginx_has_dynamic_stream(&arguments) && nginx_stream_module_is_enabled()
}

fn nginx_compile_arguments(nginx_binary: &Path) -> Option<String> {
  let output = Command::new(nginx_binary).arg("-V").output().ok()?;
  let stdout = String::from_utf8_lossy(&output.stdout);
  let stderr = String::from_utf8_lossy(&output.stderr);

  Some(format!("{stdout}\n{stderr}"))
}

fn nginx_has_built_in_stream(arguments: &str) -> bool {
  arguments.contains("--with-stream") && !arguments.contains("--with-stream=dynamic")
}

fn nginx_has_dynamic_stream(arguments: &str) -> bool {
  arguments.contains("--with-stream=dynamic")
}

fn nginx_stream_module_is_enabled() -> bool {
  fs::read_dir("/etc/nginx/modules-enabled")
    .ok()
    .into_iter()
    .flat_map(|entries| entries.filter_map(Result::ok))
    .map(|entry| entry.path())
    .filter(|path| path.is_file())
    .any(|path| {
      path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.contains("stream"))
        || fs::read_to_string(&path)
          .map(|contents| contents.contains("ngx_stream_module.so"))
          .unwrap_or(false)
    })
}

fn nginx_stream_module_path() -> Option<&'static str> {
  [
    "/usr/lib/nginx/modules/ngx_stream_module.so",
    "/usr/lib64/nginx/modules/ngx_stream_module.so",
  ]
  .into_iter()
  .find(|path| Path::new(path).is_file())
}

fn ensure_nginx_dynamic_stream_module_loaded(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let Some(module_path) = nginx_stream_module_path() else {
    return Ok(());
  };

  if nginx_stream_module_is_enabled() {
    return Ok(());
  }

  if !file_contains(NGINX_MAIN_CONF, NGINX_MODULES_INCLUDE_MARKER) {
    return Ok(());
  }

  let staged = stage_text_file(
    "nginx-stream-module",
    "conf",
    format!("load_module {module_path};\n"),
  )?;
  install_staged_file(ctx, &staged, NGINX_STREAM_MODULE_RET2BOOT_CONF)?;
  let _ = fs::remove_file(&staged);

  Ok(())
}

fn ensure_nginx_site_include(ctx: &StepExecutionContext<'_>) -> Result<()> {
  let contents = fs::read_to_string(NGINX_MAIN_CONF)
    .with_context(|| format!("failed to read `{NGINX_MAIN_CONF}`"))?;

  if contents.contains(NGINX_SITE_INCLUDE_MARKER_DEFAULT) && contents.contains(NGINX_SITE_INCLUDE_MARKER) {
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
    ApplicationExposureMode::Ingress => t!("install.exposure.options.ingress").to_string(),
    ApplicationExposureMode::NodePortExternalNginx => {
      t!("install.exposure.options.nodeport_external_nginx").to_string()
    }
  }
}

fn supported_application_exposure_modes(
  profile: DeploymentProfile,
) -> &'static [ApplicationExposureMode] {
  match profile {
    DeploymentProfile::LocalLab => &[ApplicationExposureMode::NodePortExternalNginx],
    DeploymentProfile::CampusInternal | DeploymentProfile::PublicDomain => {
      &[ApplicationExposureMode::Ingress]
    }
  }
}

fn validate_profile_exposure(
  profile: DeploymentProfile, exposure: ApplicationExposureMode,
) -> Result<()> {
  if supported_application_exposure_modes(profile).contains(&exposure) {
    return Ok(());
  }

  match profile {
    DeploymentProfile::LocalLab => bail!(
      "the local intranet debugging profile only supports `NodePort + external nginx` exposure"
    ),
    DeploymentProfile::CampusInternal | DeploymentProfile::PublicDomain => {
      bail!("this deployment profile only supports `Kubernetes ingress` exposure")
    }
  }
}

fn deployment_profile_label(profile: DeploymentProfile) -> String {
  match profile {
    DeploymentProfile::LocalLab => t!("install.deployment_profile.options.local_lab").to_string(),
    DeploymentProfile::CampusInternal => {
      t!("install.deployment_profile.options.campus_internal").to_string()
    }
    DeploymentProfile::PublicDomain => {
      t!("install.deployment_profile.options.public_domain").to_string()
    }
  }
}

fn deployment_profile_notice(profile: DeploymentProfile) -> String {
  match profile {
    DeploymentProfile::LocalLab => t!("install.deployment_profile.notice.local_lab").to_string(),
    DeploymentProfile::CampusInternal => {
      t!("install.deployment_profile.notice.campus_internal").to_string()
    }
    DeploymentProfile::PublicDomain => {
      t!("install.deployment_profile.notice.public_domain").to_string()
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{
    nginx_has_built_in_stream, nginx_has_dynamic_stream, remove_custom_site_include_line,
    render_nginx_http_site, supported_application_exposure_modes, validate_profile_exposure,
  };
  use crate::config::{ApplicationExposureMode, DeploymentProfile};

  #[test]
  fn renders_nginx_site_with_rewritten_upstream_host() {
    let rendered = render_nginx_http_site(
      "192.168.23.132",
      10080,
      "ret2shell-103-151-173-97.ret2boot.invalid",
      "192.168.23.132",
    )
    .expect("template should render");

    assert!(rendered.contains("server 192.168.23.132:10080;"));
    assert!(rendered.contains("server_name 192.168.23.132;"));
    assert!(rendered.contains(
      "proxy_set_header Host ret2shell-103-151-173-97.ret2boot.invalid;"
    ));
    assert!(rendered.contains("proxy_set_header X-Forwarded-Host $host;"));
  }

  #[test]
  fn detects_built_in_and_dynamic_stream_flags() {
    assert!(nginx_has_built_in_stream("configure arguments: --with-stream --with-http_ssl_module"));
    assert!(!nginx_has_built_in_stream(
      "configure arguments: --with-stream=dynamic --with-http_ssl_module"
    ));
    assert!(nginx_has_dynamic_stream(
      "configure arguments: --with-stream=dynamic --with-http_ssl_module"
    ));
  }

  #[test]
  fn removes_custom_site_include_without_touching_default_include() {
    let contents = "http {\n    include /etc/nginx/sites-enabled/*;\n    include /etc/nginx/sites-enabled/*.conf;\n}\n";
    let updated = remove_custom_site_include_line(contents);

    assert!(updated.contains("include /etc/nginx/sites-enabled/*;"));
    assert!(!updated.contains("include /etc/nginx/sites-enabled/*.conf;"));
  }

  #[test]
  fn local_lab_profile_only_offers_nodeport_exposure() {
    assert_eq!(
      supported_application_exposure_modes(DeploymentProfile::LocalLab),
      [ApplicationExposureMode::NodePortExternalNginx]
    );
  }

  #[test]
  fn campus_and_public_profiles_only_offer_ingress_exposure() {
    assert_eq!(
      supported_application_exposure_modes(DeploymentProfile::CampusInternal),
      [ApplicationExposureMode::Ingress]
    );
    assert_eq!(
      supported_application_exposure_modes(DeploymentProfile::PublicDomain),
      [ApplicationExposureMode::Ingress]
    );
  }

  #[test]
  fn local_lab_profile_rejects_ingress_exposure() {
    let error = validate_profile_exposure(
      DeploymentProfile::LocalLab,
      ApplicationExposureMode::Ingress,
    )
    .expect_err("local lab should reject ingress exposure");

    assert!(error.to_string().contains("only supports"));
  }

  #[test]
  fn campus_internal_profile_rejects_nodeport_exposure() {
    let error = validate_profile_exposure(
      DeploymentProfile::CampusInternal,
      ApplicationExposureMode::NodePortExternalNginx,
    )
    .expect_err("campus internal should reject nodeport exposure");

    assert!(error.to_string().contains("Kubernetes ingress"));
  }
}
