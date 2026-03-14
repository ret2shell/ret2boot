use std::{
  env, fs,
  time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rust_i18n::t;
use tracing::{info, warn};

use crate::{
  config::{ROOT_CONFIG_PATH, Ret2BootConfig},
  install::collectors::{Collector, ConfirmCollector, SingleSelectCollector},
  l10n,
  privilege::PrivilegeSession,
  terminal::TerminalCharset,
  ui,
  update::{self, UpdateCheckResult},
};

pub struct RuntimeState {
  pub locale: String,
  pub privilege_backend: &'static str,
  _privilege_session: PrivilegeSession,
}

impl RuntimeState {
  pub fn run_privileged_command(
    &self, program: &str, args: &[String], envs: &[(String, String)],
  ) -> Result<()> {
    self._privilege_session.run_command(program, args, envs)
  }

  pub fn run_privileged_command_capture(
    &self, program: &str, args: &[String], envs: &[(String, String)],
  ) -> Result<String> {
    self
      ._privilege_session
      .run_command_capture(program, args, envs)
  }

  pub fn persist_system_config_copy(&self, config: &Ret2BootConfig) -> Result<()> {
    let stamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map(|duration| duration.as_nanos())
      .unwrap_or_default();
    let temp_path = env::temp_dir().join(format!("ret2boot-system-config-{stamp}.toml"));
    let contents = toml::to_string_pretty(config).context("failed to serialize app config")?;

    fs::write(&temp_path, format!("{contents}\n"))
      .with_context(|| format!("failed to write `{}`", temp_path.display()))?;

    let install_result = self.run_privileged_command(
      "install",
      &[
        "-D".to_string(),
        "-m".to_string(),
        "600".to_string(),
        temp_path.display().to_string(),
        ROOT_CONFIG_PATH.to_string(),
      ],
      &[],
    );
    let _ = fs::remove_file(&temp_path);

    install_result
  }
}

pub fn initialize(config: &mut Ret2BootConfig) -> Result<Option<RuntimeState>> {
  initialize_with_safety_confirmation(config, true)
}

pub fn initialize_maintenance(config: &mut Ret2BootConfig) -> Result<RuntimeState> {
  initialize_with_safety_confirmation(config, false)?.ok_or_else(|| {
    anyhow::anyhow!("maintenance initialization exited before a runtime state was available")
  })
}

fn initialize_with_safety_confirmation(
  config: &mut Ret2BootConfig, require_safety_confirmation: bool,
) -> Result<Option<RuntimeState>> {
  let config_path = Ret2BootConfig::path_display()?;
  let terminal_charset = TerminalCharset::detect();
  let mut needs_save = config.set_terminal_charset(terminal_charset.as_config_value());

  let locale = if !terminal_charset.is_utf8() {
    let locale = l10n::DEFAULT_LOCALE.to_string();
    l10n::set_locale(&locale);

    if config.set_language(locale.clone()) {
      needs_save = true;
    }

    warn!(
        config_path = %config_path,
        "terminal appears to be ASCII-only; forcing en-us locale"
    );

    locale
  } else {
    resolve_utf8_locale(config, &config_path, &mut needs_save)?
  };

  if needs_save {
    config.save()?;
  }

  handle_update_check(update::check_for_updates());

  if require_safety_confirmation {
    print_safety_notice(&config_path);

    let should_continue =
      ConfirmCollector::new(t!("startup.safety.continue_prompt"), false).collect()?;

    if !should_continue {
      info!(config_path = %config_path, "user cancelled before deployment privileges");
      println!();
      println!("{}", ui::warning(t!("startup.safety.cancelled")));
      return Ok(None);
    }
  }

  let already_root = PrivilegeSession::is_root_user();

  if !already_root {
    println!();
    println!("{}", ui::section(t!("startup.privilege.request")));
  }

  let privilege_session = PrivilegeSession::acquire()?;
  let privilege_backend = privilege_session.backend_name();

  println!();
  println!(
    "{}",
    if already_root {
      ui::success(t!("startup.privilege.already_root"))
    } else {
      ui::success(t!("startup.privilege.acquired"))
    }
  );

  info!(
      config_path = %config_path,
      locale = %locale,
      privilege_backend,
      "deployment privileges are ready"
  );

  Ok(Some(RuntimeState {
    locale,
    privilege_backend,
    _privilege_session: privilege_session,
  }))
}

fn resolve_utf8_locale(
  config: &mut Ret2BootConfig, config_path: &str, needs_save: &mut bool,
) -> Result<String> {
  let system_locale = l10n::system_locale().unwrap_or_else(|| l10n::DEFAULT_LOCALE.to_string());

  if let Some(locale) = configured_locale(config, config_path, needs_save) {
    l10n::set_locale(&locale);
    return Ok(locale);
  }

  l10n::set_locale(&system_locale);

  let locale = choose_language(&system_locale)?;
  l10n::set_locale(&locale);

  if config.set_language(locale.clone()) {
    *needs_save = true;
  }

  info!(
      selected_locale = %locale,
      default_locale = %system_locale,
      config_path = %config_path,
      "selected interface language"
  );

  Ok(locale)
}

fn configured_locale(
  config: &mut Ret2BootConfig, config_path: &str, needs_save: &mut bool,
) -> Option<String> {
  let raw = config.language.clone()?;

  match l10n::normalize_locale(&raw) {
    Some(locale) => {
      if raw != locale && config.set_language(locale.clone()) {
        *needs_save = true;
      }

      Some(locale)
    }
    None => {
      warn!(
          configured_language = %raw,
          config_path = %config_path,
          "ignoring unsupported language from config"
      );

      None
    }
  }
}

fn choose_language(system_locale: &str) -> Result<String> {
  let options = l10n::locale_options();
  let default = options
    .iter()
    .position(|option| option.id == system_locale)
    .or_else(|| {
      options
        .iter()
        .position(|option| option.id == l10n::DEFAULT_LOCALE)
    })
    .unwrap_or(0);

  let selected = SingleSelectCollector::new(
    t!("startup.language.prompt"),
    options.iter().map(|option| option.label.clone()).collect(),
  )
  .with_default(default)
  .collect_index()?;

  Ok(options[selected].id.to_string())
}

fn print_safety_notice(config_path: &str) {
  println!();
  println!("{}", ui::section(t!("startup.safety.title")));
  println!("{}", ui::warning(t!("startup.safety.root_required")));
  println!("{}", ui::warning(t!("startup.safety.clean_server")));
  println!("{}", ui::warning(t!("startup.safety.conflict_risk")));
  println!(
    "{}",
    ui::note(t!("startup.safety.user_config", path = config_path))
  );
}

fn handle_update_check(report: UpdateCheckResult) {
  match report {
    UpdateCheckResult::UpToDate | UpdateCheckResult::NoPublishedRelease => {}
    UpdateCheckResult::Downloaded {
      version,
      path,
      reused,
    } => {
      println!();

      let message = if reused {
        ui::note(t!(
          "startup.update.cached",
          version = version.as_str(),
          path = path.display().to_string()
        ))
      } else {
        ui::success(t!(
          "startup.update.downloaded",
          version = version.as_str(),
          path = path.display().to_string()
        ))
      };

      println!("{message}");
    }
    UpdateCheckResult::UpdateAvailableNoAsset {
      source,
      version,
      release_url,
    } => {
      println!();
      println!(
        "{}",
        ui::warning(t!(
          "startup.update.no_asset",
          source = source.as_str(),
          version = version.as_str()
        ))
      );
      println!(
        "{}",
        ui::note(t!(
          "startup.update.release_page",
          release_url = release_url.as_str()
        ))
      );
    }
    UpdateCheckResult::DownloadFailed {
      source,
      version,
      release_url,
    } => {
      println!();
      println!(
        "{}",
        ui::warning(t!(
          "startup.update.download_failed",
          source = source.as_str(),
          version = version.as_str()
        ))
      );
      println!(
        "{}",
        ui::note(t!(
          "startup.update.release_page",
          release_url = release_url.as_str()
        ))
      );
    }
    UpdateCheckResult::Unavailable {
      source,
      repository_url,
    } => {
      println!();
      println!(
        "{}",
        ui::warning(t!(
          "startup.update.unreachable",
          source = source.as_str(),
          repository = repository_url.as_str()
        ))
      );
    }
  }
}
