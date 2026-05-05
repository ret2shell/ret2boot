use std::{
  env, fs,
  path::{Path, PathBuf},
  process::Command,
  thread,
  time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{blocking::Client, header::USER_AGENT};
use rust_i18n::t;
use semver::Version;
use tracing::{info, warn};

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  support::{find_command_path, unique_temp_path},
};
use crate::{
  checksum::sha256_bytes_hex,
  config::{InstallStepId, InstallTargetRole},
};

const HELM_LATEST_VERSION_URL: &str = "https://get.helm.sh/helm3-latest-version";
const HELM_DOWNLOAD_BASE_URL: &str = "https://get.helm.sh";
const HELM_BINARY_DEST: &str = "/usr/local/bin/helm";
const HELM_DOWNLOAD_ATTEMPTS: usize = 3;

pub struct HelmCliStep;

impl AtomicInstallStep for HelmCliStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::HelmCli
  }

  fn should_include(&self, ctx: &StepPlanContext<'_>) -> bool {
    ctx.node_role() == Some(InstallTargetRole::ControlPlane)
  }

  fn collect(&self, _ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    println!();
    println!("{}", crate::ui::note(t!("install.helm.notice")));

    if let Some(path) = find_command_path("helm") {
      println!(
        "{}",
        crate::ui::note(t!(
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
      let path_display = path.display().to_string();
      let owned = ctx
        .config()
        .install_step_metadata(self.id(), "owned_by_ret2boot")
        .is_some_and(|value| value == "true")
        && ctx
          .config()
          .install_step_metadata(self.id(), "binary_path")
          .is_some_and(|value| value == path_display);

      ctx.persist_change(
        "install.execution.helm.owned_by_ret2boot",
        if owned { "true" } else { "false" },
        |config| {
          let changed = config.set_install_step_metadata(
            self.id(),
            "owned_by_ret2boot",
            if owned { "true" } else { "false" },
          );
          let changed =
            config.set_install_step_metadata(self.id(), "binary_path", path_display) || changed;
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

    let package = stage_helm_release_package()?;
    info!(
      step = self.id().as_config_value(),
      version = package.version.as_str(),
      url = package.download_url.as_str(),
      "installing helm from official release archive"
    );

    let install_result = install_helm_binary(ctx, &package.binary_path);
    let _ = fs::remove_dir_all(&package.temp_root);
    install_result?;

    ctx.persist_change(
      "install.execution.helm.owned_by_ret2boot",
      "true",
      |config| {
        let changed = config.set_install_step_metadata(self.id(), "owned_by_ret2boot", "true");
        let changed =
          config.set_install_step_metadata(self.id(), "binary_path", HELM_BINARY_DEST) || changed;
        let changed = config.set_install_step_metadata(
          self.id(),
          "install_source",
          package.download_url.clone(),
        ) || changed;
        config.set_install_step_metadata(self.id(), "version", package.version.clone()) || changed
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

    ctx.run_privileged_command("rm", &["-f".to_string(), binary_path], &[])?;
    ctx.persist_change(
      "install.execution.helm.owned_by_ret2boot",
      "false",
      |config| {
        let changed = config.remove_install_step_metadata(self.id(), "owned_by_ret2boot");
        let changed = config.remove_install_step_metadata(self.id(), "binary_path") || changed;
        let changed = config.remove_install_step_metadata(self.id(), "install_source") || changed;
        config.remove_install_step_metadata(self.id(), "version") || changed
      },
    )?;

    Ok(())
  }
}

struct HelmReleasePackage {
  version: String,
  download_url: String,
  temp_root: PathBuf,
  binary_path: PathBuf,
}

fn stage_helm_release_package() -> Result<HelmReleasePackage> {
  let client = Client::builder()
    .https_only(true)
    .timeout(Duration::from_secs(60))
    .build()
    .context("failed to build helm download HTTP client")?;
  let version = fetch_latest_helm_version(&client)?;
  let (os, arch) = helm_target_triplet()?;
  let archive_name = format!("helm-{version}-{os}-{arch}.tar.gz");
  let download_url = format!("{HELM_DOWNLOAD_BASE_URL}/{archive_name}");
  let checksum_url = format!("{download_url}.sha256");
  let archive = download_bytes_with_retries(&client, &download_url, "helm archive")?;
  let checksum = download_text_with_retries(&client, &checksum_url, "helm checksum")?;
  let expected_checksum = parse_helm_archive_checksum(&checksum, &archive_name)?;
  let actual_checksum = sha256_bytes_hex(&archive);

  if actual_checksum != expected_checksum {
    bail!(
      "downloaded Helm archive checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
    );
  }

  let temp_root = unique_temp_path("helm-install", "tmp");
  let extract_dir = temp_root.join("extract");
  let archive_path = temp_root.join(&archive_name);
  fs::create_dir_all(&extract_dir)
    .with_context(|| format!("failed to create `{}`", extract_dir.display()))?;
  fs::write(&archive_path, &archive)
    .with_context(|| format!("failed to write `{}`", archive_path.display()))?;
  extract_helm_archive(&archive_path, &extract_dir)?;

  let binary_path = extract_dir.join(format!("{os}-{arch}")).join("helm");
  if !binary_path.is_file() {
    bail!(
      "the Helm archive `{archive_name}` did not contain an executable at `{}`",
      binary_path.display()
    );
  }

  Ok(HelmReleasePackage {
    version,
    download_url,
    temp_root,
    binary_path,
  })
}

fn install_helm_binary(ctx: &StepExecutionContext<'_>, binary_path: &Path) -> Result<()> {
  ctx.run_privileged_command(
    "install",
    &[
      "-D".to_string(),
      "-m".to_string(),
      "755".to_string(),
      binary_path.display().to_string(),
      HELM_BINARY_DEST.to_string(),
    ],
    &[],
  )
}

fn fetch_latest_helm_version(client: &Client) -> Result<String> {
  let raw = download_text_with_retries(client, HELM_LATEST_VERSION_URL, "helm version manifest")?;
  parse_helm_version_manifest(&raw)
}

fn extract_helm_archive(archive_path: &Path, extract_dir: &Path) -> Result<()> {
  let tar = find_command_path("tar").ok_or_else(|| anyhow!("`tar` is required to extract Helm"))?;
  let output = Command::new(&tar)
    .args([
      "-xzf",
      &archive_path.display().to_string(),
      "-C",
      &extract_dir.display().to_string(),
    ])
    .output()
    .with_context(|| format!("failed to run `{}`", tar.display()))?;

  if output.status.success() {
    return Ok(());
  }

  let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
  let detail = if stderr.is_empty() {
    String::new()
  } else {
    format!(": {stderr}")
  };

  Err(anyhow!(
    "`{}` exited with status {:?}{detail}",
    tar.display(),
    output.status.code()
  ))
}

fn download_text_with_retries(client: &Client, url: &str, label: &str) -> Result<String> {
  for attempt in 1..=HELM_DOWNLOAD_ATTEMPTS {
    match client
      .get(url)
      .header(USER_AGENT, helm_user_agent())
      .send()
      .with_context(|| format!("failed to request {label} `{url}`"))
      .and_then(|response| {
        response
          .error_for_status()
          .with_context(|| format!("{label} `{url}` returned an error status"))
      })
      .and_then(|response| {
        response
          .text()
          .with_context(|| format!("failed to read {label} `{url}`"))
      }) {
      Ok(body) => return Ok(body),
      Err(error) if attempt < HELM_DOWNLOAD_ATTEMPTS => {
        warn!(
          attempt,
          attempts = HELM_DOWNLOAD_ATTEMPTS,
          url,
          error = %error,
          "helm download attempt failed; retrying"
        );
        thread::sleep(Duration::from_secs(attempt as u64));
      }
      Err(error) => return Err(error),
    }
  }

  unreachable!("download loop should return or error on the last attempt")
}

fn download_bytes_with_retries(client: &Client, url: &str, label: &str) -> Result<Vec<u8>> {
  for attempt in 1..=HELM_DOWNLOAD_ATTEMPTS {
    match client
      .get(url)
      .header(USER_AGENT, helm_user_agent())
      .send()
      .with_context(|| format!("failed to request {label} `{url}`"))
      .and_then(|response| {
        response
          .error_for_status()
          .with_context(|| format!("{label} `{url}` returned an error status"))
      })
      .and_then(|response| {
        response
          .bytes()
          .with_context(|| format!("failed to read {label} `{url}`"))
      }) {
      Ok(body) => return Ok(body.to_vec()),
      Err(error) if attempt < HELM_DOWNLOAD_ATTEMPTS => {
        warn!(
          attempt,
          attempts = HELM_DOWNLOAD_ATTEMPTS,
          url,
          error = %error,
          "helm download attempt failed; retrying"
        );
        thread::sleep(Duration::from_secs(attempt as u64));
      }
      Err(error) => return Err(error),
    }
  }

  unreachable!("download loop should return or error on the last attempt")
}

fn parse_helm_version_manifest(contents: &str) -> Result<String> {
  let version = contents
    .lines()
    .map(str::trim)
    .find(|line| !line.is_empty())
    .ok_or_else(|| anyhow!("the Helm version manifest is empty"))?;
  let normalized = version.trim_start_matches('v');
  Version::parse(normalized).with_context(|| {
    format!("failed to parse Helm version `{version}` from the version manifest")
  })?;

  Ok(version.to_string())
}

fn parse_helm_archive_checksum(contents: &str, archive_name: &str) -> Result<String> {
  let line = contents
    .lines()
    .map(str::trim)
    .find(|line| !line.is_empty())
    .ok_or_else(|| anyhow!("the Helm checksum file for `{archive_name}` is empty"))?;

  if line.len() == 64 && line.chars().all(|ch| ch.is_ascii_hexdigit()) {
    return Ok(line.to_ascii_lowercase());
  }

  let mut parts = line.splitn(2, char::is_whitespace);
  let checksum = parts
    .next()
    .ok_or_else(|| anyhow!("the Helm checksum file for `{archive_name}` is malformed"))?;
  let file_name = parts
    .next()
    .ok_or_else(|| anyhow!("the Helm checksum file for `{archive_name}` is missing the file name"))?
    .trim()
    .trim_start_matches('*');
  let referenced_name = file_name.rsplit(['/', '\\']).next().unwrap_or(file_name);

  if referenced_name != archive_name {
    bail!("the Helm checksum file references `{file_name}` instead of `{archive_name}`");
  }
  if checksum.len() != 64 || !checksum.chars().all(|ch| ch.is_ascii_hexdigit()) {
    bail!("the Helm checksum file for `{archive_name}` does not contain a valid sha256 digest");
  }

  Ok(checksum.to_ascii_lowercase())
}

fn helm_target_triplet() -> Result<(&'static str, &'static str)> {
  helm_target_triplet_for(env::consts::OS, env::consts::ARCH)
}

fn helm_target_triplet_for(os: &str, arch: &str) -> Result<(&'static str, &'static str)> {
  let os = match os {
    "linux" => "linux",
    "macos" => "darwin",
    other => bail!("Helm does not publish prebuilt binaries for `{other}`"),
  };
  let arch = match arch {
    "x86_64" => "amd64",
    "x86" | "i386" | "i586" | "i686" => "386",
    "arm" => "arm",
    "aarch64" => "arm64",
    "loongarch64" => "loong64",
    "powerpc64le" => "ppc64le",
    "s390x" => "s390x",
    "riscv64" | "riscv64gc" => "riscv64",
    other => bail!("Helm does not publish prebuilt binaries for architecture `{other}`"),
  };

  Ok((os, arch))
}

fn helm_user_agent() -> String {
  format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests {
  use super::{helm_target_triplet_for, parse_helm_archive_checksum, parse_helm_version_manifest};

  #[test]
  fn parses_helm_version_manifest() {
    let version = parse_helm_version_manifest("v3.18.6\n").expect("version should parse");

    assert_eq!(version, "v3.18.6");
  }

  #[test]
  fn parses_raw_helm_checksum() {
    let checksum = parse_helm_archive_checksum(
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
      "helm-v3.18.6-linux-amd64.tar.gz",
    )
    .expect("raw checksum should parse");

    assert_eq!(
      checksum,
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
  }

  #[test]
  fn parses_checksum_with_path_prefixed_archive_name() {
    let checksum = parse_helm_archive_checksum(
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef  /tmp/helm-v3.18.6-linux-amd64.tar.gz\n",
      "helm-v3.18.6-linux-amd64.tar.gz",
    )
    .expect("checksum should parse");

    assert_eq!(
      checksum,
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
  }

  #[test]
  fn maps_linux_x86_64_to_supported_helm_triplet() {
    let triplet = helm_target_triplet_for("linux", "x86_64").expect("triplet should map");

    assert_eq!(triplet, ("linux", "amd64"));
  }
}
