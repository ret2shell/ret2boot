use std::{
  env,
  fs::{self, File},
  io,
  path::PathBuf,
  time::Duration,
};

use anyhow::{Context, Result, anyhow};
use reqwest::{
  StatusCode,
  blocking::Client,
  header::{ACCEPT, USER_AGENT},
};
use semver::Version;
use serde::Deserialize;
use tracing::{debug, info, warn};

pub const DEFAULT_RELEASE_SOURCE_NAME: &str = "github";
pub const DEFAULT_REPOSITORY_URL: &str = "https://github.com/ret2shell/ret2boot";

pub enum UpdateCheckResult {
  UpToDate,
  NoPublishedRelease,
  Downloaded {
    version: String,
    path: PathBuf,
    reused: bool,
  },
  UpdateAvailableNoAsset {
    source: String,
    version: String,
    release_url: String,
  },
  DownloadFailed {
    source: String,
    version: String,
    release_url: String,
  },
  Unavailable {
    source: String,
    repository_url: String,
  },
}

pub fn check_for_updates() -> UpdateCheckResult {
  match UpdateManager::new_default().and_then(|manager| manager.check_for_updates()) {
    Ok(report) => report,
    Err(error) => {
      warn!(error = %error, "update subsystem failed before release check completed");
      UpdateCheckResult::Unavailable {
        source: DEFAULT_RELEASE_SOURCE_NAME.to_string(),
        repository_url: DEFAULT_REPOSITORY_URL.to_string(),
      }
    }
  }
}

struct UpdateManager {
  client: Client,
  current_version: Version,
  cache_dir: PathBuf,
  selector: AssetSelector,
  sources: Vec<Box<dyn ReleaseSource>>,
}

impl UpdateManager {
  fn new_default() -> Result<Self> {
    let client = Client::builder()
      .https_only(true)
      .timeout(Duration::from_secs(10))
      .build()
      .context("failed to build update HTTP client")?;

    Ok(Self {
      client,
      current_version: Version::parse(env!("CARGO_PKG_VERSION"))
        .context("failed to parse current package version")?,
      cache_dir: cache_dir()?,
      selector: AssetSelector::current(),
      sources: vec![Box::new(GitHubReleaseSource::ret2boot())],
    })
  }

  fn check_for_updates(&self) -> Result<UpdateCheckResult> {
    let mut last_unavailable = None;

    for source in &self.sources {
      match source.fetch_latest_release(&self.client) {
        Ok(None) => {
          info!(source = source.name(), "no published releases found");
          continue;
        }
        Err(error) => {
          warn!(
            source = source.name(),
            repository = source.repository_url(),
            error = %error,
            "failed to query release source"
          );
          last_unavailable = Some(UpdateCheckResult::Unavailable {
            source: source.name().to_string(),
            repository_url: source.repository_url().to_string(),
          });
          continue;
        }
        Ok(Some(release)) if release.version <= self.current_version => {
          info!(
            source = source.name(),
            current_version = %self.current_version,
            latest_version = %release.version,
            "installer is already up to date"
          );
          return Ok(UpdateCheckResult::UpToDate);
        }
        Ok(Some(release)) => {
          info!(
            source = source.name(),
            current_version = %self.current_version,
            latest_version = %release.version,
            "new installer release is available"
          );

          let Some(asset) = self.selector.select(&release.assets) else {
            warn!(
              source = source.name(),
              latest_version = %release.version,
              target_os = self.selector.os,
              target_arch = self.selector.arch,
              "no compatible release asset found"
            );

            return Ok(UpdateCheckResult::UpdateAvailableNoAsset {
              source: source.name().to_string(),
              version: release.version_text,
              release_url: release.html_url,
            });
          };

          match self.download_asset(&release, asset) {
            Ok(download) => {
              return Ok(UpdateCheckResult::Downloaded {
                version: release.version_text,
                path: download.path,
                reused: download.reused,
              });
            }
            Err(error) => {
              warn!(
                source = source.name(),
                latest_version = %release.version,
                asset = %asset.name,
                error = %error,
                "failed to download installer update"
              );

              return Ok(UpdateCheckResult::DownloadFailed {
                source: source.name().to_string(),
                version: release.version_text,
                release_url: release.html_url,
              });
            }
          }
        }
      }
    }

    Ok(last_unavailable.unwrap_or(UpdateCheckResult::NoPublishedRelease))
  }

  fn download_asset(
    &self, release: &RemoteRelease, asset: &RemoteAsset,
  ) -> Result<DownloadedAsset> {
    let release_dir = self.cache_dir.join("releases").join(&release.version_text);
    fs::create_dir_all(&release_dir)
      .with_context(|| format!("failed to create update cache `{}`", release_dir.display()))?;

    let final_path = release_dir.join(&asset.name);
    if final_path.is_file() {
      debug!(path = %final_path.display(), "reusing cached installer update");
      return Ok(DownloadedAsset {
        path: final_path,
        reused: true,
      });
    }

    let temp_path = final_path.with_extension("download");
    let mut response = self
      .client
      .get(&asset.download_url)
      .header(USER_AGENT, user_agent())
      .send()
      .with_context(|| format!("failed to request update asset `{}`", asset.download_url))?
      .error_for_status()
      .with_context(|| format!("update asset `{}` returned an error status", asset.name))?;

    let mut file = File::create(&temp_path)
      .with_context(|| format!("failed to create `{}`", temp_path.display()))?;

    io::copy(&mut response, &mut file)
      .with_context(|| format!("failed to write `{}`", temp_path.display()))?;

    fs::rename(&temp_path, &final_path).with_context(|| {
      format!(
        "failed to move downloaded asset into place at `{}`",
        final_path.display()
      )
    })?;

    Ok(DownloadedAsset {
      path: final_path,
      reused: false,
    })
  }
}

struct DownloadedAsset {
  path: PathBuf,
  reused: bool,
}

trait ReleaseSource {
  fn name(&self) -> &'static str;
  fn repository_url(&self) -> &str;
  fn fetch_latest_release(&self, client: &Client) -> Result<Option<RemoteRelease>>;
}

struct RemoteRelease {
  version: Version,
  version_text: String,
  html_url: String,
  assets: Vec<RemoteAsset>,
}

struct RemoteAsset {
  name: String,
  download_url: String,
}

struct GitHubReleaseSource {
  repository_url: String,
  latest_release_api: String,
}

impl GitHubReleaseSource {
  fn ret2boot() -> Self {
    let repository_url = DEFAULT_REPOSITORY_URL.to_string();

    Self {
      latest_release_api: format!(
        "https://api.github.com/repos/{}/{}/releases/latest",
        "ret2shell", "ret2boot"
      ),
      repository_url,
    }
  }
}

impl ReleaseSource for GitHubReleaseSource {
  fn name(&self) -> &'static str {
    "github"
  }

  fn repository_url(&self) -> &str {
    &self.repository_url
  }

  fn fetch_latest_release(&self, client: &Client) -> Result<Option<RemoteRelease>> {
    let response = client
      .get(&self.latest_release_api)
      .header(USER_AGENT, user_agent())
      .header(ACCEPT, "application/vnd.github+json")
      .send()
      .with_context(|| format!("failed to request `{}`", self.latest_release_api))?;

    if response.status() == StatusCode::NOT_FOUND {
      return Ok(None);
    }

    let release: GitHubRelease = response
      .error_for_status()
      .context("github latest release request failed")?
      .json()
      .context("failed to parse github latest release response")?;

    Ok(Some(RemoteRelease {
      version: parse_release_version(&release.tag_name)?,
      version_text: release.tag_name,
      html_url: release.html_url,
      assets: release
        .assets
        .into_iter()
        .map(|asset| RemoteAsset {
          name: asset.name,
          download_url: asset.browser_download_url,
        })
        .collect(),
    }))
  }
}

#[derive(Deserialize)]
struct GitHubRelease {
  tag_name: String,
  html_url: String,
  assets: Vec<GitHubAsset>,
}

#[derive(Deserialize)]
struct GitHubAsset {
  name: String,
  browser_download_url: String,
}

struct AssetSelector {
  binary_name: String,
  os: &'static str,
  arch: &'static str,
  env: Option<&'static str>,
}

impl AssetSelector {
  fn current() -> Self {
    Self {
      binary_name: env!("CARGO_PKG_NAME").to_ascii_lowercase(),
      os: env::consts::OS,
      arch: env::consts::ARCH,
      env: target_env(),
    }
  }

  fn select<'a>(&self, assets: &'a [RemoteAsset]) -> Option<&'a RemoteAsset> {
    let asset_count = assets.len();

    assets
      .iter()
      .filter_map(|asset| {
        let name = asset.name.to_ascii_lowercase();
        let mut score = 0;

        if name.contains(&self.binary_name) {
          score += 8;
        }
        if name.contains(self.os) {
          score += 4;
        }
        if name.contains(self.arch) {
          score += 4;
        }
        if let Some(target_env) = self.env
          && name.contains(target_env)
        {
          score += 2;
        }
        if name.ends_with(".tar.gz") || name.ends_with(".tgz") || name.ends_with(".zip") {
          score += 1;
        }

        if score == 0 {
          None
        } else {
          Some((score, asset))
        }
      })
      .max_by_key(|(score, _)| *score)
      .and_then(|(score, asset)| {
        if score >= 12 || (asset_count == 1 && score >= 8) {
          Some(asset)
        } else {
          None
        }
      })
  }
}

fn parse_release_version(raw: &str) -> Result<Version> {
  let normalized = raw
    .trim()
    .trim_start_matches("refs/tags/")
    .trim_start_matches('v');

  Version::parse(normalized)
    .with_context(|| format!("failed to parse release version from tag `{raw}`"))
}

fn user_agent() -> String {
  format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}

fn cache_dir() -> Result<PathBuf> {
  if is_root_user() {
    return Ok(PathBuf::from("/var/cache/ret2shell/ret2boot"));
  }

  if let Some(path) = xdg_cache_home() {
    return Ok(path.join("ret2shell").join("ret2boot"));
  }

  let home = env::var_os("HOME")
    .filter(|value| !value.is_empty())
    .map(PathBuf::from)
    .ok_or_else(|| anyhow!("HOME is not set and XDG_CACHE_HOME is unavailable"))?;

  Ok(home.join(".cache").join("ret2shell").join("ret2boot"))
}

fn xdg_cache_home() -> Option<PathBuf> {
  let path = PathBuf::from(env::var_os("XDG_CACHE_HOME")?);

  if path.as_os_str().is_empty() || !path.is_absolute() {
    return None;
  }

  Some(path)
}

fn is_root_user() -> bool {
  unsafe { libc::geteuid() == 0 }
}

fn target_env() -> Option<&'static str> {
  if cfg!(target_env = "gnu") {
    Some("gnu")
  } else if cfg!(target_env = "musl") {
    Some("musl")
  } else if cfg!(target_env = "msvc") {
    Some("msvc")
  } else {
    None
  }
}
