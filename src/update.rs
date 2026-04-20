use std::{
  env,
  fs::{self, File},
  io::{self, Read},
  path::{Path, PathBuf},
  process::Command,
  time::Duration,
};

use anyhow::{Context, Result, anyhow};
use reqwest::{
  StatusCode,
  blocking::Client,
  header::{ACCEPT, AUTHORIZATION, USER_AGENT},
};
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

pub const DEFAULT_RELEASE_SOURCE_NAME: &str = "github";
pub const DEFAULT_REPOSITORY_URL: &str = "https://github.com/ret2shell/ret2boot";
pub const RET2SHELL_REPOSITORY_URL: &str = "https://github.com/ret2shell/ret2shell";
const GITHUB_HOSTNAME: &str = "github.com";
const GITHUB_JSON_MEDIA_TYPE: &str = "application/vnd.github+json";
const GITHUB_ASSET_MEDIA_TYPE: &str = "application/octet-stream";

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

pub struct Ret2ShellChartDownload {
  pub version: String,
  pub path: PathBuf,
  pub download_url: String,
  pub release_url: String,
}

pub fn download_ret2shell_chart() -> Result<Ret2ShellChartDownload> {
  Ret2ShellChartManager::new_default()?.download_latest_chart()
}

pub fn cache_dir_path() -> Result<PathBuf> {
  cache_dir()
}

pub fn system_cache_dir_path() -> PathBuf {
  PathBuf::from("/var/cache/ret2shell/ret2boot")
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
      sources: vec![Box::new(GitHubReleaseSource::ret2boot(
        GitHubAuth::from_gh(GITHUB_HOSTNAME)?,
      ))],
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
    let mut response = request_remote_asset(&self.client, asset)?
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

struct Ret2ShellChartManager {
  client: Client,
  cache_dir: PathBuf,
  source: GitHubReleaseSource,
}

impl Ret2ShellChartManager {
  fn new_default() -> Result<Self> {
    Ok(Self {
      client: Client::builder()
        .https_only(true)
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to build ret2shell chart HTTP client")?,
      cache_dir: cache_dir()?,
      source: GitHubReleaseSource::ret2shell(GitHubAuth::from_gh(GITHUB_HOSTNAME)?),
    })
  }

  fn download_latest_chart(&self) -> Result<Ret2ShellChartDownload> {
    let release = self
      .source
      .fetch_latest_release(&self.client)?
      .ok_or_else(|| anyhow!("no published ret2shell release chart is available"))?;
    let version = release.version.to_string();
    let chart_name = format!("ret2shell-{version}.tgz");
    let checksum_name = format!("{chart_name}.sha256");
    let chart_asset = release
      .asset_named(&chart_name)
      .ok_or_else(|| anyhow!("latest ret2shell release is missing chart asset `{chart_name}`"))?;
    let checksum_asset = release.asset_named(&checksum_name).ok_or_else(|| {
      anyhow!("latest ret2shell release is missing checksum asset `{checksum_name}`")
    })?;
    let release_dir = self
      .cache_dir
      .join("charts")
      .join("ret2shell")
      .join(&version);
    fs::create_dir_all(&release_dir).with_context(|| {
      format!(
        "failed to create ret2shell chart cache `{}`",
        release_dir.display()
      )
    })?;

    let chart_path = release_dir.join(&chart_name);
    let checksum_path = release_dir.join(&checksum_name);
    let checksum = download_text_asset(&self.client, checksum_asset)?;
    fs::write(&checksum_path, &checksum)
      .with_context(|| format!("failed to write `{}`", checksum_path.display()))?;
    let expected_checksum = parse_sha256sum_line(&checksum, &chart_name)?;

    if chart_path.is_file() {
      if compute_sha256(&chart_path)? == expected_checksum {
        return Ok(Ret2ShellChartDownload {
          version,
          path: chart_path,
          download_url: chart_asset.download_url.clone(),
          release_url: release.html_url,
        });
      }

      fs::remove_file(&chart_path).with_context(|| {
        format!(
          "failed to remove stale ret2shell chart cache `{}`",
          chart_path.display()
        )
      })?;
    }

    let temp_path = chart_path.with_extension("tgz.download");
    download_asset_to_path(&self.client, chart_asset, &temp_path)?;

    let actual_checksum = compute_sha256(&temp_path)?;
    if actual_checksum != expected_checksum {
      let _ = fs::remove_file(&temp_path);
      return Err(anyhow!(
        "downloaded ret2shell chart checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
      ));
    }

    fs::rename(&temp_path, &chart_path).with_context(|| {
      format!(
        "failed to move downloaded chart into place at `{}`",
        chart_path.display()
      )
    })?;

    Ok(Ret2ShellChartDownload {
      version,
      path: chart_path,
      download_url: chart_asset.download_url.clone(),
      release_url: release.html_url,
    })
  }
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

impl RemoteRelease {
  fn asset_named(&self, name: &str) -> Option<&RemoteAsset> {
    self.assets.iter().find(|asset| asset.name == name)
  }
}

struct RemoteAsset {
  name: String,
  download_url: String,
  request_url: String,
  auth: RequestAuth,
  accept: RemoteAssetAccept,
}

struct GitHubReleaseSource {
  repository_url: String,
  latest_release_api: String,
  auth: GitHubAuth,
}

impl GitHubReleaseSource {
  fn ret2boot(auth: GitHubAuth) -> Self {
    Self::new("ret2shell", "ret2boot", DEFAULT_REPOSITORY_URL, auth)
  }

  fn ret2shell(auth: GitHubAuth) -> Self {
    Self::new("ret2shell", "ret2shell", RET2SHELL_REPOSITORY_URL, auth)
  }

  fn new(owner: &str, repo: &str, repository_url: &str, auth: GitHubAuth) -> Self {
    Self {
      latest_release_api: format!("https://api.github.com/repos/{owner}/{repo}/releases/latest"),
      repository_url: repository_url.to_string(),
      auth,
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
    let response = self
      .auth
      .apply(
        client
          .get(&self.latest_release_api)
          .header(USER_AGENT, user_agent())
          .header(ACCEPT, GITHUB_JSON_MEDIA_TYPE),
      )
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
          request_url: asset.url,
          auth: RequestAuth::GitHub(self.auth.clone()),
          accept: RemoteAssetAccept::OctetStream,
        })
        .collect(),
    }))
  }
}

#[derive(Clone)]
struct GitHubAuth {
  token: String,
}

impl GitHubAuth {
  fn from_gh(hostname: &str) -> Result<Self> {
    let output = Command::new("gh")
      .args(["auth", "token", "--hostname", hostname])
      .output()
      .with_context(|| format!("failed to run `gh auth token --hostname {hostname}`"))?;

    if !output.status.success() {
      let stderr = String::from_utf8_lossy(&output.stderr);
      let stderr = stderr.trim();
      let detail = if stderr.is_empty() {
        String::new()
      } else {
        format!(": {stderr}")
      };

      return Err(anyhow!(
        "`gh auth token` did not return a usable token for `{hostname}`{detail}"
      ));
    }

    Ok(Self {
      token: parse_gh_auth_token(&output.stdout)?,
    })
  }

  fn apply(&self, request: reqwest::blocking::RequestBuilder) -> reqwest::blocking::RequestBuilder {
    request.header(AUTHORIZATION, format!("Bearer {}", self.token))
  }
}

#[derive(Clone)]
enum RequestAuth {
  GitHub(GitHubAuth),
}

impl RequestAuth {
  fn apply(&self, request: reqwest::blocking::RequestBuilder) -> reqwest::blocking::RequestBuilder {
    match self {
      Self::GitHub(auth) => auth.apply(request),
    }
  }
}

#[derive(Clone, Copy)]
enum RemoteAssetAccept {
  OctetStream,
}

impl RemoteAssetAccept {
  fn apply(self, request: reqwest::blocking::RequestBuilder) -> reqwest::blocking::RequestBuilder {
    match self {
      Self::OctetStream => request.header(ACCEPT, GITHUB_ASSET_MEDIA_TYPE),
    }
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
  url: String,
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

fn download_text_asset(client: &Client, asset: &RemoteAsset) -> Result<String> {
  request_remote_asset(client, asset)?
    .error_for_status()
    .with_context(|| format!("release asset `{}` returned an error status", asset.name))?
    .text()
    .with_context(|| format!("failed to read release asset `{}`", asset.name))
}

fn download_asset_to_path(client: &Client, asset: &RemoteAsset, path: &Path) -> Result<()> {
  let mut response = request_remote_asset(client, asset)?
    .error_for_status()
    .with_context(|| format!("release asset `{}` returned an error status", asset.name))?;
  let mut file =
    File::create(path).with_context(|| format!("failed to create `{}`", path.display()))?;

  io::copy(&mut response, &mut file)
    .with_context(|| format!("failed to write `{}`", path.display()))?;

  Ok(())
}

fn request_remote_asset(
  client: &Client, asset: &RemoteAsset,
) -> Result<reqwest::blocking::Response> {
  asset
    .accept
    .apply(
      asset.auth.apply(
        client
          .get(&asset.request_url)
          .header(USER_AGENT, user_agent()),
      ),
    )
    .send()
    .with_context(|| format!("failed to request release asset `{}`", asset.request_url))
}

fn parse_gh_auth_token(stdout: &[u8]) -> Result<String> {
  let token = std::str::from_utf8(stdout)
    .context("`gh auth token` returned non-utf8 output")?
    .trim();

  if token.is_empty() {
    return Err(anyhow!("`gh auth token` returned an empty token"));
  }

  Ok(token.to_string())
}

fn parse_sha256sum_line(contents: &str, expected_name: &str) -> Result<String> {
  let line = contents
    .lines()
    .find(|line| !line.trim().is_empty())
    .ok_or_else(|| anyhow!("checksum asset for `{expected_name}` is empty"))?;
  let mut parts = line.splitn(2, char::is_whitespace);
  let checksum = parts
    .next()
    .ok_or_else(|| anyhow!("checksum asset for `{expected_name}` is malformed"))?;
  let file_name = parts
    .next()
    .ok_or_else(|| anyhow!("checksum asset for `{expected_name}` is missing the file name"))?
    .trim()
    .trim_start_matches('*');
  let referenced_name = checksum_entry_base_name(file_name);

  if referenced_name != expected_name {
    return Err(anyhow!(
      "checksum asset references `{file_name}` instead of `{expected_name}`"
    ));
  }

  if checksum.len() != 64 || !checksum.chars().all(|ch| ch.is_ascii_hexdigit()) {
    return Err(anyhow!(
      "checksum asset for `{expected_name}` does not contain a valid sha256 digest"
    ));
  }

  Ok(checksum.to_ascii_lowercase())
}

fn checksum_entry_base_name(path: &str) -> &str {
  path.rsplit(['/', '\\']).next().unwrap_or(path)
}

fn compute_sha256(path: &Path) -> Result<String> {
  let mut file =
    File::open(path).with_context(|| format!("failed to open `{}`", path.display()))?;
  let mut digest = Sha256::new();
  let mut buffer = [0_u8; 8192];

  loop {
    let read = file
      .read(&mut buffer)
      .with_context(|| format!("failed to read `{}`", path.display()))?;

    if read == 0 {
      break;
    }

    digest.update(&buffer[..read]);
  }

  Ok(format!("{:x}", digest.finalize()))
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
  #[cfg(unix)]
  {
    unsafe { libc::geteuid() == 0 }
  }

  #[cfg(not(unix))]
  {
    false
  }
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

#[cfg(test)]
mod tests {
  use super::{checksum_entry_base_name, parse_gh_auth_token, parse_sha256sum_line};

  #[test]
  fn parses_gh_auth_token_output() {
    let token = parse_gh_auth_token(b"gho_example_token\r\n").expect("token should parse");

    assert_eq!(token, "gho_example_token");
  }

  #[test]
  fn rejects_empty_gh_auth_token_output() {
    let error = parse_gh_auth_token(b" \n").expect_err("empty `gh auth token` output should fail");

    assert!(error.to_string().contains("returned an empty token"));
  }

  #[test]
  fn parses_standard_sha256sum_output() {
    let checksum = parse_sha256sum_line(
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef  ret2shell-3.10.4.tgz\n",
      "ret2shell-3.10.4.tgz",
    )
    .expect("checksum should parse");

    assert_eq!(
      checksum,
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
  }

  #[test]
  fn parses_sha256sum_output_with_path_prefix() {
    let checksum = parse_sha256sum_line(
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef  /home/runner1/_work/_temp/helm-dist/ret2shell-3.10.4.tgz\n",
      "ret2shell-3.10.4.tgz",
    )
    .expect("checksum should parse when the checksum file includes a path prefix");

    assert_eq!(
      checksum,
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
  }

  #[test]
  fn parses_binary_sha256sum_output_with_windows_path_prefix() {
    let checksum = parse_sha256sum_line(
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef *C:\\artifacts\\ret2shell-3.10.4.tgz\n",
      "ret2shell-3.10.4.tgz",
    )
    .expect("checksum should parse when the checksum file includes a binary-mode path");

    assert_eq!(
      checksum,
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
  }

  #[test]
  fn extracts_checksum_entry_base_name_from_either_path_separator() {
    assert_eq!(
      checksum_entry_base_name("/home/runner1/_work/ret2shell-3.10.4.tgz"),
      "ret2shell-3.10.4.tgz"
    );
    assert_eq!(
      checksum_entry_base_name("C:\\artifacts\\ret2shell-3.10.4.tgz"),
      "ret2shell-3.10.4.tgz"
    );
  }

  #[test]
  fn rejects_checksum_for_unexpected_file_name() {
    let error = parse_sha256sum_line(
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef  other.tgz\n",
      "ret2shell-3.10.4.tgz",
    )
    .expect_err("checksum parser should reject mismatched asset names");

    assert!(
      error
        .to_string()
        .contains("checksum asset references `other.tgz` instead of `ret2shell-3.10.4.tgz`")
    );
  }
}
