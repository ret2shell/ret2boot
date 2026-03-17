use std::{
  collections::BTreeSet,
  env,
  ffi::CString,
  fs,
  mem::MaybeUninit,
  path::{Path, PathBuf},
  process::Command,
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use reqwest::blocking::Client;

use super::context::StepExecutionContext;

pub(crate) fn stage_remote_script(url: &str, prefix: &str) -> Result<PathBuf> {
  let client = Client::builder()
    .https_only(true)
    .timeout(Duration::from_secs(30))
    .build()
    .context("failed to build install script HTTP client")?;

  let script = client
    .get(url)
    .send()
    .with_context(|| format!("failed to request install script `{url}`"))?
    .error_for_status()
    .with_context(|| format!("install script source `{url}` returned an error status"))?
    .text()
    .with_context(|| format!("failed to read install script `{url}`"))?;

  let path = unique_temp_path(prefix, "sh");
  fs::write(&path, script).with_context(|| format!("failed to write `{}`", path.display()))?;
  Ok(path)
}

pub(crate) fn run_staged_script(
  ctx: &StepExecutionContext<'_>, script_path: &Path, envs: &[(String, String)],
) -> Result<()> {
  let (program, args) = staged_script_invocation(script_path)?;
  ctx.run_privileged_command(&program, &args, envs)
}

pub(crate) fn stage_text_file(prefix: &str, extension: &str, contents: String) -> Result<PathBuf> {
  let path = unique_temp_path(prefix, extension);
  fs::write(&path, contents).with_context(|| format!("failed to write `{}`", path.display()))?;
  Ok(path)
}

pub(crate) fn unique_temp_path(prefix: &str, extension: &str) -> PathBuf {
  let stamp = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|duration| duration.as_nanos())
    .unwrap_or_default();

  env::temp_dir().join(format!("{prefix}-{stamp}.{}", extension))
}

fn staged_script_invocation(script_path: &Path) -> Result<(String, Vec<String>)> {
  let contents = fs::read_to_string(script_path)
    .with_context(|| format!("failed to inspect staged script `{}`", script_path.display()))?;

  Ok(script_invocation_from_contents(script_path, &contents))
}

fn script_invocation_from_contents(script_path: &Path, contents: &str) -> (String, Vec<String>) {
  let script_arg = script_path.display().to_string();

  if let Some((program, mut args)) = contents.lines().next().and_then(parse_shebang) {
    args.push(script_arg);
    return (program, args);
  }

  ("sh".to_string(), vec![script_arg])
}

fn parse_shebang(line: &str) -> Option<(String, Vec<String>)> {
  let interpreter = line
    .trim_start_matches('\u{feff}')
    .strip_prefix("#!")?
    .trim();
  let mut parts = interpreter.split_whitespace();
  let program = parts.next()?.to_string();
  let args = parts.map(str::to_string).collect();

  Some((program, args))
}

pub(crate) fn install_staged_file(
  ctx: &StepExecutionContext<'_>, source: &Path, dest: &str,
) -> Result<()> {
  ctx.run_privileged_command(
    "install",
    &[
      "-D".to_string(),
      "-m".to_string(),
      "600".to_string(),
      source.display().to_string(),
      dest.to_string(),
    ],
    &[],
  )
}

pub(crate) fn install_directory(ctx: &StepExecutionContext<'_>, path: &str) -> Result<()> {
  ctx.run_privileged_command(
    "install",
    &[
      "-d".to_string(),
      "-m".to_string(),
      "755".to_string(),
      path.to_string(),
    ],
    &[],
  )
}

pub(crate) fn ensure_symlink(
  ctx: &StepExecutionContext<'_>, source: &str, target: &str,
) -> Result<()> {
  ctx.run_privileged_command(
    "ln",
    &["-sfn".to_string(), source.to_string(), target.to_string()],
    &[],
  )
}

pub(crate) fn find_existing_path(candidates: &[PathBuf]) -> Option<PathBuf> {
  candidates.iter().find(|path| path.is_file()).cloned()
}

pub(crate) fn command_exists(binary: &str) -> bool {
  find_command_path(binary).is_some()
}

pub(crate) fn find_command_path(binary: &str) -> Option<PathBuf> {
  env::var_os("PATH").and_then(|paths| {
    env::split_paths(&paths).find_map(|dir| {
      let candidate = dir.join(binary);
      candidate.is_file().then_some(candidate)
    })
  })
}

pub(crate) fn detect_nginx_binary_path() -> Option<PathBuf> {
  find_existing_path(&[
    PathBuf::from("/usr/sbin/nginx"),
    PathBuf::from("/usr/bin/nginx"),
    PathBuf::from("/sbin/nginx"),
    PathBuf::from("/bin/nginx"),
  ])
  .or_else(|| find_command_path("nginx"))
}

pub(crate) fn nginx_service_exists() -> bool {
  command_exists("systemctl")
    && Command::new("systemctl")
      .args(["cat", "nginx.service"])
      .status()
      .map(|status| status.success())
      .unwrap_or(false)
}

pub(crate) fn disk_free_bytes(path: &str) -> Result<u64> {
  let path = CString::new(path).context("invalid disk path")?;
  let mut stat = MaybeUninit::<libc::statvfs>::uninit();

  let result = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
  if result != 0 {
    return Err(anyhow!(
      "failed to stat filesystem for `{}`",
      path.to_string_lossy()
    ));
  }

  let stat = unsafe { stat.assume_init() };
  Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

pub(crate) fn memory_total_bytes() -> Result<u64> {
  let contents = fs::read_to_string("/proc/meminfo").context("failed to read /proc/meminfo")?;

  let kibibytes = contents
    .lines()
    .find_map(|line| {
      let value = line.strip_prefix("MemTotal:")?.trim();
      value.split_whitespace().next()?.parse::<u64>().ok()
    })
    .ok_or_else(|| anyhow!("unable to parse MemTotal from /proc/meminfo"))?;

  Ok(kibibytes.saturating_mul(1024))
}

pub(crate) fn listening_tcp_ports() -> BTreeSet<u16> {
  ["/proc/net/tcp", "/proc/net/tcp6"]
    .into_iter()
    .filter_map(|path| fs::read_to_string(path).ok())
    .flat_map(|contents| {
      contents
        .lines()
        .skip(1)
        .filter_map(|line| {
          let columns = line.split_whitespace().collect::<Vec<_>>();
          if columns.get(3).copied() != Some("0A") {
            return None;
          }

          let port = columns.get(1)?.split(':').nth(1)?;
          u16::from_str_radix(port, 16).ok()
        })
        .collect::<Vec<_>>()
    })
    .collect()
}

pub(crate) fn format_ports(ports: &[u16]) -> String {
  ports
    .iter()
    .map(u16::to_string)
    .collect::<Vec<_>>()
    .join(", ")
}

pub(crate) fn format_gib(bytes: u64) -> String {
  format!("{:.1} GiB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
}

pub(crate) fn cgroup_memory_available() -> bool {
  if let Ok(controllers) = fs::read_to_string("/sys/fs/cgroup/cgroup.controllers")
    && controllers
      .split_whitespace()
      .any(|controller| controller == "memory")
  {
    return true;
  }

  fs::read_to_string("/proc/cgroups")
    .ok()
    .map(|contents| {
      contents.lines().any(|line| {
        let columns = line.split_whitespace().collect::<Vec<_>>();
        columns.len() >= 4 && columns[0] == "memory" && columns[3] == "1"
      })
    })
    .unwrap_or(false)
}

pub(crate) fn file_contains(path: &str, needle: &str) -> bool {
  fs::read_to_string(path)
    .map(|contents| contents.contains(needle))
    .unwrap_or(false)
}

pub(crate) fn modprobe_can_load(module_name: &str) -> bool {
  command_exists("modprobe")
    && Command::new("modprobe")
      .args(["-n", "-q", module_name])
      .status()
      .map(|status| status.success())
      .unwrap_or(false)
}

pub(crate) fn yaml_quote(value: &str) -> String {
  format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
  use std::path::Path;

  use super::script_invocation_from_contents;

  #[test]
  fn uses_script_shebang_when_present() {
    let script_path = Path::new("staged-script.sh");
    let (program, args) =
      script_invocation_from_contents(script_path, "#!/usr/bin/env bash\nprintf 'ok'\n");

    assert_eq!(program, "/usr/bin/env");
    assert_eq!(args, vec!["bash".to_string(), "staged-script.sh".to_string()]);
  }

  #[test]
  fn falls_back_to_sh_when_shebang_is_missing() {
    let script_path = Path::new("staged-script.sh");
    let (program, args) = script_invocation_from_contents(script_path, "printf 'ok'\n");

    assert_eq!(program, "sh");
    assert_eq!(args, vec!["staged-script.sh".to_string()]);
  }
}
