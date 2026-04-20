use std::{
  io::ErrorKind,
  process::{Command, Output},
  sync::mpsc::{self, Sender},
  thread::{self, JoinHandle},
  time::Duration,
};

use anyhow::{Context, Result, bail};
use tracing::{debug, warn};

const SUDO_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(240);

pub struct PrivilegeSession {
  kind: PrivilegeKind,
}

enum PrivilegeKind {
  Root,
  Sudo { _keepalive: SudoKeepalive },
  Doas,
  Su,
}

struct SudoKeepalive {
  stop_tx: Sender<()>,
  handle: Option<JoinHandle<()>>,
}

impl PrivilegeSession {
  pub fn acquire() -> Result<Self> {
    if Self::is_root_user() {
      return Ok(Self {
        kind: PrivilegeKind::Root,
      });
    }

    if let Some(session) = try_sudo()? {
      return Ok(session);
    }

    if let Some(session) = try_doas()? {
      return Ok(session);
    }

    if let Some(session) = try_su()? {
      return Ok(session);
    }

    bail!("failed to acquire root privileges using sudo, doas, or su")
  }

  pub fn backend_name(&self) -> &'static str {
    match self.kind {
      PrivilegeKind::Root => "root",
      PrivilegeKind::Sudo { .. } => "sudo",
      PrivilegeKind::Doas => "doas",
      PrivilegeKind::Su => "su",
    }
  }

  pub fn is_root_user() -> bool {
    #[cfg(unix)]
    {
      unsafe { libc::geteuid() == 0 }
    }

    #[cfg(not(unix))]
    {
      false
    }
  }

  pub fn run_command(
    &self, program: &str, args: &[String], envs: &[(String, String)],
  ) -> Result<()> {
    self.execute_command(program, args, envs).map(|_| ())
  }

  pub fn run_command_capture(
    &self, program: &str, args: &[String], envs: &[(String, String)],
  ) -> Result<String> {
    let output = self.execute_command(program, args, envs)?;

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
  }

  fn execute_command(
    &self, program: &str, args: &[String], envs: &[(String, String)],
  ) -> Result<Output> {
    let mut command = self.command_for(program, args, envs);
    let output = command.output().with_context(|| {
      format!(
        "failed to execute privileged command `{}` via {}",
        program,
        self.backend_name()
      )
    })?;

    if output.status.success() {
      return Ok(output);
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() {
      stderr
    } else if !stdout.is_empty() {
      stdout
    } else {
      "no output".to_string()
    };

    bail!(
      "privileged command `{}` via {} exited with status {:?}: {}",
      program,
      self.backend_name(),
      output.status.code(),
      detail
    )
  }

  fn command_for(&self, program: &str, args: &[String], envs: &[(String, String)]) -> Command {
    match self.kind {
      PrivilegeKind::Root => {
        let mut command = Command::new(program);
        command.args(args);
        command.envs(envs.iter().map(|(key, value)| (key, value)));
        command
      }
      PrivilegeKind::Sudo { .. } => env_wrapped_command("sudo", program, args, envs),
      PrivilegeKind::Doas => env_wrapped_command("doas", program, args, envs),
      PrivilegeKind::Su => {
        let mut command = Command::new("su");
        command
          .arg("root")
          .arg("-c")
          .arg(shell_command(program, args, envs));
        command
      }
    }
  }
}

impl Drop for SudoKeepalive {
  fn drop(&mut self) {
    let _ = self.stop_tx.send(());

    if let Some(handle) = self.handle.take() {
      let _ = handle.join();
    }
  }
}

fn try_sudo() -> Result<Option<PrivilegeSession>> {
  let status = match run_status("sudo", &["-v"]) {
    AttemptStatus::Success(status) => status,
    AttemptStatus::Unavailable => return Ok(None),
    AttemptStatus::Failed(status) => {
      debug!(code = ?status.code(), "sudo privilege validation failed");
      return Ok(None);
    }
  };

  debug!(code = ?status.code(), "acquired deployment privileges via sudo");

  let (stop_tx, stop_rx) = mpsc::channel();
  let handle = thread::Builder::new()
    .name("ret2boot-sudo-keepalive".to_string())
    .spawn(move || {
      loop {
        match stop_rx.recv_timeout(SUDO_KEEPALIVE_INTERVAL) {
          Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
          Err(mpsc::RecvTimeoutError::Timeout) => {
            if !refresh_sudo_credentials() {
              break;
            }
          }
        }
      }
    })
    .context("failed to spawn sudo keepalive thread")?;

  Ok(Some(PrivilegeSession {
    kind: PrivilegeKind::Sudo {
      _keepalive: SudoKeepalive {
        stop_tx,
        handle: Some(handle),
      },
    },
  }))
}

fn try_doas() -> Result<Option<PrivilegeSession>> {
  match run_status("doas", &["-u", "root", "true"]) {
    AttemptStatus::Success(status) => {
      debug!(code = ?status.code(), "acquired deployment privileges via doas");
      Ok(Some(PrivilegeSession {
        kind: PrivilegeKind::Doas,
      }))
    }
    AttemptStatus::Unavailable => Ok(None),
    AttemptStatus::Failed(status) => {
      debug!(code = ?status.code(), "doas privilege validation failed");
      Ok(None)
    }
  }
}

fn try_su() -> Result<Option<PrivilegeSession>> {
  match run_status("su", &["root", "-c", "true"]) {
    AttemptStatus::Success(status) => {
      debug!(code = ?status.code(), "acquired deployment privileges via su");
      Ok(Some(PrivilegeSession {
        kind: PrivilegeKind::Su,
      }))
    }
    AttemptStatus::Unavailable => Ok(None),
    AttemptStatus::Failed(status) => {
      debug!(code = ?status.code(), "su privilege validation failed");
      Ok(None)
    }
  }
}

enum AttemptStatus {
  Success(std::process::ExitStatus),
  Failed(std::process::ExitStatus),
  Unavailable,
}

fn run_status(program: &str, args: &[&str]) -> AttemptStatus {
  match Command::new(program).args(args).status() {
    Ok(status) if status.success() => AttemptStatus::Success(status),
    Ok(status) => AttemptStatus::Failed(status),
    Err(error) if error.kind() == ErrorKind::NotFound => AttemptStatus::Unavailable,
    Err(error) => {
      debug!(program, error = %error, "failed to invoke privilege tool");
      AttemptStatus::Unavailable
    }
  }
}

fn refresh_sudo_credentials() -> bool {
  match Command::new("sudo").args(["-n", "-v"]).status() {
    Ok(status) if status.success() => true,
    Ok(status) => {
      warn!(code = ?status.code(), "lost cached sudo credentials");
      false
    }
    Err(error) => {
      warn!(error = %error, "failed to refresh sudo credentials");
      false
    }
  }
}

fn env_wrapped_command(
  launcher: &str, program: &str, args: &[String], envs: &[(String, String)],
) -> Command {
  let mut command = Command::new(launcher);
  command.arg("env");

  for (key, value) in envs {
    command.arg(format!("{key}={value}"));
  }

  command.arg(program);
  command.args(args);
  command
}

fn shell_command(program: &str, args: &[String], envs: &[(String, String)]) -> String {
  let mut parts: Vec<String> = envs
    .iter()
    .map(|(key, value)| format!("{}={}", key, shell_quote(value)))
    .collect();

  parts.push(shell_quote(program));
  parts.extend(args.iter().map(|arg| shell_quote(arg)));
  parts.join(" ")
}

fn shell_quote(value: &str) -> String {
  format!("'{}'", value.replace('\'', "'\"'\"'"))
}
