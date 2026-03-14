use std::{fs, path::Path};

use anyhow::{Result, bail};
use reqwest::blocking::Client;
use rust_i18n::t;

use super::{
  AtomicInstallStep, InstallStepPlan, StepPlanContext, StepPreflightContext, SystemPackageManager,
  support::{
    cgroup_memory_available, command_exists, disk_free_bytes, file_contains, format_gib,
    format_ports, listening_tcp_ports, memory_total_bytes, modprobe_can_load,
  },
};
use crate::{config::InstallStepId, ui};

const PREFLIGHT_MIN_DISK_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const PREFLIGHT_WARN_DISK_FREE_BYTES: u64 = 20 * 1024 * 1024 * 1024;
const PREFLIGHT_MIN_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const PREFLIGHT_WARN_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;

pub struct PreflightValidationStep;

impl AtomicInstallStep for PreflightValidationStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::PreflightValidation
  }

  fn preflight(&self, ctx: &mut StepPreflightContext<'_>) -> Result<bool> {
    let (report, state) = PreflightReport::collect()?;
    *ctx.state_mut() = state;
    report.print();

    if report.has_failures() {
      bail!(t!("install.preflight.summary.failed").to_string())
    }

    Ok(true)
  }

  fn describe(&self, _ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    Ok(InstallStepPlan {
      id: self.id(),
      title: t!("install.steps.preflight").to_string(),
      details: vec![t!("install.steps.preflight_detail").to_string()],
    })
  }

  fn uninstall(&self, _ctx: &mut super::StepExecutionContext<'_>) -> Result<()> {
    Ok(())
  }
}

enum PreflightStatus {
  Passed,
  Warning,
  Failed,
}

struct PreflightCheck {
  label: String,
  detail: String,
  status: PreflightStatus,
}

struct PreflightReport {
  checks: Vec<PreflightCheck>,
}

impl PreflightReport {
  fn collect() -> Result<(Self, super::PreflightState)> {
    let client = Client::builder()
      .https_only(true)
      .timeout(std::time::Duration::from_secs(5))
      .build()?;

    println!();
    println!("{}", ui::section(t!("install.preflight.title")));

    let mut state = super::PreflightState::default();
    let disk_free = disk_free_bytes("/var/lib").or_else(|_| disk_free_bytes("/"));
    state.set_disk_free_bytes(disk_free.as_ref().ok().copied());

    let mut checks = Vec::new();
    checks.push(run_preflight_check(
      t!("install.preflight.checks.downloader").to_string(),
      check_downloader,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.systemd").to_string(),
      check_systemd,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.package_manager").to_string(),
      || {
        let package_manager = SystemPackageManager::detect();
        state.set_package_manager(package_manager);
        check_package_manager(package_manager)
      },
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.public_network").to_string(),
      || {
        let public_network = probe_public_network(&client);
        state.set_public_network(public_network.clone());
        check_public_network(&public_network)
      },
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.github").to_string(),
      || check_github_connectivity(probe_endpoint(&client, "https://github.com")),
    ));

    let k3s_label = t!("install.preflight.checks.k3s_sources").to_string();
    checks.push(run_preflight_check(k3s_label.clone(), || {
      let official = probe_endpoint(&client, "https://get.k3s.io");
      let mirror = probe_endpoint(
        &client,
        "https://rancher-mirror.rancher.cn/k3s/k3s-install.sh",
      );
      state.set_source_reachability(
        crate::config::KubernetesDistribution::K3s,
        crate::config::KubernetesInstallSource::Official,
        official,
      );
      state.set_source_reachability(
        crate::config::KubernetesDistribution::K3s,
        crate::config::KubernetesInstallSource::ChinaMirror,
        mirror,
      );

      check_source_connectivity(
        k3s_label,
        &[
          EndpointReachability {
            label: "get.k3s.io",
            reachable: official,
          },
          EndpointReachability {
            label: "rancher-mirror.rancher.cn/k3s",
            reachable: mirror,
          },
        ],
      )
    }));

    let rke2_label = t!("install.preflight.checks.rke2_sources").to_string();
    checks.push(run_preflight_check(rke2_label.clone(), || {
      let official = probe_endpoint(&client, "https://get.rke2.io");
      let mirror = probe_endpoint(&client, "https://rancher-mirror.rancher.cn/rke2/install.sh");
      state.set_source_reachability(
        crate::config::KubernetesDistribution::Rke2,
        crate::config::KubernetesInstallSource::Official,
        official,
      );
      state.set_source_reachability(
        crate::config::KubernetesDistribution::Rke2,
        crate::config::KubernetesInstallSource::ChinaMirror,
        mirror,
      );

      check_source_connectivity(
        rke2_label,
        &[
          EndpointReachability {
            label: "get.rke2.io",
            reachable: official,
          },
          EndpointReachability {
            label: "rancher-mirror.rancher.cn/rke2",
            reachable: mirror,
          },
        ],
      )
    }));

    checks.push(run_preflight_check(
      t!("install.preflight.checks.disk").to_string(),
      || check_disk_capacity(disk_free),
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.memory").to_string(),
      check_memory_capacity,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.ports").to_string(),
      check_port_state,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.sysctl").to_string(),
      check_sysctl_state,
    ));
    checks.push(run_preflight_check(
      t!("install.preflight.checks.cgroup_memory").to_string(),
      check_cgroup_memory,
    ));

    let overlay_label = t!("install.preflight.checks.overlay").to_string();
    checks.push(run_preflight_check(overlay_label.clone(), || {
      check_kernel_feature(overlay_label, kernel_feature_state_overlay())
    }));

    let br_netfilter_label = t!("install.preflight.checks.br_netfilter").to_string();
    checks.push(run_preflight_check(br_netfilter_label.clone(), || {
      check_kernel_feature(br_netfilter_label, kernel_feature_state_br_netfilter())
    }));

    Ok((Self { checks }, state))
  }

  fn has_failures(&self) -> bool {
    self
      .checks
      .iter()
      .any(|check| matches!(check.status, PreflightStatus::Failed))
  }

  fn has_warnings(&self) -> bool {
    self
      .checks
      .iter()
      .any(|check| matches!(check.status, PreflightStatus::Warning))
  }

  fn print(&self) {
    println!();
    println!(
      "{}",
      if self.has_failures() {
        ui::warning(t!("install.preflight.summary.failed"))
      } else if self.has_warnings() {
        ui::note(t!("install.preflight.summary.warning"))
      } else {
        ui::success(t!("install.preflight.summary.passed"))
      }
    );
  }
}

struct EndpointReachability<'a> {
  label: &'a str,
  reachable: bool,
}

enum KernelFeatureState {
  Ready,
  Loadable,
  Missing,
}

fn check_downloader() -> PreflightCheck {
  let available = ["curl", "wget"]
    .into_iter()
    .filter(|binary| command_exists(binary))
    .collect::<Vec<_>>();

  if available.is_empty() {
    return PreflightCheck {
      label: t!("install.preflight.checks.downloader").to_string(),
      detail: t!("install.preflight.details.downloader_missing").to_string(),
      status: PreflightStatus::Failed,
    };
  }

  PreflightCheck {
    label: t!("install.preflight.checks.downloader").to_string(),
    detail: t!(
      "install.preflight.details.downloader_available",
      available = available.join(", ")
    )
    .to_string(),
    status: PreflightStatus::Passed,
  }
}

fn check_systemd() -> PreflightCheck {
  let active = Path::new("/run/systemd/system").is_dir() && command_exists("systemctl");

  PreflightCheck {
    label: t!("install.preflight.checks.systemd").to_string(),
    detail: if active {
      t!("install.preflight.details.systemd_ready").to_string()
    } else {
      t!("install.preflight.details.systemd_missing").to_string()
    },
    status: if active {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Failed
    },
  }
}

fn check_package_manager(package_manager: Option<SystemPackageManager>) -> PreflightCheck {
  PreflightCheck {
    label: t!("install.preflight.checks.package_manager").to_string(),
    detail: match package_manager {
      Some(package_manager) => t!(
        "install.preflight.details.package_manager_ready",
        package_manager = package_manager.label()
      )
      .to_string(),
      None => t!("install.preflight.details.package_manager_missing").to_string(),
    },
    status: if package_manager.is_some() {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Failed
    },
  }
}

fn check_public_network(
  public_network: &Option<super::context::PublicNetworkIdentity>,
) -> PreflightCheck {
  PreflightCheck {
    label: t!("install.preflight.checks.public_network").to_string(),
    detail: match public_network {
      Some(identity) => t!(
        "install.preflight.details.public_network_detected",
        public_network = identity.display()
      )
      .to_string(),
      None => t!("install.preflight.details.public_network_unknown").to_string(),
    },
    status: if public_network.is_some() {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Warning
    },
  }
}

fn check_github_connectivity(reachable: bool) -> PreflightCheck {
  PreflightCheck {
    label: t!("install.preflight.checks.github").to_string(),
    detail: if reachable {
      t!("install.preflight.details.github_reachable").to_string()
    } else {
      t!("install.preflight.details.github_unreachable").to_string()
    },
    status: if reachable {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Warning
    },
  }
}

fn check_source_connectivity(
  label: String, endpoints: &[EndpointReachability<'_>],
) -> PreflightCheck {
  let reachable = endpoints
    .iter()
    .filter(|endpoint| endpoint.reachable)
    .map(|endpoint| endpoint.label)
    .collect::<Vec<_>>();
  let unreachable = endpoints
    .iter()
    .filter(|endpoint| !endpoint.reachable)
    .map(|endpoint| endpoint.label)
    .collect::<Vec<_>>();

  match (reachable.is_empty(), unreachable.is_empty()) {
    (false, true) => PreflightCheck {
      label,
      detail: t!(
        "install.preflight.details.sources_all_reachable",
        reachable = reachable.join(", ")
      )
      .to_string(),
      status: PreflightStatus::Passed,
    },
    (false, false) => PreflightCheck {
      label,
      detail: t!(
        "install.preflight.details.sources_partial",
        reachable = reachable.join(", "),
        unreachable = unreachable.join(", ")
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
    (true, false) => PreflightCheck {
      label,
      detail: t!(
        "install.preflight.details.sources_missing",
        unreachable = unreachable.join(", ")
      )
      .to_string(),
      status: PreflightStatus::Failed,
    },
    (true, true) => PreflightCheck {
      label,
      detail: t!("install.preflight.details.sources_unknown").to_string(),
      status: PreflightStatus::Failed,
    },
  }
}

fn check_disk_capacity(result: Result<u64>) -> PreflightCheck {
  match result {
    Ok(free_bytes) if free_bytes < PREFLIGHT_MIN_DISK_FREE_BYTES => PreflightCheck {
      label: t!("install.preflight.checks.disk").to_string(),
      detail: t!(
        "install.preflight.details.disk_failed",
        free = format_gib(free_bytes)
      )
      .to_string(),
      status: PreflightStatus::Failed,
    },
    Ok(free_bytes) if free_bytes < PREFLIGHT_WARN_DISK_FREE_BYTES => PreflightCheck {
      label: t!("install.preflight.checks.disk").to_string(),
      detail: t!(
        "install.preflight.details.disk_warning",
        free = format_gib(free_bytes)
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
    Ok(free_bytes) => PreflightCheck {
      label: t!("install.preflight.checks.disk").to_string(),
      detail: t!(
        "install.preflight.details.disk_ready",
        free = format_gib(free_bytes)
      )
      .to_string(),
      status: PreflightStatus::Passed,
    },
    Err(error) => PreflightCheck {
      label: t!("install.preflight.checks.disk").to_string(),
      detail: t!(
        "install.preflight.details.disk_unknown",
        error = error.to_string()
      )
      .to_string(),
      status: PreflightStatus::Failed,
    },
  }
}

fn check_memory_capacity() -> PreflightCheck {
  match memory_total_bytes() {
    Ok(total_bytes) if total_bytes < PREFLIGHT_MIN_MEMORY_BYTES => PreflightCheck {
      label: t!("install.preflight.checks.memory").to_string(),
      detail: t!(
        "install.preflight.details.memory_failed",
        total = format_gib(total_bytes)
      )
      .to_string(),
      status: PreflightStatus::Failed,
    },
    Ok(total_bytes) if total_bytes < PREFLIGHT_WARN_MEMORY_BYTES => PreflightCheck {
      label: t!("install.preflight.checks.memory").to_string(),
      detail: t!(
        "install.preflight.details.memory_warning",
        total = format_gib(total_bytes)
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
    Ok(total_bytes) => PreflightCheck {
      label: t!("install.preflight.checks.memory").to_string(),
      detail: t!(
        "install.preflight.details.memory_ready",
        total = format_gib(total_bytes)
      )
      .to_string(),
      status: PreflightStatus::Passed,
    },
    Err(error) => PreflightCheck {
      label: t!("install.preflight.checks.memory").to_string(),
      detail: t!(
        "install.preflight.details.memory_unknown",
        error = error.to_string()
      )
      .to_string(),
      status: PreflightStatus::Warning,
    },
  }
}

fn check_port_state() -> PreflightCheck {
  let listening = listening_tcp_ports();
  let all_nodes_in_use = [10250_u16]
    .into_iter()
    .filter(|port| listening.contains(port))
    .collect::<Vec<_>>();
  let control_plane_in_use = [6443_u16, 9345, 2379, 2380]
    .into_iter()
    .filter(|port| listening.contains(port))
    .collect::<Vec<_>>();

  if !all_nodes_in_use.is_empty() {
    return PreflightCheck {
      label: t!("install.preflight.checks.ports").to_string(),
      detail: t!(
        "install.preflight.details.ports_failed",
        ports = format_ports(&all_nodes_in_use)
      )
      .to_string(),
      status: PreflightStatus::Failed,
    };
  }

  if !control_plane_in_use.is_empty() {
    return PreflightCheck {
      label: t!("install.preflight.checks.ports").to_string(),
      detail: t!(
        "install.preflight.details.ports_warning",
        ports = format_ports(&control_plane_in_use)
      )
      .to_string(),
      status: PreflightStatus::Warning,
    };
  }

  PreflightCheck {
    label: t!("install.preflight.checks.ports").to_string(),
    detail: t!("install.preflight.details.ports_ready").to_string(),
    status: PreflightStatus::Passed,
  }
}

fn check_sysctl_state() -> PreflightCheck {
  let sysctls = [
    ("net.ipv4.ip_forward", "/proc/sys/net/ipv4/ip_forward"),
    (
      "net.bridge.bridge-nf-call-iptables",
      "/proc/sys/net/bridge/bridge-nf-call-iptables",
    ),
    (
      "net.bridge.bridge-nf-call-ip6tables",
      "/proc/sys/net/bridge/bridge-nf-call-ip6tables",
    ),
  ];

  let mismatches = sysctls
    .into_iter()
    .filter_map(|(name, path)| match fs::read_to_string(path) {
      Ok(value) => {
        let trimmed = value.trim().to_string();
        if trimmed == "1" {
          None
        } else {
          Some(format!("{name}={trimmed}"))
        }
      }
      Err(_) => Some(format!("{name}=missing")),
    })
    .collect::<Vec<_>>();

  if mismatches.is_empty() {
    return PreflightCheck {
      label: t!("install.preflight.checks.sysctl").to_string(),
      detail: t!("install.preflight.details.sysctl_ready").to_string(),
      status: PreflightStatus::Passed,
    };
  }

  PreflightCheck {
    label: t!("install.preflight.checks.sysctl").to_string(),
    detail: t!(
      "install.preflight.details.sysctl_warning",
      values = mismatches.join(", ")
    )
    .to_string(),
    status: PreflightStatus::Warning,
  }
}

fn check_cgroup_memory() -> PreflightCheck {
  let available = cgroup_memory_available();

  PreflightCheck {
    label: t!("install.preflight.checks.cgroup_memory").to_string(),
    detail: if available {
      t!("install.preflight.details.cgroup_memory_ready").to_string()
    } else {
      t!("install.preflight.details.cgroup_memory_missing").to_string()
    },
    status: if available {
      PreflightStatus::Passed
    } else {
      PreflightStatus::Failed
    },
  }
}

fn check_kernel_feature(label: String, state: KernelFeatureState) -> PreflightCheck {
  let (detail, status) = match state {
    KernelFeatureState::Ready => (
      t!("install.preflight.details.kernel_ready").to_string(),
      PreflightStatus::Passed,
    ),
    KernelFeatureState::Loadable => (
      t!("install.preflight.details.kernel_loadable").to_string(),
      PreflightStatus::Warning,
    ),
    KernelFeatureState::Missing => (
      t!("install.preflight.details.kernel_missing").to_string(),
      PreflightStatus::Failed,
    ),
  };

  PreflightCheck {
    label,
    detail,
    status,
  }
}

fn probe_endpoint(client: &Client, url: &str) -> bool {
  client
    .get(url)
    .header(
      "User-Agent",
      format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
    )
    .send()
    .and_then(|response| response.error_for_status())
    .is_ok()
}

fn probe_public_network(client: &Client) -> Option<super::context::PublicNetworkIdentity> {
  client
    .get("https://api.ip.sb/geoip")
    .header(
      "User-Agent",
      format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
    )
    .send()
    .ok()?
    .error_for_status()
    .ok()?
    .json::<super::context::PublicNetworkIdentity>()
    .ok()
}

fn kernel_feature_state_overlay() -> KernelFeatureState {
  kernel_feature_state(
    "/sys/module/overlay",
    "/proc/filesystems",
    "overlay",
    "overlay",
  )
}

fn kernel_feature_state_br_netfilter() -> KernelFeatureState {
  kernel_feature_state(
    "/sys/module/br_netfilter",
    "/proc/sys/net/bridge/bridge-nf-call-iptables",
    "",
    "br_netfilter",
  )
}

fn kernel_feature_state(
  module_path: &str, marker_path: &str, marker_text: &str, module_name: &str,
) -> KernelFeatureState {
  if Path::new(module_path).exists()
    || (Path::new(marker_path).is_file()
      && (marker_text.is_empty() || file_contains(marker_path, marker_text)))
  {
    return KernelFeatureState::Ready;
  }

  if modprobe_can_load(module_name) {
    return KernelFeatureState::Loadable;
  }

  KernelFeatureState::Missing
}

fn preflight_status_tag(status: &PreflightStatus) -> String {
  match status {
    PreflightStatus::Passed => {
      ui::status_tag(t!("install.preflight.status.ok"), ui::BadgeTone::Success)
    }
    PreflightStatus::Warning => ui::status_tag(
      t!("install.preflight.status.warning"),
      ui::BadgeTone::Pending,
    ),
    PreflightStatus::Failed => {
      ui::status_tag(t!("install.preflight.status.failed"), ui::BadgeTone::Danger)
    }
  }
}

fn preflight_pending_line(label: &str) -> String {
  format!(
    "{} {}",
    ui::status_tag(
      t!("install.preflight.status.running"),
      ui::BadgeTone::Active
    ),
    label
  )
}

fn preflight_result_line(check: &PreflightCheck) -> String {
  format!(
    "{} {} - {}",
    preflight_status_tag(&check.status),
    check.label,
    check.detail
  )
}

fn run_preflight_check<F>(label: String, run: F) -> PreflightCheck
where
  F: FnOnce() -> PreflightCheck, {
  ui::transient_line(preflight_pending_line(&label));
  let check = run();
  ui::transient_line_done(preflight_result_line(&check));
  check
}
