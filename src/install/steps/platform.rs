use std::collections::BTreeMap;

use anyhow::{Result, anyhow, bail};
use rust_i18n::t;

use super::{
  AtomicInstallStep, InstallStepPlan, StepExecutionContext, StepPlanContext, StepQuestionContext,
  support::{install_directory, install_staged_file, stage_text_file, yaml_quote},
};
use crate::{
  config::{
    InstallStepId, InstallTargetRole, PlatformServiceDeploymentMode, PlatformServiceId,
    PlatformStorageMode,
  },
  install::collectors::{Collector, InputCollector, SingleSelectCollector},
  ui,
};

const PLATFORM_PLAN_DEST: &str = "/etc/ret2shell/ret2boot-platform-plan.yaml";
const PLATFORM_NAMESPACE: &str = "ret2shell-platform";
const CHALLENGE_NAMESPACE: &str = "ret2shell-challenge";
const PLATFORM_SERVICE_ACCOUNT: &str = "ret2shell-service";
const PLATFORM_CLUSTER_ROLE: &str = "ret2shell-service";
const PLATFORM_CLUSTER_ROLE_BINDING: &str = "ret2shell-service-global";

#[derive(Clone, Copy)]
struct PlatformServiceDefinition {
  id: PlatformServiceId,
  release_name: &'static str,
  namespace: &'static str,
  allow_disabled: bool,
  fixed_local_path: bool,
  legacy_local_disk_gib: u32,
  legacy_storage_class: &'static str,
}

const PLATFORM_SERVICE_DEFINITIONS: [PlatformServiceDefinition; 6] = [
  PlatformServiceDefinition {
    id: PlatformServiceId::Platform,
    release_name: "ret2shell-platform",
    namespace: PLATFORM_NAMESPACE,
    allow_disabled: false,
    fixed_local_path: true,
    legacy_local_disk_gib: 420,
    legacy_storage_class: "local-path",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Database,
    release_name: "ret2shell-database",
    namespace: PLATFORM_NAMESPACE,
    allow_disabled: false,
    fixed_local_path: false,
    legacy_local_disk_gib: 140,
    legacy_storage_class: "ret2shell-storage-database",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Cache,
    release_name: "ret2shell-cache",
    namespace: PLATFORM_NAMESPACE,
    allow_disabled: false,
    fixed_local_path: false,
    legacy_local_disk_gib: 10,
    legacy_storage_class: "ret2shell-storage-cache",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Queue,
    release_name: "ret2shell-queue",
    namespace: PLATFORM_NAMESPACE,
    allow_disabled: false,
    fixed_local_path: false,
    legacy_local_disk_gib: 10,
    legacy_storage_class: "ret2shell-storage-queue",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Registry,
    release_name: "ret2shell-registry",
    namespace: PLATFORM_NAMESPACE,
    allow_disabled: true,
    fixed_local_path: false,
    legacy_local_disk_gib: 300,
    legacy_storage_class: "ret2shell-storage-registry",
  },
  PlatformServiceDefinition {
    id: PlatformServiceId::Logs,
    release_name: "ret2shell-logs",
    namespace: PLATFORM_NAMESPACE,
    allow_disabled: true,
    fixed_local_path: false,
    legacy_local_disk_gib: 3,
    legacy_storage_class: "ret2shell-storage-logs",
  },
];

pub struct PlatformDeploymentStep;

impl AtomicInstallStep for PlatformDeploymentStep {
  fn id(&self) -> InstallStepId {
    InstallStepId::PlatformDeployment
  }

  fn should_include(&self, ctx: &StepPlanContext<'_>) -> bool {
    ctx.node_role() == Some(InstallTargetRole::ControlPlane)
  }

  fn collect(&self, ctx: &mut StepQuestionContext<'_>) -> Result<()> {
    print_platform_bootstrap_notice();

    let remaining_disk = ctx.preflight_state().disk_free_gib().ok_or_else(|| {
      anyhow!("unable to detect the remaining disk capacity for platform planning")
    })?;
    ctx.persist_change(
      "install.questionnaire.platform.remaining_disk_gib",
      &remaining_disk.to_string(),
      |config| config.set_platform_remaining_disk_gib(remaining_disk),
    )?;
    println!(
      "{}",
      ui::note(t!(
        "install.platform.detected_remaining_disk",
        remaining = remaining_disk
      ))
    );

    let suggested_requested = ctx
      .config()
      .install
      .questionnaire
      .platform
      .requested_disk_gib
      .unwrap_or_else(|| remaining_disk.min(platform_legacy_total_disk_gib()).max(1));
    let requested_disk = collect_u32_gib_input(
      t!("install.platform.requested_disk.prompt").to_string(),
      suggested_requested.min(remaining_disk).max(1),
      1,
      Some(remaining_disk),
    )?;
    ctx.persist_change(
      "install.questionnaire.platform.requested_disk_gib",
      &requested_disk.to_string(),
      |config| config.set_platform_requested_disk_gib(requested_disk),
    )?;

    println!();
    println!("{}", ui::section(t!("install.platform.services_intro")));

    for definition in PLATFORM_SERVICE_DEFINITIONS {
      collect_platform_service(ctx, &definition)?;
    }

    collect_platform_disk_plan(ctx)?;

    Ok(())
  }

  fn describe(&self, ctx: &StepPlanContext<'_>) -> Result<InstallStepPlan> {
    let summary = platform_plan_summary(ctx)?;
    let mut details = vec![
      t!(
        "install.steps.platform_plan.namespaces",
        platform = PLATFORM_NAMESPACE,
        challenge = CHALLENGE_NAMESPACE
      )
      .to_string(),
      t!(
        "install.steps.platform_plan.service_account",
        name = PLATFORM_SERVICE_ACCOUNT,
        namespace = PLATFORM_NAMESPACE
      )
      .to_string(),
      t!(
        "install.steps.platform_plan.rbac",
        role = PLATFORM_CLUSTER_ROLE,
        binding = PLATFORM_CLUSTER_ROLE_BINDING
      )
      .to_string(),
      t!(
        "install.steps.platform_plan.disk_budget",
        remaining = summary.remaining_disk_gib,
        requested = summary.requested_disk_gib,
        allocated = summary.allocated_local_disk_gib
      )
      .to_string(),
    ];

    if summary.unallocated_local_disk_gib > 0 {
      details.push(
        t!(
          "install.steps.platform_plan.unallocated_disk",
          remaining = summary.unallocated_local_disk_gib
        )
        .to_string(),
      );
    }

    for service in &summary.services {
      details.push(format!(
        "{}: {}",
        platform_service_label(service.id),
        platform_service_summary_line(service)
      ));
    }

    Ok(InstallStepPlan {
      id: self.id(),
      title: t!("install.steps.platform_plan.title").to_string(),
      details,
    })
  }

  fn install(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    let summary = platform_plan_summary(&ctx.as_plan_context())?;
    install_directory(ctx, "/etc/ret2shell")?;

    let staged = stage_text_file(
      "ret2boot-platform-plan",
      "yaml",
      render_platform_plan_yaml(&summary),
    )?;
    install_staged_file(ctx, &staged, PLATFORM_PLAN_DEST)?;
    let _ = std::fs::remove_file(&staged);

    ctx.persist_change(
      "install.execution.platform.plan",
      PLATFORM_PLAN_DEST,
      |config| config.set_install_step_metadata(self.id(), "plan_path", PLATFORM_PLAN_DEST),
    )?;

    Ok(())
  }

  fn uninstall(&self, ctx: &mut StepExecutionContext<'_>) -> Result<()> {
    ctx.run_privileged_command(
      "rm",
      &["-f".to_string(), PLATFORM_PLAN_DEST.to_string()],
      &[],
    )?;
    ctx.persist_change("install.execution.platform.plan", "removed", |config| {
      config.remove_install_step_metadata(self.id(), "plan_path")
    })?;
    Ok(())
  }
}

struct PlatformPlanSummary {
  remaining_disk_gib: u32,
  requested_disk_gib: u32,
  allocated_local_disk_gib: u32,
  unallocated_local_disk_gib: u32,
  services: Vec<ResolvedPlatformServicePlan>,
}

struct ResolvedPlatformServicePlan {
  id: PlatformServiceId,
  release_name: &'static str,
  namespace: &'static str,
  deployment: PlatformServiceDeploymentMode,
  storage_mode: Option<PlatformStorageMode>,
  storage_class_name: Option<String>,
  local_disk_gib: Option<u32>,
}

fn platform_plan_summary(ctx: &StepPlanContext<'_>) -> Result<PlatformPlanSummary> {
  let remaining_disk_gib = ctx
    .config()
    .install
    .questionnaire
    .platform
    .remaining_disk_gib
    .ok_or_else(|| anyhow!("remaining disk capacity is required before planning the platform"))?;
  let requested_disk_gib = ctx
    .config()
    .install
    .questionnaire
    .platform
    .requested_disk_gib
    .ok_or_else(|| anyhow!("requested disk budget is required before planning the platform"))?;

  let services = PLATFORM_SERVICE_DEFINITIONS
    .iter()
    .map(|definition| resolve_platform_service_plan(ctx, definition))
    .collect::<Result<Vec<_>>>()?;
  let allocated_local_disk_gib = services
    .iter()
    .filter_map(|service| service.local_disk_gib)
    .sum();

  if allocated_local_disk_gib > requested_disk_gib {
    bail!("platform local disk allocations exceed the requested disk budget");
  }

  Ok(PlatformPlanSummary {
    remaining_disk_gib,
    requested_disk_gib,
    allocated_local_disk_gib,
    unallocated_local_disk_gib: requested_disk_gib - allocated_local_disk_gib,
    services,
  })
}

fn resolve_platform_service_plan(
  ctx: &StepPlanContext<'_>, definition: &PlatformServiceDefinition,
) -> Result<ResolvedPlatformServicePlan> {
  let stored = ctx
    .config()
    .platform_service_config(definition.id)
    .cloned()
    .unwrap_or_default();

  let deployment = if definition.fixed_local_path {
    PlatformServiceDeploymentMode::Local
  } else {
    stored.deployment.ok_or_else(|| {
      anyhow!(
        "deployment mode for service `{}` is required before planning the platform",
        definition.id.as_config_value()
      )
    })?
  };

  let storage_mode = if deployment == PlatformServiceDeploymentMode::Local {
    if definition.fixed_local_path {
      Some(PlatformStorageMode::LocalPath)
    } else {
      Some(stored.storage_mode.ok_or_else(|| {
        anyhow!(
          "storage mode for service `{}` is required before planning the platform",
          definition.id.as_config_value()
        )
      })?)
    }
  } else {
    None
  };

  let storage_class_name = match storage_mode {
    Some(PlatformStorageMode::CustomStorageClass) => {
      Some(stored.storage_class_name.clone().ok_or_else(|| {
        anyhow!(
          "custom storage class for service `{}` is required before planning the platform",
          definition.id.as_config_value()
        )
      })?)
    }
    _ => None,
  };

  let local_disk_gib = if uses_local_disk(definition.id, deployment, storage_mode) {
    Some(stored.local_disk_gib.ok_or_else(|| {
      anyhow!(
        "local disk allocation for service `{}` is required before planning the platform",
        definition.id.as_config_value()
      )
    })?)
  } else {
    None
  };

  Ok(ResolvedPlatformServicePlan {
    id: definition.id,
    release_name: definition.release_name,
    namespace: definition.namespace,
    deployment,
    storage_mode,
    storage_class_name,
    local_disk_gib,
  })
}

fn print_platform_bootstrap_notice() {
  println!();
  println!("{}", ui::section(t!("install.platform.resources_intro")));
  println!(
    "{}",
    ui::note(t!(
      "install.platform.namespaces",
      platform = PLATFORM_NAMESPACE,
      challenge = CHALLENGE_NAMESPACE
    ))
  );
  println!(
    "{}",
    ui::note(t!(
      "install.platform.service_account",
      name = PLATFORM_SERVICE_ACCOUNT,
      namespace = PLATFORM_NAMESPACE
    ))
  );
  println!(
    "{}",
    ui::note(t!(
      "install.platform.rbac",
      role = PLATFORM_CLUSTER_ROLE,
      binding = PLATFORM_CLUSTER_ROLE_BINDING
    ))
  );
  println!("{}", ui::note(t!("install.platform.configmaps")));
  println!("{}", ui::warning(t!("install.platform.storage_notice")));
}

fn collect_platform_service(
  ctx: &mut StepQuestionContext<'_>, definition: &PlatformServiceDefinition,
) -> Result<()> {
  println!();
  println!(
    "{}",
    ui::section(t!(
      "install.platform.service_heading",
      service = platform_service_label(definition.id)
    ))
  );
  println!(
    "{}",
    ui::note(t!(
      "install.platform.service_release",
      release = definition.release_name,
      namespace = definition.namespace
    ))
  );
  println!("{}", ui::note(platform_service_description(definition.id)));

  if definition.fixed_local_path {
    ctx.persist_change(
      "install.questionnaire.platform.service.deployment",
      PlatformServiceDeploymentMode::Local.as_config_value(),
      |config| {
        config.set_platform_service_deployment(definition.id, PlatformServiceDeploymentMode::Local)
      },
    )?;
    ctx.persist_change(
      "install.questionnaire.platform.service.storage_mode",
      PlatformStorageMode::LocalPath.as_config_value(),
      |config| {
        config.set_platform_service_storage_mode(definition.id, PlatformStorageMode::LocalPath)
      },
    )?;
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.storage_class_name",
      "cleared",
      |config| config.clear_platform_service_storage_class_name(definition.id),
    )?;

    println!(
      "{}",
      ui::note(t!("install.platform.service_platform_fixed"))
    );
    return Ok(());
  }

  let deployment_modes = deployment_modes_for_service(definition);
  let deployment_default = ctx
    .config()
    .platform_service_config(definition.id)
    .and_then(|service| service.deployment)
    .unwrap_or(PlatformServiceDeploymentMode::Local);
  let deployment_default_index = deployment_modes
    .iter()
    .position(|mode| *mode == deployment_default)
    .unwrap_or(0);
  let deployment_options = deployment_modes
    .iter()
    .copied()
    .map(platform_service_deployment_label)
    .collect();
  let deployment = deployment_modes[SingleSelectCollector::new(
    t!("install.platform.service_deployment.prompt"),
    deployment_options,
  )
  .with_default(deployment_default_index)
  .collect_index()?];

  ctx.persist_change(
    "install.questionnaire.platform.service.deployment",
    deployment.as_config_value(),
    |config| config.set_platform_service_deployment(definition.id, deployment),
  )?;

  if deployment != PlatformServiceDeploymentMode::Local {
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.storage_mode",
      "cleared",
      |config| config.clear_platform_service_storage_mode(definition.id),
    )?;
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.storage_class_name",
      "cleared",
      |config| config.clear_platform_service_storage_class_name(definition.id),
    )?;
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.local_disk_gib",
      "cleared",
      |config| config.clear_platform_service_local_disk_gib(definition.id),
    )?;
    return Ok(());
  }

  let storage_modes = [
    PlatformStorageMode::LocalPath,
    PlatformStorageMode::CustomStorageClass,
  ];
  let storage_default = ctx
    .config()
    .platform_service_config(definition.id)
    .and_then(|service| service.storage_mode)
    .unwrap_or(PlatformStorageMode::LocalPath);
  let storage_default_index = storage_modes
    .iter()
    .position(|mode| *mode == storage_default)
    .unwrap_or(0);
  let storage_options = storage_modes
    .iter()
    .copied()
    .map(platform_storage_mode_label)
    .collect();
  let storage_mode = storage_modes[SingleSelectCollector::new(
    t!("install.platform.service_storage.prompt"),
    storage_options,
  )
  .with_default(storage_default_index)
  .collect_index()?];

  ctx.persist_change(
    "install.questionnaire.platform.service.storage_mode",
    storage_mode.as_config_value(),
    |config| config.set_platform_service_storage_mode(definition.id, storage_mode),
  )?;

  if storage_mode == PlatformStorageMode::CustomStorageClass {
    let class_prompt = InputCollector::new(t!("install.platform.service_storage_class.prompt"));
    let class_prompt = match ctx
      .config()
      .platform_service_config(definition.id)
      .and_then(|service| service.storage_class_name.clone())
    {
      Some(default) => class_prompt.with_default(default),
      None => class_prompt.with_default(definition.legacy_storage_class),
    };
    let storage_class = class_prompt.collect()?.trim().to_string();

    ctx.persist_change(
      "install.questionnaire.platform.service.storage_class_name",
      &storage_class,
      |config| config.set_platform_service_storage_class_name(definition.id, storage_class.clone()),
    )?;
  } else {
    let _ = ctx.persist_change(
      "install.questionnaire.platform.service.storage_class_name",
      "cleared",
      |config| config.clear_platform_service_storage_class_name(definition.id),
    )?;
  }

  Ok(())
}

fn collect_platform_disk_plan(ctx: &mut StepQuestionContext<'_>) -> Result<()> {
  let remaining_disk = ctx
    .config()
    .install
    .questionnaire
    .platform
    .remaining_disk_gib
    .ok_or_else(|| anyhow!("remaining disk capacity is required before planning local storage"))?;
  let mut requested_disk = ctx
    .config()
    .install
    .questionnaire
    .platform
    .requested_disk_gib
    .ok_or_else(|| anyhow!("requested disk capacity is required before planning local storage"))?;
  let local_disk_services = local_disk_services(ctx.config());

  if local_disk_services.is_empty() {
    return Ok(());
  }

  let minimum_requested = local_disk_services.len() as u32;
  if requested_disk < minimum_requested {
    println!(
      "{}",
      ui::warning(t!("install.platform.disk_budget_too_small"))
    );
    requested_disk = collect_u32_gib_input(
      t!("install.platform.requested_disk.prompt").to_string(),
      minimum_requested.min(remaining_disk).max(1),
      minimum_requested,
      Some(remaining_disk),
    )?;
    ctx.persist_change(
      "install.questionnaire.platform.requested_disk_gib",
      &requested_disk.to_string(),
      |config| config.set_platform_requested_disk_gib(requested_disk),
    )?;
  }

  loop {
    println!();
    println!("{}", ui::section(t!("install.platform.disk_plan_intro")));

    let defaults = scaled_platform_disk_defaults(&local_disk_services, requested_disk);
    let mut assigned_total = 0_u32;
    let mut answers = Vec::new();

    for definition in &local_disk_services {
      let default_value = ctx
        .config()
        .platform_service_config(definition.id)
        .and_then(|service| service.local_disk_gib)
        .unwrap_or_else(|| {
          *defaults
            .get(&definition.id)
            .unwrap_or(&definition.legacy_local_disk_gib)
        });
      let value = collect_u32_gib_input(
        t!(
          "install.platform.service_disk.prompt",
          service = platform_service_label(definition.id)
        )
        .to_string(),
        default_value.max(1),
        1,
        Some(requested_disk),
      )?;
      assigned_total = assigned_total.saturating_add(value);
      answers.push((definition.id, value));
    }

    if assigned_total > requested_disk {
      println!(
        "{}",
        ui::warning(t!(
          "install.platform.disk_plan_overflow",
          assigned = assigned_total,
          budget = requested_disk
        ))
      );
      continue;
    }

    for (service, value) in answers {
      ctx.persist_change(
        "install.questionnaire.platform.service.local_disk_gib",
        &value.to_string(),
        |config| config.set_platform_service_local_disk_gib(service, value),
      )?;
    }

    for definition in PLATFORM_SERVICE_DEFINITIONS {
      if !local_disk_services
        .iter()
        .any(|item| item.id == definition.id)
      {
        let _ = ctx.persist_change(
          "install.questionnaire.platform.service.local_disk_gib",
          "cleared",
          |config| config.clear_platform_service_local_disk_gib(definition.id),
        )?;
      }
    }

    if assigned_total < requested_disk {
      println!(
        "{}",
        ui::note(t!(
          "install.platform.disk_plan_unused",
          unused = requested_disk - assigned_total
        ))
      );
    }

    break;
  }

  Ok(())
}

fn platform_service_label(service: PlatformServiceId) -> String {
  match service {
    PlatformServiceId::Platform => t!("install.platform.services.platform").to_string(),
    PlatformServiceId::Database => t!("install.platform.services.database").to_string(),
    PlatformServiceId::Cache => t!("install.platform.services.cache").to_string(),
    PlatformServiceId::Queue => t!("install.platform.services.queue").to_string(),
    PlatformServiceId::Registry => t!("install.platform.services.registry").to_string(),
    PlatformServiceId::Logs => t!("install.platform.services.logs").to_string(),
  }
}

fn platform_service_description(service: PlatformServiceId) -> String {
  match service {
    PlatformServiceId::Platform => t!("install.platform.service_desc.platform").to_string(),
    PlatformServiceId::Database => t!("install.platform.service_desc.database").to_string(),
    PlatformServiceId::Cache => t!("install.platform.service_desc.cache").to_string(),
    PlatformServiceId::Queue => t!("install.platform.service_desc.queue").to_string(),
    PlatformServiceId::Registry => t!("install.platform.service_desc.registry").to_string(),
    PlatformServiceId::Logs => t!("install.platform.service_desc.logs").to_string(),
  }
}

fn platform_service_deployment_label(mode: PlatformServiceDeploymentMode) -> String {
  match mode {
    PlatformServiceDeploymentMode::Local => t!("install.platform.deployment.local").to_string(),
    PlatformServiceDeploymentMode::External => {
      t!("install.platform.deployment.external").to_string()
    }
    PlatformServiceDeploymentMode::Disabled => {
      t!("install.platform.deployment.disabled").to_string()
    }
  }
}

fn platform_storage_mode_label(mode: PlatformStorageMode) -> String {
  match mode {
    PlatformStorageMode::LocalPath => t!("install.platform.storage.local_path").to_string(),
    PlatformStorageMode::CustomStorageClass => {
      t!("install.platform.storage.custom_storage_class").to_string()
    }
  }
}

fn deployment_modes_for_service(
  definition: &PlatformServiceDefinition,
) -> Vec<PlatformServiceDeploymentMode> {
  let mut modes = vec![
    PlatformServiceDeploymentMode::Local,
    PlatformServiceDeploymentMode::External,
  ];
  if definition.allow_disabled {
    modes.push(PlatformServiceDeploymentMode::Disabled);
  }
  modes
}

fn local_disk_services(config: &crate::config::Ret2BootConfig) -> Vec<PlatformServiceDefinition> {
  PLATFORM_SERVICE_DEFINITIONS
    .iter()
    .filter_map(|definition| {
      let deployment = if definition.fixed_local_path {
        PlatformServiceDeploymentMode::Local
      } else {
        config.platform_service_config(definition.id)?.deployment?
      };
      let storage_mode = if definition.fixed_local_path {
        Some(PlatformStorageMode::LocalPath)
      } else {
        config.platform_service_config(definition.id)?.storage_mode
      };

      uses_local_disk(definition.id, deployment, storage_mode).then_some(*definition)
    })
    .collect()
}

fn uses_local_disk(
  service: PlatformServiceId, deployment: PlatformServiceDeploymentMode,
  storage_mode: Option<PlatformStorageMode>,
) -> bool {
  if deployment != PlatformServiceDeploymentMode::Local {
    return false;
  }

  let definition = PLATFORM_SERVICE_DEFINITIONS
    .iter()
    .find(|definition| definition.id == service)
    .expect("platform service definition exists");
  definition.fixed_local_path || storage_mode == Some(PlatformStorageMode::LocalPath)
}

fn platform_legacy_total_disk_gib() -> u32 {
  PLATFORM_SERVICE_DEFINITIONS
    .iter()
    .map(|definition| definition.legacy_local_disk_gib)
    .sum()
}

fn scaled_platform_disk_defaults(
  services: &[PlatformServiceDefinition], target_total_gib: u32,
) -> BTreeMap<PlatformServiceId, u32> {
  let minimum_total = services.len() as u32;
  let extra_total = target_total_gib.saturating_sub(minimum_total);
  let base_total = services
    .iter()
    .map(|definition| definition.legacy_local_disk_gib as u64)
    .sum::<u64>()
    .max(1);
  let mut remaining_extra = extra_total;
  let mut allocations = BTreeMap::new();

  for (index, definition) in services.iter().enumerate() {
    let extra = if index + 1 == services.len() {
      remaining_extra
    } else {
      let scaled = (definition.legacy_local_disk_gib as u64 * extra_total as u64) / base_total;
      let scaled = scaled.min(remaining_extra as u64) as u32;
      remaining_extra -= scaled;
      scaled
    };

    allocations.insert(definition.id, 1 + extra);
  }

  allocations
}

fn collect_u32_gib_input(
  prompt: String, default_value: u32, min_value: u32, max_value: Option<u32>,
) -> Result<u32> {
  let mut default_value = default_value.max(min_value);
  if let Some(max_value) = max_value {
    default_value = default_value.min(max_value);
  }

  loop {
    let value = InputCollector::new(prompt.clone())
      .with_default(default_value.to_string())
      .collect()?;
    let trimmed = value.trim();

    let Ok(parsed) = trimmed.parse::<u32>() else {
      println!(
        "{}",
        ui::warning(t!("install.platform.input.invalid_integer"))
      );
      continue;
    };

    if parsed < min_value {
      println!(
        "{}",
        ui::warning(t!("install.platform.input.too_small", minimum = min_value))
      );
      continue;
    }

    if let Some(max_value) = max_value
      && parsed > max_value
    {
      println!(
        "{}",
        ui::warning(t!("install.platform.input.too_large", maximum = max_value))
      );
      continue;
    }

    return Ok(parsed);
  }
}

fn render_platform_plan_yaml(summary: &PlatformPlanSummary) -> String {
  let mut lines = vec![
    "apiVersion: ret2boot.ret2shell/v1alpha1".to_string(),
    "kind: PlatformPlan".to_string(),
    "metadata:".to_string(),
    "  name: ret2boot-platform".to_string(),
    "spec:".to_string(),
    format!("  platformNamespace: {}", yaml_quote(PLATFORM_NAMESPACE)),
    format!("  challengeNamespace: {}", yaml_quote(CHALLENGE_NAMESPACE)),
    "  serviceAccount:".to_string(),
    format!("    name: {}", yaml_quote(PLATFORM_SERVICE_ACCOUNT)),
    format!("    namespace: {}", yaml_quote(PLATFORM_NAMESPACE)),
    "  clusterRbac:".to_string(),
    format!("    role: {}", yaml_quote(PLATFORM_CLUSTER_ROLE)),
    format!("    binding: {}", yaml_quote(PLATFORM_CLUSTER_ROLE_BINDING)),
    "  configMaps:".to_string(),
    "    - ret2shell-config".to_string(),
    "    - ret2shell-blocked".to_string(),
    "  disk:".to_string(),
    format!("    remainingGiB: {}", summary.remaining_disk_gib),
    format!("    requestedGiB: {}", summary.requested_disk_gib),
    format!(
      "    allocatedLocalGiB: {}",
      summary.allocated_local_disk_gib
    ),
    format!("    unallocatedGiB: {}", summary.unallocated_local_disk_gib),
    "  services:".to_string(),
  ];

  for service in &summary.services {
    lines.push(format!("    - id: {}", service.id.as_config_value()));
    lines.push(format!(
      "      releaseName: {}",
      yaml_quote(service.release_name)
    ));
    lines.push(format!(
      "      namespace: {}",
      yaml_quote(service.namespace)
    ));
    lines.push(format!(
      "      deployment: {}",
      service.deployment.as_config_value()
    ));

    if let Some(storage_mode) = service.storage_mode {
      lines.push(format!(
        "      storageMode: {}",
        storage_mode.as_config_value()
      ));
    }
    if let Some(storage_class_name) = &service.storage_class_name {
      lines.push(format!(
        "      storageClassName: {}",
        yaml_quote(storage_class_name)
      ));
    }
    if let Some(local_disk_gib) = service.local_disk_gib {
      lines.push(format!("      localDiskGiB: {}", local_disk_gib));
    }
  }

  lines.push(String::new());
  lines.join("\n")
}

fn platform_service_summary_line(service: &ResolvedPlatformServicePlan) -> String {
  let mut parts = vec![platform_service_deployment_label(service.deployment)];

  if let Some(storage_mode) = service.storage_mode {
    parts.push(match storage_mode {
      PlatformStorageMode::LocalPath => platform_storage_mode_label(storage_mode),
      PlatformStorageMode::CustomStorageClass => format!(
        "{} ({})",
        platform_storage_mode_label(storage_mode),
        service.storage_class_name.as_deref().unwrap_or("-")
      ),
    });
  }

  if let Some(local_disk_gib) = service.local_disk_gib {
    parts.push(format!("{local_disk_gib} GiB"));
  }

  parts.join(" / ")
}
