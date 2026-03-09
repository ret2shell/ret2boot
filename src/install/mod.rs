pub mod collectors;
pub mod steps;

use anyhow::{Result, anyhow};
use rust_i18n::t;
use tracing::{debug, info};

use self::{
  collectors::{Collector, ConfirmCollector, SingleSelectCollector},
  steps::{InstallStepPlan, StepPlanContext, StepQuestionContext},
};
use crate::{
  config::{InstallExecutionPhase, InstallStepStatus, InstallTargetRole, Ret2BootConfig},
  l10n,
  startup::RuntimeState,
  telemetry,
  ui::{self, BadgeTone},
};

pub fn run(config: &mut Ret2BootConfig, runtime: &RuntimeState) -> Result<()> {
  info!(
      locale = %l10n::current_locale(),
      configured_language = ?config.language,
      privilege_backend = runtime.privilege_backend,
      "starting installer workflow"
  );

  let mut flow = InstallFlow::new(config, runtime)?;
  flow.greet();
  flow.collect_questionnaire()?;
  flow.enter_review_phase()?;

  let plan = flow.build_plan()?;
  flow.print_plan(&plan);

  if !flow.ensure_install_confirmation()? {
    return Ok(());
  }

  flow.prepare_installation(&plan)?;
  flow.print_progress(&plan);

  Ok(())
}

struct InstallPlan {
  node_role: InstallTargetRole,
  steps: Vec<InstallStepPlan>,
}

struct InstallFlow<'a> {
  config: &'a mut Ret2BootConfig,
  runtime: &'a RuntimeState,
  config_path: String,
}

impl<'a> InstallFlow<'a> {
  fn new(config: &'a mut Ret2BootConfig, runtime: &'a RuntimeState) -> Result<Self> {
    Ok(Self {
      config,
      runtime,
      config_path: Ret2BootConfig::path_display()?,
    })
  }

  fn greet(&self) {
    println!();
    println!(
      "{}",
      ui::banner_startup("Ret 2 Boot", env!("CARGO_PKG_VERSION"))
    );
    println!("{}", ui::section(t!("install.entry.title")));
    println!(
      "{}",
      ui::note_value(t!("install.entry.language"), &self.runtime.locale)
    );
    println!(
      "{}",
      ui::note_value(t!("install.entry.config_path"), &self.config_path)
    );
    println!(
      "{}",
      ui::note_value(
        t!("install.entry.privilege_backend"),
        self.runtime.privilege_backend
      )
    );
    println!(
      "{}",
      ui::note(t!(
        "install.entry.resume_hint",
        path = self.config_path.as_str()
      ))
    );
  }

  fn collect_questionnaire(&mut self) -> Result<()> {
    let default = self
      .config
      .install
      .questionnaire
      .node_role
      .unwrap_or(InstallTargetRole::ControlPlane)
      .default_index();

    let options = InstallTargetRole::ALL
      .iter()
      .copied()
      .map(node_role_label)
      .collect();

    let selected = SingleSelectCollector::new(t!("install.node_role.prompt"), options)
      .with_default(default)
      .collect_index()?;

    let role = InstallTargetRole::ALL[selected];

    self.persist_change(
      "install.questionnaire.node_role",
      role.as_config_value(),
      |config| config.set_install_node_role(role),
    )?;

    for step in steps::registry() {
      let should_include = {
        let plan_context = StepPlanContext::new(self.config, self.runtime, &self.config_path);
        step.should_include(&plan_context)
      };

      if !should_include {
        continue;
      }

      let mut question_context =
        StepQuestionContext::new(self.config, self.runtime, &self.config_path);
      step.collect(&mut question_context)?;
    }

    Ok(())
  }

  fn enter_review_phase(&mut self) -> Result<()> {
    if self.config.install.review.confirmed {
      return Ok(());
    }

    self.persist_change(
      "install.execution.phase",
      InstallExecutionPhase::Review.as_config_value(),
      |config| config.set_install_phase(InstallExecutionPhase::Review),
    )?;

    Ok(())
  }

  fn build_plan(&self) -> Result<InstallPlan> {
    let node_role = self
      .config
      .install
      .questionnaire
      .node_role
      .ok_or_else(|| anyhow!("install questionnaire is incomplete"))?;

    let plan_context = StepPlanContext::new(self.config, self.runtime, &self.config_path);
    let steps = steps::registry()
      .into_iter()
      .filter(|step| step.should_include(&plan_context))
      .map(|step| step.describe(&plan_context))
      .collect::<Result<Vec<_>>>()?;

    Ok(InstallPlan { node_role, steps })
  }

  fn print_plan(&self, plan: &InstallPlan) {
    println!();
    println!("{}", ui::section(t!("install.plan.title")));
    println!(
      "{}",
      ui::note_value(
        t!("install.plan.node_role"),
        node_role_label(plan.node_role)
      )
    );
    println!(
      "{}",
      ui::note_value(
        t!("install.plan.phase"),
        phase_label(self.config.install.execution.phase)
      )
    );
    println!("{}", ui::note(t!("install.plan.steps")));

    for (index, step) in plan.steps.iter().enumerate() {
      let status = self
        .config
        .install_step_status(step.id)
        .unwrap_or(InstallStepStatus::Pending);

      println!(
        "  {}. {} {}",
        index + 1,
        step.title,
        step_status_tag(status)
      );

      for detail in &step.details {
        println!("     - {detail}");
      }
    }
  }

  fn ensure_install_confirmation(&mut self) -> Result<bool> {
    println!();

    if self.config.install.review.confirmed {
      println!("{}", ui::note(t!("install.review.already_confirmed")));
      return Ok(true);
    }

    let confirmed = ConfirmCollector::new(t!("install.review.confirm_prompt"), false).collect()?;

    self.persist_change(
      "install.review.confirmed",
      if confirmed { "true" } else { "false" },
      |config| config.set_install_review_confirmed(confirmed),
    )?;

    if !confirmed {
      println!();
      println!("{}", ui::warning(t!("install.review.cancelled")));
    }

    Ok(confirmed)
  }

  fn prepare_installation(&mut self, plan: &InstallPlan) -> Result<()> {
    let step_ids: Vec<_> = plan.steps.iter().map(|step| step.id).collect();
    let serialized_steps = step_ids
      .iter()
      .map(|step| step.as_config_value())
      .collect::<Vec<_>>()
      .join(",");

    let steps_changed =
      self.persist_change("install.execution.steps", &serialized_steps, |config| {
        config.sync_install_steps(&step_ids)
      })?;

    let phase_changed = self.persist_change(
      "install.execution.phase",
      InstallExecutionPhase::Installing.as_config_value(),
      |config| config.set_install_phase(InstallExecutionPhase::Installing),
    )?;

    telemetry::init()?;

    info!(
      locale = %l10n::current_locale(),
      privilege_backend = self.runtime.privilege_backend,
      config_path = %self.config_path,
      step_count = plan.steps.len(),
      "installation phase logging activated"
    );

    println!();
    println!("{}", ui::section(t!("install.execution.title")));
    println!(
      "{}",
      if steps_changed || phase_changed {
        ui::success(t!("install.execution.progress_saved"))
      } else {
        ui::note(t!("install.execution.resume_ready"))
      }
    );
    println!("{}", ui::warning(t!("install.execution.noop_notice")));

    Ok(())
  }

  fn print_progress(&self, plan: &InstallPlan) {
    println!();
    println!("{}", ui::section(t!("install.progress.title")));

    for (index, step) in plan.steps.iter().enumerate() {
      let status = self
        .config
        .install_step_status(step.id)
        .unwrap_or(InstallStepStatus::Pending);

      println!(
        "  {}. {} {}",
        index + 1,
        step.title,
        step_status_tag(status)
      );
    }
  }

  fn persist_change<F>(&mut self, field: &'static str, value: &str, update: F) -> Result<bool>
  where
    F: FnOnce(&mut Ret2BootConfig) -> bool, {
    let changed = update(self.config);

    if changed {
      self.config.save()?;
    }

    debug!(
        config_path = %self.config_path,
        field,
        value,
        changed,
        "persisted installer state"
    );

    Ok(changed)
  }
}

fn node_role_label(role: InstallTargetRole) -> String {
  match role {
    InstallTargetRole::ControlPlane => t!("install.node_role.options.control_plane").to_string(),
    InstallTargetRole::Worker => t!("install.node_role.options.worker").to_string(),
  }
}

fn phase_label(phase: InstallExecutionPhase) -> String {
  match phase {
    InstallExecutionPhase::Questionnaire => t!("install.phase.questionnaire").to_string(),
    InstallExecutionPhase::Review => t!("install.phase.review").to_string(),
    InstallExecutionPhase::Installing => t!("install.phase.installing").to_string(),
    InstallExecutionPhase::Completed => t!("install.phase.completed").to_string(),
  }
}

fn step_status_label(status: InstallStepStatus) -> String {
  match status {
    InstallStepStatus::Pending => t!("install.progress.status.pending").to_string(),
    InstallStepStatus::InProgress => t!("install.progress.status.in_progress").to_string(),
    InstallStepStatus::Completed => t!("install.progress.status.completed").to_string(),
    InstallStepStatus::Failed => t!("install.progress.status.failed").to_string(),
  }
}

fn step_status_tag(status: InstallStepStatus) -> String {
  let tone = match status {
    InstallStepStatus::Pending => BadgeTone::Pending,
    InstallStepStatus::InProgress => BadgeTone::Active,
    InstallStepStatus::Completed => BadgeTone::Success,
    InstallStepStatus::Failed => BadgeTone::Danger,
  };

  ui::status_tag(step_status_label(status), tone)
}
