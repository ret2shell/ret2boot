pub mod collectors;
pub mod steps;

use anyhow::{Result, anyhow};
use rust_i18n::t;
use tracing::{debug, info};

use self::{
  collectors::{Collector, ConfirmCollector, SingleSelectCollector},
  steps::{
    InstallStepPlan, PreflightState, StepExecutionContext, StepPlanContext, StepPreflightContext,
    StepQuestionContext,
  },
};
use crate::{
  config::{
    InstallExecutionPhase, InstallFailureStage, InstallStepId, InstallStepStatus,
    InstallTargetRole, Ret2BootConfig,
  },
  errors, l10n,
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
  flow.clear_recorded_failure()?;
  flow.greet();
  flow.capture_stage(InstallFailureStage::Preflight, None, |flow| {
    flow.run_preflight()
  })?;
  flow.capture_stage(InstallFailureStage::Questionnaire, None, |flow| {
    flow.collect_questionnaire()
  })?;
  flow.capture_stage(InstallFailureStage::Review, None, |flow| {
    flow.enter_review_phase()
  })?;

  let plan = flow.capture_stage(InstallFailureStage::Planning, None, |flow| {
    flow.build_plan()
  })?;
  flow.print_plan(&plan);

  if !flow.capture_stage(InstallFailureStage::Review, None, |flow| {
    flow.ensure_install_confirmation()
  })? {
    return Ok(());
  }

  flow.capture_stage(InstallFailureStage::Preparation, None, |flow| {
    flow.prepare_installation(&plan)
  })?;
  flow.capture_stage(InstallFailureStage::Install, None, |flow| {
    flow.execute_installation(&plan)
  })?;
  flow.print_progress(&plan);
  flow.clear_recorded_failure()?;

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
  validated_steps: Vec<crate::config::InstallStepId>,
  preflight_state: PreflightState,
}

impl<'a> InstallFlow<'a> {
  fn new(config: &'a mut Ret2BootConfig, runtime: &'a RuntimeState) -> Result<Self> {
    Ok(Self {
      config,
      runtime,
      config_path: Ret2BootConfig::path_display()?,
      validated_steps: Vec::new(),
      preflight_state: PreflightState::default(),
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
      ui::note_value(
        t!("install.entry.language"),
        l10n::locale_label(&self.runtime.locale)
      )
    );
    println!(
      "{}",
      ui::note_value(t!("install.entry.config_path"), &self.config_path)
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

      let mut question_context = StepQuestionContext::new(
        self.config,
        self.runtime,
        &self.config_path,
        &self.preflight_state,
      );
      step.collect(&mut question_context)?;
    }

    Ok(())
  }

  fn run_preflight(&mut self) -> Result<()> {
    for step in steps::registry() {
      let validated = {
        let mut preflight_context = StepPreflightContext::new(
          self.config,
          self.runtime,
          &self.config_path,
          &mut self.preflight_state,
        );
        step.preflight(&mut preflight_context)?
      };

      if validated {
        self.validated_steps.push(step.id());
      }
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
    println!("{}", ui::note(t!("install.plan.steps")));

    for (index, step) in plan.steps.iter().enumerate() {
      let status = if self.validated_steps.contains(&step.id) {
        InstallStepStatus::Completed
      } else {
        self
          .config
          .install_step_status(step.id)
          .unwrap_or(InstallStepStatus::Pending)
      };

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

    for step_id in self.validated_steps.clone() {
      self.persist_change(
        "install.execution.step",
        step_id.as_config_value(),
        |config| config.mark_install_step_completed(step_id),
      )?;
    }

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

    Ok(())
  }

  fn execute_installation(&mut self, plan: &InstallPlan) -> Result<()> {
    let registry = steps::registry();
    let mut completed_step_ids = Vec::new();

    for step in registry
      .into_iter()
      .filter(|step| plan.steps.iter().any(|planned| planned.id == step.id()))
    {
      let step_id = step.id();
      let step_title = plan
        .steps
        .iter()
        .find(|planned| planned.id == step_id)
        .map(|planned| planned.title.clone())
        .unwrap_or_else(|| step_id.as_config_value().to_string());

      if self.config.install_step_status(step_id) == Some(InstallStepStatus::Completed) {
        println!(
          "{}",
          ui::note(t!("install.execution.step_skipped", step = step_title))
        );
        completed_step_ids.push(step_id);
        continue;
      }

      println!();
      println!(
        "{}",
        ui::section(t!("install.execution.step_running", step = step_title))
      );

      self.persist_change(
        "install.execution.step",
        step_id.as_config_value(),
        |config| config.mark_install_step_started(step_id),
      )?;

      let result = {
        let mut execution_context = StepExecutionContext::new(
          self.config,
          self.runtime,
          &self.config_path,
          &self.preflight_state,
        );
        step.install(&mut execution_context)
      };

      match result {
        Ok(()) => {
          self.persist_change(
            "install.execution.step",
            step_id.as_config_value(),
            |config| config.mark_install_step_completed(step_id),
          )?;
          completed_step_ids.push(step_id);
          println!(
            "{}",
            ui::success(t!("install.execution.step_done", step = step_title))
          );
        }
        Err(error) => {
          let error_text = error.to_string();

          self.persist_change("install.execution.step", &error_text, |config| {
            config.mark_install_step_failed(step_id, error_text.clone())
          })?;
          let _ = self.record_failure(InstallFailureStage::Install, Some(step_id), &error);

          println!(
            "{}",
            ui::warning(t!(
              "install.execution.step_failed",
              step = step_title.as_str()
            ))
          );
          println!(
            "{}",
            ui::note(t!(
              "install.execution.step_failed_detail",
              error = error_text.as_str()
            ))
          );

          self.rollback_failed_installation(&completed_step_ids, step_id)?;

          return Err(error);
        }
      }
    }

    self.persist_change(
      "install.execution.phase",
      InstallExecutionPhase::Completed.as_config_value(),
      |config| config.set_install_phase(InstallExecutionPhase::Completed),
    )?;

    println!();
    println!("{}", ui::success(t!("install.execution.completed")));

    Ok(())
  }

  fn rollback_failed_installation(
    &mut self, completed_step_ids: &[crate::config::InstallStepId],
    failed_step_id: crate::config::InstallStepId,
  ) -> Result<()> {
    println!();
    println!("{}", ui::warning(t!("install.execution.rollback_started")));

    let registry = steps::registry();
    let mut failed_step_rollback_error = None;

    if let Some(step) = registry.iter().find(|step| step.id() == failed_step_id) {
      let mut execution_context = StepExecutionContext::new(
        self.config,
        self.runtime,
        &self.config_path,
        &self.preflight_state,
      );
      if let Err(error) = step.rollback(&mut execution_context) {
        let error_text = error.to_string();
        self.persist_change("install.execution.rollback", &error_text, |config| {
          config.mark_install_step_failed(failed_step_id, error_text.clone())
        })?;
        let _ = self.record_failure(InstallFailureStage::Rollback, Some(failed_step_id), &error);
        failed_step_rollback_error = Some(error_text);
      } else {
        self.persist_change(
          "install.execution.rollback",
          failed_step_id.as_config_value(),
          |config| config.reset_install_step(failed_step_id),
        )?;
      }
    }

    for step_id in completed_step_ids.iter().rev().copied() {
      let Some(step) = registry.iter().find(|step| step.id() == step_id) else {
        continue;
      };

      let step_title = plan_step_title(step_id);
      println!(
        "{}",
        ui::note(t!("install.execution.rollback_step", step = step_title))
      );

      let rollback_result = {
        let mut execution_context = StepExecutionContext::new(
          self.config,
          self.runtime,
          &self.config_path,
          &self.preflight_state,
        );
        step.rollback(&mut execution_context)
      };

      match rollback_result {
        Ok(()) => {
          self.persist_change(
            "install.execution.rollback",
            step_id.as_config_value(),
            |config| config.reset_install_step(step_id),
          )?;
        }
        Err(error) => {
          let error_text = error.to_string();
          self.persist_change("install.execution.rollback", &error_text, |config| {
            config.mark_install_step_failed(step_id, error_text.clone())
          })?;
          let _ = self.record_failure(InstallFailureStage::Rollback, Some(step_id), &error);

          return Err(anyhow!(
            "rollback failed for `{}` after installation error: {}",
            step_title,
            error_text
          ));
        }
      }
    }

    if let Some(error_text) = failed_step_rollback_error {
      return Err(anyhow!(
        "rollback failed for `{}` after installation error: {}",
        plan_step_title(failed_step_id),
        error_text
      ));
    }

    println!("{}", ui::success(t!("install.execution.rollback_done")));

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

  fn capture_stage<T, F>(
    &mut self, stage: InstallFailureStage, step_id: Option<InstallStepId>, action: F,
  ) -> Result<T>
  where
    F: FnOnce(&mut Self) -> Result<T>, {
    match action(self) {
      Ok(value) => Ok(value),
      Err(error) => {
        let _ = self.record_failure(stage, step_id, &error);
        Err(error.context(format!(
          "installer failed during {}",
          stage.as_config_value()
        )))
      }
    }
  }

  fn record_failure(
    &mut self, stage: InstallFailureStage, step_id: Option<InstallStepId>, error: &anyhow::Error,
  ) -> Result<()> {
    errors::record_install_failure(self.config, stage, step_id, error)?;

    debug!(
      config_path = %self.config_path,
      stage = stage.as_config_value(),
      step_id = step_id.map(InstallStepId::as_config_value),
      error = %error,
      "captured installer failure"
    );

    Ok(())
  }

  fn clear_recorded_failure(&mut self) -> Result<()> {
    errors::clear_install_failure(self.config)
  }
}

fn node_role_label(role: InstallTargetRole) -> String {
  match role {
    InstallTargetRole::ControlPlane => t!("install.node_role.options.control_plane").to_string(),
    InstallTargetRole::Worker => t!("install.node_role.options.worker").to_string(),
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

fn plan_step_title(step_id: crate::config::InstallStepId) -> String {
  step_id.as_config_value().to_string()
}
