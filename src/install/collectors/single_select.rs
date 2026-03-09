use anyhow::{Context, Result, bail};
use dialoguer::{Select, theme::ColorfulTheme};

use super::Collector;

pub struct SingleSelectCollector {
  prompt: String,
  options: Vec<String>,
  default: usize,
}

impl SingleSelectCollector {
  pub fn new(prompt: impl Into<String>, options: Vec<String>) -> Self {
    Self {
      prompt: prompt.into(),
      options,
      default: 0,
    }
  }

  pub fn with_default(mut self, default: usize) -> Self {
    self.default = default;
    self
  }

  pub fn collect_index(&self) -> Result<usize> {
    if self.options.is_empty() {
      bail!("single select collector requires at least one option");
    }

    let default = self.default.min(self.options.len() - 1);

    Select::with_theme(&ColorfulTheme::default())
      .with_prompt(&self.prompt)
      .default(default)
      .items(&self.options)
      .interact()
      .with_context(|| format!("failed to collect selection for `{}`", self.prompt))
  }
}

impl Collector<String> for SingleSelectCollector {
  fn collect(&self) -> Result<String> {
    let selected = self.collect_index()?;

    Ok(self.options[selected].clone())
  }
}
