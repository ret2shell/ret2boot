use anyhow::{Context, Result, bail};
use dialoguer::{MultiSelect, theme::ColorfulTheme};

use super::Collector;

#[allow(dead_code)]
pub struct MultiSelectCollector {
  prompt: String,
  options: Vec<String>,
  defaults: Vec<bool>,
}

#[allow(dead_code)]
impl MultiSelectCollector {
  pub fn new(prompt: impl Into<String>, options: Vec<String>) -> Self {
    let defaults = vec![false; options.len()];

    Self {
      prompt: prompt.into(),
      options,
      defaults,
    }
  }

  pub fn with_defaults(mut self, selected_indexes: &[usize]) -> Self {
    for index in selected_indexes {
      if let Some(default) = self.defaults.get_mut(*index) {
        *default = true;
      }
    }

    self
  }
}

impl Collector<Vec<String>> for MultiSelectCollector {
  fn collect(&self) -> Result<Vec<String>> {
    if self.options.is_empty() {
      bail!("multi select collector requires at least one option");
    }

    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
      .with_prompt(&self.prompt)
      .items(&self.options)
      .defaults(&self.defaults)
      .interact()
      .with_context(|| format!("failed to collect selections for `{}`", self.prompt))?;

    Ok(
      selected
        .into_iter()
        .map(|index| self.options[index].to_string())
        .collect(),
    )
  }
}
