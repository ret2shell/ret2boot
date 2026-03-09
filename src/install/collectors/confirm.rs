use anyhow::{Context, Result};
use dialoguer::{Confirm, theme::ColorfulTheme};

use super::Collector;

pub struct ConfirmCollector {
  prompt: String,
  default: bool,
}

impl ConfirmCollector {
  pub fn new(prompt: impl Into<String>, default: bool) -> Self {
    Self {
      prompt: prompt.into(),
      default,
    }
  }
}

impl Collector<bool> for ConfirmCollector {
  fn collect(&self) -> Result<bool> {
    Confirm::with_theme(&ColorfulTheme::default())
      .with_prompt(&self.prompt)
      .default(self.default)
      .interact()
      .with_context(|| format!("failed to collect confirmation for `{}`", self.prompt))
  }
}
