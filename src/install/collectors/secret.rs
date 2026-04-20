use anyhow::{Context, Result};
use dialoguer::{Password, theme::ColorfulTheme};

use super::Collector;

pub struct SecretCollector {
  prompt: String,
  existing_value: Option<String>,
}

impl SecretCollector {
  pub fn new(prompt: impl Into<String>) -> Self {
    Self {
      prompt: prompt.into(),
      existing_value: None,
    }
  }

  pub fn with_existing_value(mut self, existing_value: impl Into<String>) -> Self {
    self.existing_value = Some(existing_value.into());
    self
  }
}

impl Collector<String> for SecretCollector {
  fn collect(&self) -> Result<String> {
    let mut prompt = self.prompt.clone();
    if self.existing_value.is_some() {
      prompt.push_str(" (leave blank to keep the saved value)");
    }

    let value = Password::with_theme(&ColorfulTheme::default())
      .with_prompt(prompt)
      .allow_empty_password(self.existing_value.is_some())
      .interact()
      .with_context(|| format!("failed to collect secret input for `{}`", self.prompt))?;

    let value = value.trim().to_string();
    if value.is_empty()
      && let Some(existing_value) = &self.existing_value
    {
      return Ok(existing_value.clone());
    }

    if value.is_empty() {
      anyhow::bail!("the secret value cannot be empty");
    }

    Ok(value)
  }
}
