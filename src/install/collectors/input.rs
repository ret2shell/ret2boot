use anyhow::{Context, Result};
use dialoguer::{Input, theme::ColorfulTheme};
use rust_i18n::t;

use super::Collector;

#[allow(dead_code)]
pub struct InputCollector {
  prompt: String,
  default: Option<String>,
}

#[allow(dead_code)]
impl InputCollector {
  pub fn new(prompt: impl Into<String>) -> Self {
    Self {
      prompt: prompt.into(),
      default: None,
    }
  }

  pub fn with_default(mut self, default: impl Into<String>) -> Self {
    self.default = Some(default.into());
    self
  }
}

impl Collector<String> for InputCollector {
  fn collect(&self) -> Result<String> {
    let theme = ColorfulTheme::default();
    let input = Input::<String>::with_theme(&theme).with_prompt(&self.prompt);
    let input = match &self.default {
      Some(default) => input.default(default.clone()),
      None => input,
    };

    input
      .validate_with(|value: &String| -> std::result::Result<(), String> {
        if value.trim().is_empty() {
          Err(t!("install.errors.empty_value").to_string())
        } else {
          Ok(())
        }
      })
      .interact_text()
      .with_context(|| format!("failed to collect input for `{}`", self.prompt))
  }
}
