use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalCharset {
  Ascii,
  Utf8,
}

impl TerminalCharset {
  pub fn detect() -> Self {
    for key in ["LC_ALL", "LC_CTYPE", "LANG"] {
      if let Ok(value) = env::var(key) {
        let normalized = value.trim().to_ascii_lowercase();

        if normalized.is_empty() {
          continue;
        }

        if normalized.contains("utf-8") || normalized.contains("utf8") {
          return Self::Utf8;
        }

        return Self::Ascii;
      }
    }

    Self::Ascii
  }

  pub fn as_config_value(self) -> &'static str {
    match self {
      Self::Ascii => "ascii",
      Self::Utf8 => "utf-8",
    }
  }

  pub fn is_utf8(self) -> bool {
    matches!(self, Self::Utf8)
  }
}
