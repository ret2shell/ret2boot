use std::env;

use rust_i18n::t;

pub const DEFAULT_LOCALE: &str = "en-us";
pub const SUPPORTED_LOCALES: [&str; 4] = ["zh-hans", "zh-hant", "en-us", "ja-jp"];

#[derive(Debug, Clone)]
pub struct LocaleOption {
  pub id: &'static str,
  pub label: String,
}

pub fn set_locale(locale: &str) {
  rust_i18n::set_locale(locale);
}

pub fn current_locale() -> String {
  rust_i18n::locale().to_string()
}

pub fn supported_locales() -> &'static [&'static str] {
  &SUPPORTED_LOCALES
}

pub fn system_locale() -> Option<String> {
  ["LC_ALL", "LC_MESSAGES", "LC_CTYPE", "LANG"]
    .into_iter()
    .filter_map(|key| env::var(key).ok())
    .find_map(|value| normalize_locale(&value))
}

pub fn normalize_locale(raw: &str) -> Option<String> {
  let normalized = raw
    .split('.')
    .next()
    .unwrap_or(raw)
    .replace('_', "-")
    .to_lowercase();

  if SUPPORTED_LOCALES.contains(&normalized.as_str()) {
    return Some(normalized);
  }

  match normalized.as_str() {
    locale
      if locale == "zh"
        || locale.starts_with("zh-cn")
        || locale.starts_with("zh-sg")
        || locale.starts_with("zh-hans") =>
    {
      Some("zh-hans".to_string())
    }
    locale
      if locale.starts_with("zh-tw")
        || locale.starts_with("zh-hk")
        || locale.starts_with("zh-mo")
        || locale.starts_with("zh-hant") =>
    {
      Some("zh-hant".to_string())
    }
    locale if locale.starts_with("ja") => Some("ja-jp".to_string()),
    locale if locale.starts_with("en") => Some("en-us".to_string()),
    _ => None,
  }
}

pub fn locale_options() -> Vec<LocaleOption> {
  SUPPORTED_LOCALES
    .into_iter()
    .map(|id| LocaleOption {
      id,
      label: language_label(id),
    })
    .collect()
}

fn language_label(locale: &str) -> String {
  match locale {
    "zh-hans" => t!("startup.language.options.zh_hans").to_string(),
    "zh-hant" => t!("startup.language.options.zh_hant").to_string(),
    "en-us" => t!("startup.language.options.en_us").to_string(),
    "ja-jp" => t!("startup.language.options.ja_jp").to_string(),
    _ => locale.to_string(),
  }
}
