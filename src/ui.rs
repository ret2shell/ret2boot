use std::{
  env,
  fmt::Display,
  io::{self, IsTerminal, Write},
};

pub enum BadgeTone {
  Pending,
  Active,
  Success,
  Danger,
}

pub fn banner_startup(product: &str, version: &str) -> String {
  format!(
    "{} {} {}",
    paint("[START UP]", "1;32"),
    paint(product, "1"),
    paint(version, "2")
  )
}

pub fn section(message: impl AsRef<str>) -> String {
  format!("{} {}", paint("==>", "1;36"), paint(message.as_ref(), "1"))
}

pub fn note(message: impl AsRef<str>) -> String {
  format!("{} {}", paint("note:", "1;34"), message.as_ref())
}

pub fn note_value(label: impl AsRef<str>, value: impl Display) -> String {
  note(format!("{}: {}", label.as_ref(), value))
}

pub fn warning(message: impl AsRef<str>) -> String {
  format!("{} {}", paint("warning:", "1;33"), message.as_ref())
}

pub fn success(message: impl AsRef<str>) -> String {
  format!("{} {}", paint("done:", "1;32"), message.as_ref())
}

pub fn status_tag(label: impl AsRef<str>, tone: BadgeTone) -> String {
  let code = match tone {
    BadgeTone::Pending => "1;33",
    BadgeTone::Active => "1;36",
    BadgeTone::Success => "1;32",
    BadgeTone::Danger => "1;31",
  };

  format!("[{}]", paint(label.as_ref(), code))
}

pub fn transient_line(message: impl AsRef<str>) {
  let mut stdout = io::stdout();

  if terminal_effects_enabled() {
    let _ = write!(stdout, "\r\x1b[2K{}", message.as_ref());
    let _ = stdout.flush();
  } else {
    let _ = writeln!(stdout, "{}", message.as_ref());
  }
}

pub fn transient_line_done(message: impl AsRef<str>) {
  let mut stdout = io::stdout();

  if terminal_effects_enabled() {
    let _ = writeln!(stdout, "\r\x1b[2K{}", message.as_ref());
    let _ = stdout.flush();
  } else {
    let _ = writeln!(stdout, "{}", message.as_ref());
  }
}

fn paint(text: &str, code: &str) -> String {
  if colors_enabled() {
    format!("\x1b[{code}m{text}\x1b[0m")
  } else {
    text.to_string()
  }
}

fn colors_enabled() -> bool {
  terminal_effects_enabled()
}

fn terminal_effects_enabled() -> bool {
  io::stdout().is_terminal()
    && env::var_os("NO_COLOR").is_none()
    && env::var("TERM")
      .map(|value| value != "dumb")
      .unwrap_or(true)
}
