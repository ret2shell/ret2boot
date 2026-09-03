use std::{fmt::Write as _, fs::File, io::Read, path::Path};

use anyhow::{Context, Result, anyhow};
use sha2::{Digest, Sha256};

pub(crate) fn sha256_bytes_hex(bytes: impl AsRef<[u8]>) -> String {
  let digest = Sha256::digest(bytes);
  bytes_to_lower_hex(digest.as_ref())
}

pub(crate) fn sha256_file_hex(path: &Path) -> Result<String> {
  let mut file =
    File::open(path).with_context(|| format!("failed to open `{}`", path.display()))?;
  let mut digest = Sha256::new();
  let mut buffer = [0_u8; 8192];

  loop {
    let read = file
      .read(&mut buffer)
      .with_context(|| format!("failed to read `{}`", path.display()))?;

    if read == 0 {
      break;
    }

    digest.update(&buffer[..read]);
  }

  Ok(bytes_to_lower_hex(digest.finalize().as_ref()))
}

fn bytes_to_lower_hex(bytes: &[u8]) -> String {
  let mut encoded = String::with_capacity(bytes.len() * 2);

  for byte in bytes {
    write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
  }

  encoded
}

/// Parses a `<digest> <path>` (or bare digest) checksum line and returns the
/// lowercase hex digest, verifying that it references `expected_name`.
pub(crate) fn parse_sha256sum_line(contents: &str, expected_name: &str) -> Result<String> {
  let line = contents
    .lines()
    .map(str::trim)
    .find(|line| !line.is_empty())
    .ok_or_else(|| anyhow!("checksum asset for `{expected_name}` is empty"))?;

  if line.len() == 64 && line.chars().all(|ch| ch.is_ascii_hexdigit()) {
    return Ok(line.to_ascii_lowercase());
  }

  let mut parts = line.splitn(2, char::is_whitespace);
  let checksum = parts
    .next()
    .ok_or_else(|| anyhow!("checksum asset for `{expected_name}` is malformed"))?;
  let file_name = parts
    .next()
    .ok_or_else(|| anyhow!("checksum asset for `{expected_name}` is missing the file name"))?
    .trim()
    .trim_start_matches('*');
  let referenced_name = checksum_entry_base_name(file_name);

  if referenced_name != expected_name {
    return Err(anyhow!(
      "checksum asset references `{file_name}` instead of `{expected_name}`"
    ));
  }

  validate_sha256_digest(checksum, expected_name)
}

pub(crate) fn checksum_entry_base_name(path: &str) -> &str {
  path.rsplit(['/', '\\']).next().unwrap_or(path)
}

fn validate_sha256_digest(checksum: &str, name: &str) -> Result<String> {
  if checksum.len() != 64 || !checksum.chars().all(|ch| ch.is_ascii_hexdigit()) {
    return Err(anyhow!(
      "checksum asset for `{name}` does not contain a valid sha256 digest"
    ));
  }

  Ok(checksum.to_ascii_lowercase())
}

/// Shared installer User-Agent used by every HTTP client in this crate.
pub(crate) fn installer_user_agent() -> String {
  format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}
