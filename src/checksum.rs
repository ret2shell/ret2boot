use std::{fmt::Write as _, fs::File, io::Read, path::Path};

use anyhow::{Context, Result};
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
