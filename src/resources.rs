use anyhow::{Result, anyhow};
use rust_embed::RustEmbed;

#[allow(dead_code)]
#[derive(RustEmbed)]
#[folder = "src/resources/"]
pub struct EmbeddedResources;

#[allow(dead_code)]
pub fn load_utf8(path: &str) -> Result<String> {
  let asset =
    EmbeddedResources::get(path).ok_or_else(|| anyhow!("embedded resource `{path}` not found"))?;

  std::str::from_utf8(asset.data.as_ref())
    .map(str::to_owned)
    .map_err(|error| anyhow!("embedded resource `{path}` is not valid UTF-8: {error}"))
}
