mod confirm;
mod input;
mod multi_select;
mod secret;
mod single_select;

use anyhow::Result;

#[allow(unused_imports)]
pub use self::input::InputCollector;
#[allow(unused_imports)]
pub use self::multi_select::MultiSelectCollector;
pub use self::{
  confirm::ConfirmCollector, secret::SecretCollector, single_select::SingleSelectCollector,
};

pub trait Collector<T> {
  fn collect(&self) -> Result<T>;
}
