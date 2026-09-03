mod confirm;
mod input;
mod single_select;

use anyhow::Result;

pub use self::{
  confirm::ConfirmCollector, input::InputCollector, single_select::SingleSelectCollector,
};

pub trait Collector<T> {
  fn collect(&self) -> Result<T>;
}
