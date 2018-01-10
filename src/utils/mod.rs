pub mod time;
pub mod u8vec;
#[macro_use]
pub mod bindings;
pub mod math;
pub mod bincode;
pub mod future_parking_lot;

use futures::Future;

pub type FutureResult<V, E> = Box<Future<Item = V, Error = E>>;