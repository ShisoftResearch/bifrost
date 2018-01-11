pub mod time;
pub mod u8vec;
#[macro_use]
pub mod bindings;
pub mod math;
pub mod bincode;
pub mod future_parking_lot;

use futures::Future;


pub type FutureResult<V, E> = Future<Item = V, Error = E>;
pub type BoxFutureResult<V, E> = Box<FutureResult<V, E>>;