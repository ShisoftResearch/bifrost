use parking_lot;

pub mod mutex;
pub mod rwlock;

pub use self::mutex::{Mutex, AsyncMutexGuard};
pub use self::rwlock::{RwLock, AsyncRwLockWriteGuard, AsyncRwLockReadGuard};