// a simple spin lock based async mutex

use parking_lot::{Mutex as PlMutex};
use futures::{Future, Async, Poll};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone)]
pub struct Mutex<T>{
    inner: Arc<MutexInner<T>>
}

struct MutexInner<T: ?Sized> {
    raw: RawMutex,
    data: UnsafeCell<T>
}

struct RawMutex {
    state: AtomicU8
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

unsafe impl<T: Send> Send for MutexInner<T> {}
unsafe impl<T: Send> Sync for MutexInner<T> {}

pub struct AsyncMutexGuard<T> {
    mutex: Arc<MutexInner<T>>,
}

pub struct MutexGuard<T> {
    mutex: Arc<MutexInner<T>>
}

impl RawMutex {
    pub fn new() -> RawMutex {
        RawMutex {
            state: AtomicU8::new(0)
        }
    }
    pub fn try_lock(&self) -> bool {
        let success = self.state.compare_exchange_weak(
            0, 1, Ordering::Acquire, Ordering::Relaxed
        ).is_ok();
        println!("raw locking {}", success);
        return success;
    }
    pub fn unlock(&self) -> bool {
        let success = self.state.compare_exchange_weak(
            1, 0, Ordering::Acquire, Ordering::Relaxed
        ).is_ok();
        println!("raw unlocking {}", success);
        return success
    }
}

impl <T> Future for AsyncMutexGuard <T> {
    type Item = MutexGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        println!("pulling");
        if self.mutex.raw.try_lock() {
            println!("locking");
            Ok(Async::Ready(MutexGuard {
                mutex: self.mutex.clone()
            }))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl <T> Mutex <T> {
    pub fn new(val: T) -> Mutex<T> {
        Mutex {
            inner: Arc::new(MutexInner {
                raw: RawMutex::new(),
                data: UnsafeCell::new(val)
            })
        }
    }
    pub fn lock_async(&self) -> AsyncMutexGuard<T> {
        AsyncMutexGuard {
            mutex: self.inner.clone()
        }
    }
    pub fn lock(&self) -> MutexGuard<T> {
        self.lock_async().wait().unwrap()
    }
}

impl <T> Deref for MutexGuard<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.mutex.data.get() }
    }
}

impl <T> DerefMut for MutexGuard<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl <T> Drop for MutexGuard<T> {
    fn drop(&mut self) {
        println!("ulocking {}", self.mutex.raw.unlock());
    }
}