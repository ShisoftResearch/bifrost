// a simple spin lock based async mutex

use futures::{Future, Async, Poll};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Weak};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, Ordering};
use backtrace::Backtrace;

#[derive(Clone)]
pub struct Mutex<T: ?Sized>{
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

pub struct AsyncMutexGuard<T: ?Sized> {
    mutex: Arc<MutexInner<T>>
}

pub struct MutexGuard<T: ?Sized> {
    mutex: Arc<MutexInner<T>>
}

impl RawMutex {
    pub fn new() -> RawMutex {
        RawMutex {
            state: AtomicU8::new(0)
        }
    }
    #[inline]
    pub fn try_lock(&self) -> bool {
        let bt = Backtrace::new();
        let success = self.state.compare_and_swap(
            0, 1, Ordering::SeqCst
        ) == 0;
        // println!("raw locking {}", success);
        return success;
    }
    #[inline]
    pub fn unlock(&self) -> bool {
        let success = self.state.compare_and_swap(
            1, 0, Ordering::SeqCst
        ) == 1;
        // println!("raw unlocking {}", success);
        return success
    }
}

impl <T: ?Sized> Future for AsyncMutexGuard <T> {
    type Item = MutexGuard<T>;
    type Error = ();

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // println!("pulling");
        if self.mutex.raw.try_lock() {
            // println!("locking");
            Ok(Async::Ready(MutexGuard {
                mutex: self.mutex.clone()
            }))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl <T> Mutex <T> {
    pub fn new(val: T) -> Mutex<T>  {
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
    #[inline]
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

impl <T: ?Sized> Drop for MutexGuard<T> {
    #[inline]
    fn drop(&mut self) {
        let success = self.mutex.raw.unlock();
        // println!("drop ulocking {}", success);
    }
}