use futures::{Future, Async, Poll};
use super::cpu_relax;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

#[derive(Clone)]
pub struct RwLock<T: ?Sized> {
    inner: Arc<RwLockInner<T>>
}

struct RwLockInner<T: ?Sized> {
    raw: RwLockRaw,
    data: UnsafeCell<T>
}

struct RwLockRaw {
    state: AtomicUsize
}

#[derive(Clone)]
pub struct AsyncRwLockReadGuard<T: ?Sized> {
    lock: Arc<RwLockInner<T>>
}

#[derive(Clone)]
pub struct AsyncRwLockWriteGuard<T: ?Sized> {
    lock: Arc<RwLockInner<T>>
}

#[derive(Clone)]
pub struct RwLockReadGuard<T: ?Sized> {
    lock: Arc<RwLockInner<T>>
}

#[derive(Clone)]
pub struct RwLockWriteGuard<T: ?Sized> {
    lock: Arc<RwLockInner<T>>
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

unsafe impl<T: Send> Send for RwLockInner<T> {}
unsafe impl<T: Send> Sync for RwLockInner<T> {}

impl <T: ?Sized> Future for AsyncRwLockReadGuard<T> {
    type Item = RwLockReadGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.lock.raw.try_read() {
            Ok(Async::Ready(RwLockReadGuard {
                lock: self.lock.clone()
            }))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl <T> Future for AsyncRwLockWriteGuard<T> {
    type Item = RwLockWriteGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.lock.raw.try_write() {
            Ok(Async::Ready(RwLockWriteGuard {
                lock: self.lock.clone()
            }))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl <T> RwLock <T> {
    pub fn new(val: T) -> RwLock<T> {
        RwLock {
            inner: parking_lot::RwLock::new(val)
        }
    }
    pub fn read_async(&self) -> AsyncRwLockReadGuard<T> {
        AsyncRwLockReadGuard {
            lock: self.inner.clone()
        }
    }
    pub fn write_async(&self) -> AsyncRwLockWriteGuard<T> {
        AsyncRwLockWriteGuard {
            lock: self.inner.clone()
        }
    }
    pub fn read(&self) -> RwLockReadGuard<T> {
        self.read_async().wait().unwrap()
    }
    pub fn write(&self) -> RwLockWriteGuard<T> {
        self.write_async().wait().unwrap()
    }
}

impl <T> RwLockInner<T> {

}

impl RwLockRaw {
    fn try_read(&self) -> bool  {
        let lc = self.state.load(Ordering::Relaxed);
        if lc == 1 {
            // write locked
            return false;
        } else {
            let next_count = if lc == 0 { 2 } else { lc + 1 };
            return self.state.compare_and_swap(lc, next_count, Ordering::Relaxed) == lc;
        }
    }
    fn try_write(&self) -> bool {
        self.state.compare_and_swap(0, 1, Ordering::Relaxed) == 0
    }
    fn unlock_read(&self) {
        loop {
            let lc = self.state.load(Ordering::Relaxed);
            if lc < 2 {
                // illegal state
                warn!("WRONG STATE FOR RWLOCK");
            } else {
                let next_count = if lc == 2 { 0 } else { lc - 1 };
                if self.state.compare_and_swap(lc, next_count, Ordering::Relaxed) == lc {
                    return;
                } else {
                    cpu_relax();
                }
            }
        }
    }
    fn unlock_write(&self) {
        self.state.compare_and_swap(1, 0, Ordering::Relaxed);
    }
}

impl <T> Deref for RwLockReadGuard<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe {
            &*self.lock.data.get()
        }
    }
}

impl <T> Deref for RwLockWriteGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        #[inline]
        unsafe {
            &*self.lock.data.get()
        }
    }
}

impl <T> DerefMut for RwLockWriteGuard<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe {
            &mut *self.lock.data.get()
        }
    }
}

impl <T: ?Sized> Drop for RwLockReadGuard<T> {
    #[inline]
    fn drop(&mut self) {
        self.lock.raw.unlock_read()
    }
}

impl <T: ?Sized> Drop for RwLockWriteGuard<T> {
    #[inline]
    fn drop(&mut self) {
        self.lock.raw.unlock_write()
    }
}