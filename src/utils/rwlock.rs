use std::sync::atomic::AtomicUsize;
use std::cell::UnsafeCell;
use std::pin::Pin;
use std::future::Future;
use futures::task::{Context, Poll};
use std::sync::atomic::Ordering::Relaxed;
use std::ops::{Deref, DerefMut};

pub struct RwLock<T> {
    semaphore: AtomicUsize,
    obj: UnsafeCell<T>
}

pub struct RwLockReadGuardFut<'a, T> {
    lock: Pin<&'a RwLock<T>>
}

pub struct RwLockWriteGuardFut<'a, T> {
    lock: Pin<&'a RwLock<T>>
}

pub struct RwLockReadGuard<'a, T> {
    lock: &'a RwLock<T>
}

pub struct RwLockWriteGuard<'a, T> {
    lock: &'a RwLock<T>
}

impl <T> RwLock<T> {
    pub fn new(obj: T) -> Self {
        Self {
            semaphore: AtomicUsize::new(1),
            obj: UnsafeCell::new(obj)
        }
    }

    pub fn read(&self) -> RwLockReadGuardFut<T> {
        RwLockReadGuardFut {
            lock: Pin::new(self)
        }
    }

    pub fn write(&self) -> RwLockWriteGuardFut<T> {
        RwLockWriteGuardFut {
            lock: Pin::new(self)
        }
    }
}

impl <'a, T> Future for RwLockWriteGuardFut<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.lock.semaphore.compare_and_awap(1, 0, Relaxed) {
            Poll::Ready(RwLockWriteGuard {
                lock: &*self.lock
            })
        } else {
            Poll::Pending
        }
    }
}

impl <'a, T> Future for RwLockReadGuardFut<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let count = self.lock.semaphore.load(Relaxed);
        if count >= 1 && self.lock.semaphore.compare_and_swap(count, count + 1, Relaxed) == count {
            Poll::Ready(RwLockReadGuard {
                lock: &*self.lock
            })
        } else {
            Poll::Pending
        }
    }
}

impl <'a, T> Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.obj.get() }
    }
}

impl <'a, T> Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.obj.get() }
    }
}

impl <'a, T> DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.obj.get() }
    }
}

impl <'a, T> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.semaphore.fetch_sub(1, Relaxed)
    }
}

impl <'a, T> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.semaphore.store(1, Relaxed)
    }
}

unsafe impl <T> Send for RwLock<T> {}
unsafe impl <T> Sync for RwLock<T> {}
