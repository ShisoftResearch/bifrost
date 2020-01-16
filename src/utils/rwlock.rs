use futures::task::{Context, Poll};
use std::cell::UnsafeCell;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

pub struct RwLock<T: Unpin> {
    semaphore: AtomicUsize,
    obj: UnsafeCell<T>,
}

pub struct RwLockReadGuardFut<'a, T: Unpin> {
    lock: Pin<&'a RwLock<T>>,
}

pub struct RwLockWriteGuardFut<'a, T: Unpin> {
    lock: Pin<&'a RwLock<T>>,
}

pub struct RwLockReadGuard<'a, T: Unpin> {
    lock: &'a RwLock<T>,
}

pub struct RwLockWriteGuard<'a, T: Unpin> {
    lock: &'a RwLock<T>,
}

impl<T: Unpin> RwLock<T> {
    pub fn new(obj: T) -> Self {
        Self {
            semaphore: AtomicUsize::new(1),
            obj: UnsafeCell::new(obj),
        }
    }

    pub fn read(&self) -> RwLockReadGuardFut<T> {
        RwLockReadGuardFut {
            lock: Pin::new(self),
        }
    }

    pub fn write(&self) -> RwLockWriteGuardFut<T> {
        RwLockWriteGuardFut {
            lock: Pin::new(self),
        }
    }

    pub fn read_blocked(&self) -> RwLockReadGuard<T> {
        while !self.read_acquire() {
            atomic::spin_loop_hint();
        }
        RwLockReadGuard { lock: self }
    }

    pub fn write_blocked(&self) -> RwLockWriteGuard<T> {
        while !self.write_acquire() {
            atomic::spin_loop_hint();
        }
        RwLockWriteGuard { lock: self }
    }

    fn read_acquire(&self) -> bool {
        let count = self.semaphore.load(Relaxed);
        count >= 1 && self.semaphore.compare_and_swap(count, count + 1, Relaxed) == count
    }

    fn write_acquire(&self) -> bool {
        self.semaphore.compare_and_swap(1, 0, Relaxed) == 1
    }
}

impl<'a, T: Unpin> Future for RwLockWriteGuardFut<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.lock.write_acquire() {
            Poll::Ready(RwLockWriteGuard {
                lock: self.lock.get_ref(),
            })
        } else {
            Poll::Pending
        }
    }
}

impl<'a, T: Unpin> Future for RwLockReadGuardFut<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.lock.read_acquire() {
            Poll::Ready(RwLockReadGuard {
                lock: self.lock.get_ref(),
            })
        } else {
            Poll::Pending
        }
    }
}

impl<'a, T: Unpin> Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.obj.get() }
    }
}

impl<'a, T: Unpin> Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.obj.get() }
    }
}

impl<'a, T: Unpin> DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.obj.get() }
    }
}

impl<'a, T: Unpin> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.semaphore.fetch_sub(1, Relaxed);
    }
}

impl<'a, T: Unpin> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.semaphore.store(1, Relaxed)
    }
}

unsafe impl<T> Send for RwLock<T> {}
unsafe impl<T> Sync for RwLock<T> {}
