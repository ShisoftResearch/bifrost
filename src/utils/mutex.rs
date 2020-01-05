use std::sync::atomic::AtomicBool;
use std::cell::UnsafeCell;
use std::pin::Pin;
use futures::Future;
use futures::task::{Context, Poll};
use std::sync::atomic::Ordering::Relaxed;
use futures::future::Ready;
use std::ops::{Deref, DerefMut};

pub struct Mutex<T> {
    semaphore: AtomicBool,
    obj: UnsafeCell<T>
}

pub struct MutexGuardFut<'a, T> {
    lock: Pin<&'a Mutex<T>>
}

pub struct MutexGuard<'a, T> {
    lock: &'a Mutex<T>
}

impl <T> Mutex<T> {
    pub fn new(obj: T) -> Self {
        Self {
            semaphore: AtomicBool::new(false),
            obj: UnsafeCell::new(obj)
        }
    }

    pub fn lock(&self) -> MutexGuardFut<T> {
        MutexGuardFut {
            lock: Pin::new(self)
        }
    }
}

impl <'a, T> Future for MutexGuardFut<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.lock.semaphore.compare_and_swap(false, true, Relaxed) == false {
            Poll::Ready(MutexGuard {
                lock: &*self.lock
            })
        } else {
            Poll::Pending
        }
    } 
}

impl <'a, T> Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe  { &*self.lock.obj.get() }
    }
}

impl <'a, T> DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe  { &mut *self.lock.obj.get() }
    }
}

impl <'a, T> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.semaphore.store(false, Relaxed);
    }
}

unsafe impl <T> Send for Mutex<T> {}
unsafe impl <T> Sync for Mutex<T> {}