use parking_lot;
use futures::{Future, Async, Poll};
use std::ops::{Deref};

pub struct RwLock<T> {
    inner: parking_lot::RwLock<T>
}

pub struct AsyncRwLockReadGuard<'a, T: 'a> {
    outer: &'a RwLock<T>
}

pub struct AsyncRwLockWriteGuard<'a, T: 'a> {
    outer: &'a RwLock<T>
}

impl <'a, T> Future for AsyncRwLockReadGuard<'a ,T> {
    type Item = parking_lot::RwLockReadGuard<'a, T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.outer.inner.try_read() {
            Some(guard) => Ok(Async::Ready(guard)),
            None => Ok(Async::NotReady)
        }
    }
}

impl <'a, T> Future for AsyncRwLockWriteGuard<'a ,T> {
    type Item = parking_lot::RwLockWriteGuard<'a, T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.outer.inner.try_write() {
            Some(guard) => Ok(Async::Ready(guard)),
            None => Ok(Async::NotReady)
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
            outer: self
        }
    }
    pub fn write_async(&self) -> AsyncRwLockWriteGuard<T> {
        AsyncRwLockWriteGuard {
            outer: self
        }
    }
}

impl <T> Deref for RwLock<T> {
    type Target = parking_lot::RwLock<T>;
    #[inline]
    fn deref(&self) -> &parking_lot::RwLock<T> {
        &self.inner
    }
}