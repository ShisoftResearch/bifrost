use super::cpu_relax;
use futures::{Async, Future, Poll};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use futures::task;

#[derive(Clone)]
pub struct RwLock<T: Sized> {
    inner: Arc<RwLockInner<T>>,
}

struct RwLockInner<T: Sized> {
    raw: RwLockRaw,
    data: UnsafeCell<T>,
    tasks: ::parking_lot::Mutex<Vec<task::Task>>,
}

impl <T> RwLockInner <T> {
    fn notify_all(&self) {
        let mut tasks = self.tasks.lock();
        for t in &*tasks {
            t.notify();
        }
        tasks.clear();
    }
    fn add_task(&self, task: task::Task) {
        self.tasks.lock().push(task)
    }
}

struct RwLockRaw {
    state: AtomicUsize,
}

#[derive(Clone)]
pub struct AsyncRwLockReadGuard<T: Sized> {
    lock: Arc<RwLockInner<T>>,
}

#[derive(Clone)]
pub struct AsyncRwLockWriteGuard<T: Sized> {
    lock: Arc<RwLockInner<T>>,
}

struct RwLockReadGuardInner<T: Sized> {
    lock: Arc<RwLockInner<T>>,
}

struct RwLockWriteGuardInner<T: Sized> {
    lock: Arc<RwLockInner<T>>,
}

#[derive(Clone)]
pub struct RwLockReadGuard<T: Sized> {
    inner: Arc<RwLockReadGuardInner<T>>,
}

#[derive(Clone)]
pub struct RwLockWriteGuard<T: Sized> {
    inner: Arc<RwLockWriteGuardInner<T>>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

unsafe impl<T: Send> Send for RwLockInner<T> {}
unsafe impl<T: Send> Sync for RwLockInner<T> {}

impl<T: Sized> Future for AsyncRwLockReadGuard<T> {
    type Item = RwLockReadGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.lock.add_task(task::current());
        self.lock.notify_all();
        if self.lock.raw.try_read() {
            Ok(Async::Ready(RwLockReadGuard::new(&self.lock)))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl<T> Future for AsyncRwLockWriteGuard<T> {
    type Item = RwLockWriteGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.lock.add_task(task::current());
        self.lock.notify_all();
        if self.lock.raw.try_write() {
            Ok(Async::Ready(RwLockWriteGuard::new(&self.lock)))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl<T> RwLock<T> {
    pub fn new(val: T) -> RwLock<T> {
        RwLock {
            inner: RwLockInner::new(val),
        }
    }
    pub fn read_async(&self) -> AsyncRwLockReadGuard<T> {
        AsyncRwLockReadGuard {
            lock: self.inner.clone(),
        }
    }
    pub fn write_async(&self) -> AsyncRwLockWriteGuard<T> {
        AsyncRwLockWriteGuard {
            lock: self.inner.clone(),
        }
    }
    pub fn read(&self) -> RwLockReadGuard<T> {
        while !self.inner.raw.try_read() {
            cpu_relax()
        }
        RwLockReadGuard::new(&self.inner)
    }
    pub fn write(&self) -> RwLockWriteGuard<T> {
        while !self.inner.raw.try_write() {
            cpu_relax()
        }
        RwLockWriteGuard::new(&self.inner)
    }
}

impl<T> RwLockInner<T> {
    pub fn new(val: T) -> Arc<RwLockInner<T>> {
        Arc::new(RwLockInner {
            raw: RwLockRaw::new(),
            data: UnsafeCell::new(val),
            tasks: ::parking_lot::Mutex::new(Vec::new())
        })
    }
}

impl RwLockRaw {
    fn new() -> RwLockRaw {
        RwLockRaw {
            state: AtomicUsize::new(0),
        }
    }
    fn try_read(&self) -> bool {
        let lc = self.state.load(Ordering::Relaxed);
        if lc == 1 {
            // write locked
            return false;
        } else {
            let next_count = if lc == 0 { 2 } else { lc + 1 };
            return self
                .state
                .compare_and_swap(lc, next_count, Ordering::Relaxed)
                == lc;
        }
    }
    fn try_write(&self) -> bool {
        self.state.compare_and_swap(0, 1, Ordering::Relaxed) == 0
    }
    fn unlock_read(&self) {
        loop {
            let lc = self.state.load(Ordering::Relaxed);
            assert!(lc > 1);
            let next_count = if lc == 2 { 0 } else { lc - 1 };
            if self
                .state
                .compare_and_swap(lc, next_count, Ordering::Relaxed)
                == lc
            {
                return;
            } else {
                cpu_relax();
            }
        }
    }
    fn unlock_write(&self) {
        assert_eq!(self.state.compare_and_swap(1, 0, Ordering::Relaxed), 1);
    }
}

impl<T: Sized> RwLockReadGuard<T> {
    fn new(raw: &Arc<RwLockInner<T>>) -> RwLockReadGuard<T> {
        RwLockReadGuard {
            inner: Arc::new(RwLockReadGuardInner { lock: raw.clone() }),
        }
    }
}

impl<T> RwLockWriteGuard<T> {
    fn new(raw: &Arc<RwLockInner<T>>) -> RwLockWriteGuard<T> {
        RwLockWriteGuard {
            inner: Arc::new(RwLockWriteGuardInner { lock: raw.clone() }),
        }
    }
}

impl<T> Deref for RwLockReadGuardInner<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> Deref for RwLockWriteGuardInner<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuardInner<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.mutate()
    }
}

impl<T> RwLockWriteGuardInner<T> {
    #[inline]
    pub fn mutate(&self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> RwLockWriteGuard<T> {
    #[inline]
    pub fn mutate(&self) -> &mut T {
        self.inner.mutate()
    }
}

impl<T> Drop for RwLockReadGuardInner<T> {
    #[inline]
    fn drop(&mut self) {
        self.lock.raw.unlock_read()
    }
}

impl<T> Drop for RwLockWriteGuardInner<T> {
    #[inline]
    fn drop(&mut self) {
        self.lock.raw.unlock_write()
    }
}

impl<T> Deref for RwLockReadGuard<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl<T> Deref for RwLockWriteGuard<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl<T> DerefMut for RwLockWriteGuard<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.inner.mutate()
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    use self::rand::Rng;
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[derive(Eq, PartialEq, Debug)]
    struct NonCopy(i32);

    #[test]
    fn smoke() {
        let l = RwLock::new(());
        drop(l.read());
        drop(l.write());
        drop((l.read(), l.read()));
        drop(l.write());
    }

    #[test]
    fn frob() {
        const N: u32 = 10;
        const M: u32 = 1000;

        let r = Arc::new(RwLock::new(()));

        let (tx, rx) = channel::<()>();
        for _ in 0..N {
            let tx = tx.clone();
            let r = r.clone();
            thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _ in 0..M {
                    if rng.gen_weighted_bool(N) {
                        drop(r.write());
                    } else {
                        drop(r.read());
                    }
                }
                drop(tx);
            });
        }
        drop(tx);
        let _ = rx.recv();
    }

    #[test]
    fn test_rw_arc_no_poison_wr() {
        let arc = Arc::new(RwLock::new(1));
        let arc2 = arc.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let _lock = arc2.write();
            panic!();
        });
        let lock = arc.read();
        assert_eq!(*lock, 1);
    }

    #[test]
    fn test_rw_arc_no_poison_ww() {
        let arc = Arc::new(RwLock::new(1));
        let arc2 = arc.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let _lock = arc2.write();
            panic!();
        });
        let lock = arc.write();
        assert_eq!(*lock, 1);
    }

    #[test]
    fn test_rw_arc() {
        let arc = Arc::new(RwLock::new(0));
        let arc2 = arc.clone();
        let (tx, rx) = channel();

        thread::spawn(move || {
            let mut lock = arc2.write();
            for _ in 0..10 {
                let tmp = *lock;
                *lock = -1;
                thread::yield_now();
                *lock = tmp + 1;
            }
            tx.send(()).unwrap();
        });

        // Readers try to catch the writer in the act
        let mut children = Vec::new();
        for _ in 0..5 {
            let arc3 = arc.clone();
            children.push(thread::spawn(move || {
                let lock = arc3.read();
                assert!(*lock >= 0);
            }));
        }

        // Wait for children to pass their asserts
        for r in children {
            assert!(r.join().is_ok());
        }

        // Wait for writer to finish
        rx.recv().unwrap();
        let lock = arc.read();
        assert_eq!(*lock, 10);
    }

    #[test]
    fn clone_guard() {
        let lock = RwLock::new(0);
        {
            let guard1 = lock.read();
            let guard2 = guard1.clone();
        }
        {
            let mut guard3 = lock.write();
            *guard3 = 3;
        }
        let guard4 = lock.read();
        assert_eq!(*guard4, 3);
    }
}
