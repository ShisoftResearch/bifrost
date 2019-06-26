// a simple spin lock based async mutex

use super::cpu_relax;
use futures::prelude::*;
use futures::task;
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub struct Mutex<T: Sized> {
    inner: Arc<MutexInner<T>>,
}

struct MutexInner<T: Sized> {
    raw: RawMutex,
    data: UnsafeCell<T>,
}

struct RawMutex {
    state: AtomicBool,
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

unsafe impl<T: Send> Send for MutexInner<T> {}
unsafe impl<T: Send> Sync for MutexInner<T> {}

pub struct AsyncMutexGuard<T: Sized> {
    mutex: Arc<MutexInner<T>>,
}

pub struct MutexGuard<T: Sized> {
    mutex: Arc<MutexInner<T>>,
}

impl RawMutex {
    fn new() -> RawMutex {
        RawMutex {
            state: AtomicBool::new(false),
        }
    }

    fn try_lock(&self) -> bool {
        let prev_val = self.state.compare_and_swap(false, true, Ordering::Relaxed);
        let success = prev_val == false;
        //println!("raw locking {}", prev_val);
        return success;
    }

    fn lock(&self) {
        while self.state.compare_and_swap(false, true, Ordering::Relaxed) {
            while self.state.load(Ordering::Relaxed) {
                cpu_relax();
            }
        }
    }

    fn unlock(&self) {
        assert!(self.state.compare_and_swap(true, false, Ordering::Relaxed))
    }
}

impl<T: Sized> Future for AsyncMutexGuard<T> {
    type Item = MutexGuard<T>;
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.mutex.raw.try_lock() {
            Ok(Async::Ready(MutexGuard {
                mutex: self.mutex.clone(),
            }))
        } else {
            task::current().notify();
            Ok(Async::NotReady)
        }
    }
}

impl<T> Mutex<T> {
    pub fn new(val: T) -> Mutex<T> {
        Mutex {
            inner: Arc::new(MutexInner {
                raw: RawMutex::new(),
                data: UnsafeCell::new(val),
            }),
        }
    }
    pub fn lock_async(&self) -> AsyncMutexGuard<T> {
        AsyncMutexGuard {
            mutex: self.inner.clone(),
        }
    }
    pub fn lock(&self) -> MutexGuard<T> {
        self.inner.raw.lock();
        MutexGuard {
            mutex: self.inner.clone(),
        }
    }
}

impl<T> MutexGuard<T> {
    #[inline]
    pub fn mutate(&self) -> &mut T {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T> Deref for MutexGuard<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T> DerefMut for MutexGuard<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.mutate()
    }
}

impl<T: Sized> Drop for MutexGuard<T> {
    #[inline]
    fn drop(&mut self) {
        self.mutex.raw.unlock();
    }
}

#[cfg(test)]
mod tests {

    extern crate rayon;

    #[derive(Eq, PartialEq, Debug)]
    struct NonCopy(i32);

    use self::rayon::iter::IntoParallelIterator;
    use self::rayon::prelude::*;
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread;
    use utils::fut_exec::wait;

    #[test]
    fn basic() {
        let map: HashMap<i32, i32> = HashMap::new();
        let mutex = Mutex::new(map);
        mutex
            .lock_async()
            .map(|mut m| {
                m.insert(1, 2);
                m.insert(2, 3)
            })
            .wait()
            .unwrap();
        mutex
            .lock_async()
            .map(|m| assert_eq!(m.get(&1).unwrap(), &2))
            .wait()
            .unwrap();
        assert_eq!(*mutex.lock().get(&1).unwrap(), 2);
        assert_eq!(*mutex.lock().get(&2).unwrap(), 3);
    }

    #[test]
    fn parallel() {
        let map: HashMap<i32, i32> = HashMap::new();
        let mutex = Mutex::new(map);

        (0..1000).collect::<Vec<_>>().into_par_iter().for_each(|i| {
            wait(mutex.lock_async().map(move |mut m| {
                m.insert(i, i + 1);
                m.insert(i + 1, i + 2);
            }))
            .unwrap();

            wait(
                mutex
                    .lock_async()
                    .map(move |m| assert_eq!(m.get(&i).unwrap(), &(i + 1))),
            )
            .unwrap();
        });

        (0..1000).for_each(|i| {
            assert_eq!(*mutex.lock().get(&i).unwrap(), i + 1);
            assert_eq!(*mutex.lock().get(&(i + 1)).unwrap(), i + 2);
        })
    }

    #[test]
    fn smoke() {
        let m = Mutex::new(());
        drop(m.lock());
        drop(m.lock());
    }

    #[test]
    fn test_mutex_arc_nested() {
        // Tests nested mutexes and access
        // to underlying data.
        let arc = Arc::new(Mutex::new(1));
        let arc2 = Arc::new(Mutex::new(arc));
        let (tx, rx) = channel();
        let _t = thread::spawn(move || {
            let lock = arc2.lock();
            let lock2 = lock.lock();
            assert_eq!(*lock2, 1);
            tx.send(()).unwrap();
        });
        rx.recv().unwrap();
    }

    #[test]
    fn test_mutexguard_send() {
        fn send<T: Send>(_: T) {}

        let mutex = Mutex::new(());
        send(mutex.lock());
    }

    #[test]
    fn test_mutexguard_sync() {
        fn sync<T: Sync>(_: T) {}

        let mutex = Mutex::new(());
        sync(mutex.lock());
    }

    #[test]
    fn lots_and_lots() {
        const J: u32 = 1000;
        const K: u32 = 3;

        let m = Arc::new(Mutex::new(0));

        fn inc(m: &Mutex<u32>) {
            for _ in 0..J {
                *m.lock() += 1;
            }
        }

        let (tx, rx) = channel();
        for _ in 0..K {
            let tx2 = tx.clone();
            let m2 = m.clone();
            thread::spawn(move || {
                inc(&m2);
                tx2.send(()).unwrap();
            });
            let tx2 = tx.clone();
            let m2 = m.clone();
            thread::spawn(move || {
                inc(&m2);
                tx2.send(()).unwrap();
            });
        }

        drop(tx);
        for _ in 0..2 * K {
            rx.recv().unwrap();
        }
        assert_eq!(*m.lock(), J * K * 2);
    }
}
