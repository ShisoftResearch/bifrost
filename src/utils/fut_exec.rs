use futures::prelude::*;
use futures_cpupool::{Builder, CpuPool};
use num_cpus;

lazy_static! {
    static ref POOL: CpuPool = Builder::new()
        .name_prefix("Bifrost Shared Exec Pool".to_string())
        .pool_size(num_cpus::get())
        .create();
}

struct SendFuture<F>
where
    F: Future + 'static,
    F::Item: Send + 'static,
    F::Error: Send + 'static,
{
    inner: F,
}

impl<F> Future for SendFuture<F>
where
    F: Future + 'static,
    F::Item: Send + 'static,
    F::Error: Send + 'static,
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        self.inner.poll()
    }
}

unsafe impl<F> Send for SendFuture<F>
where
    F: Future + 'static,
    F::Item: Send + 'static,
    F::Error: Send + 'static,
{
}

pub fn exec<F>(future: F) -> impl Future<Item = F::Item, Error = F::Error>
where
    F: Future + 'static,
    F::Item: Send + 'static,
    F::Error: Send + 'static,
{
    POOL.spawn(SendFuture { inner: future })
}

pub fn wait<F>(future: F) -> Result<F::Item, F::Error>
where
    F: Future + 'static,
    F::Item: Send + 'static,
    F::Error: Send + 'static,
{
    exec(future).wait()
}
