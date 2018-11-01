use futures::Future;
use futures_cpupool::{Builder, CpuPool};
use num_cpus;

lazy_static! {
    static ref POOL: CpuPool = Builder::new()
        .name_prefix("Bifrost Shared Exec Pool".to_string())
        .pool_size(num_cpus::get())
        .create();
}

pub fn exec<F>(future: F) -> impl Future<Item = F::Item, Error = F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
{
    POOL.spawn(future)
}

pub fn wait<F>(future: F) -> Result<F::Item, F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
{
    exec(future).wait()
}