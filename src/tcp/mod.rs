use bifrost_hasher::hash_str;

pub mod client;
pub mod server;
pub mod shortcut;

pub static STANDALONE_ADDRESS: &'static str = "STANDALONE";

lazy_static! {
    pub static ref STANDALONE_ADDRESS_STRING: String = String::from(STANDALONE_ADDRESS);
    pub static ref STANDALONE_SERVER_ID: u64 = hash_str(&STANDALONE_ADDRESS_STRING);
}

#[cfg(test)]
lazy_static! {
    /// Serializes every test that touches the standalone identity.
    ///
    /// `STANDALONE_ADDRESS` is one process-global pseudo-address with one
    /// shortcut registration and one client-pool entry, so tests using it are not
    /// independent — they operate on the same object. Two distinct failures came
    /// out of that, measured 2026-08-17:
    ///
    /// - `rpc`'s pool tests install one-shot hooks and then wait on channels. Run
    ///   concurrently, each could consume the other's hook and block on a sender
    ///   that had already been dropped, which **hung the whole suite** rather than
    ///   failing it (79 minutes before the process was killed).
    /// - `tcp::client`'s `test_client_standalone_address` asserts that connecting
    ///   to STANDALONE *fails*, which is only true while no standalone server has
    ///   a shortcut registered. Any concurrent standalone server makes the connect
    ///   succeed through the shortcut and the assertion false.
    ///
    /// It lives here rather than in `rpc` because the thing it guards is the
    /// standalone identity, which is a `tcp` concept, and both modules need it.
    ///
    /// A `tokio` mutex rather than a `std` one: guards are held across awaits, and
    /// a panicking test releases by unwinding instead of poisoning the lock for
    /// every test after it.
    pub(crate) static ref STANDALONE_TEST_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::new(());
}
