use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use thread_id;

pub struct Binding<T>
where
    T: Clone,
{
    default: T,
    thread_vals: RwLock<HashMap<usize, T>>,
}

impl<T> Binding<T>
where
    T: Clone,
{
    pub fn new(default: T) -> Binding<T> {
        Binding {
            default,
            thread_vals: RwLock::new(HashMap::new()),
        }
    }
    pub fn get(&self) -> T {
        let tid = thread_id::get();
        let thread_map = self.thread_vals.read();
        match thread_map.get(&tid) {
            Some(v) => v.clone(),
            None => self.default.clone(),
        }
    }
    pub fn set(&self, val: T) {
        let tid = thread_id::get();
        let mut thread_map = self.thread_vals.write();
        thread_map.insert(tid, val);
    }
    pub fn del(&self) {
        let tid = thread_id::get();
        let mut thread_map = self.thread_vals.write();
        thread_map.remove(&tid);
    }
}

pub struct RefBinding<T> {
    bind: Binding<Arc<T>>,
}
impl<T> RefBinding<T> {
    pub fn new(default: T) -> RefBinding<T> {
        RefBinding {
            bind: Binding::new(Arc::new(default)),
        }
    }
    pub fn get(&self) -> Arc<T> {
        self.bind.get()
    }
    pub fn set(&self, val: T) {
        self.bind.set(Arc::new(val))
    }
    pub fn del(&self) {
        self.bind.del()
    }
}

#[macro_export]
macro_rules! def_bindings {
    ($(
        bind $bt:ident $name:ident : $t:ty = $def_val:expr;
    )*) => {
        def_bindings! {{$(
            bind $bt $name : $t = $def_val;
        )*}}
    };
    (
        {
            bind val $name:ident : $t:ty = $def_val:expr;
            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        def_bindings! {
            { $( $unexpanded )* }
            $( $expanded )*
            bind Binding $name : $t = $def_val;
        }
    };
    (
        {
            bind ref $name:ident : $t:ty = $def_val:expr;
            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        def_bindings! {
            { $( $unexpanded )* }
            $( $expanded )*
            bind RefBinding $name : $t = $def_val;
        }
    };
    ({}$(
        bind $bt:ident $name:ident : $t:ty = $def_val:expr;
    )*) =>
    {
        lazy_static! {
            $(
                pub static ref $name : $crate::utils::bindings::$bt<$t> = $crate::utils::bindings::$bt::new($def_val);
            )*
        }
    };
}

#[macro_export]
macro_rules! with_bindings {
    (
        $(
            $bind:path : $val:expr
        ),* =>
        $stat:block
    ) => {
        {
            $(
                $bind.set($val);
            )*
            let r = $stat;
            $(
                $bind.del();
            )*
            r
        }
    };
}

#[cfg(test)]
mod struct_test {
    def_bindings! {
        bind val TEST_VAL: u64 = 0;
        bind ref TEST_REF: String = String::from("Hello");
    }
}
