//! One lazily created runtime per Python host thread, shared by its clients.
//! A forked child never enters or shuts down its parent's runtime.
use pyo3::prelude::*;
use pyo3_async_runtimes::{TaskLocals, generic};
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};

thread_local! { static SHARED: RefCell<Weak<Owner>> = const {RefCell::new(Weak::new())}; }
struct Owner {
    pid: u32,
    runtime: Option<tokio::runtime::Runtime>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            if self.pid == std::process::id() {
                runtime.shutdown_background();
            } else {
                std::mem::forget(runtime);
            }
        }
    }
}
#[derive(Default)]
pub struct Slot(Mutex<Option<Arc<Owner>>>);
impl Slot {
    fn owner(&self) -> Arc<Owner> {
        let mut slot = self.0.lock().unwrap();
        slot.get_or_insert_with(|| {
            SHARED.with(|shared| {
                if let Some(owner) = shared
                    .borrow()
                    .upgrade()
                    .filter(|o| o.pid == std::process::id())
                {
                    return owner;
                }
                let owner = Arc::new(Owner {
                    pid: std::process::id(),
                    runtime: Some(
                        tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(2)
                            .enable_all()
                            .thread_name("rime-sdk")
                            .build()
                            .expect("create SDK runtime"),
                    ),
                });
                *shared.borrow_mut() = Arc::downgrade(&owner);
                owner
            })
        })
        .clone()
    }
    pub fn enter<T>(&self, f: impl FnOnce() -> T) -> T {
        let owner = self.owner();
        let _guard = owner.runtime.as_ref().unwrap().enter();
        f()
    }
    pub fn future<'p, F, T>(&self, py: Python<'p>, f: F) -> PyResult<Bound<'p, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'a> IntoPyObject<'a> + Send + 'static,
    {
        self.enter(|| generic::future_into_py::<Bridge, _, _>(py, f))
    }
}
struct Bridge;
tokio::task_local! {static LOCALS: TaskLocals;}
impl generic::Runtime for Bridge {
    type JoinError = tokio::task::JoinError;
    type JoinHandle = tokio::task::JoinHandle<()>;
    fn spawn<F: Future<Output = ()> + Send + 'static>(f: F) -> Self::JoinHandle {
        tokio::spawn(f)
    }
    fn spawn_blocking<F: FnOnce() + Send + 'static>(f: F) -> Self::JoinHandle {
        tokio::task::spawn_blocking(f)
    }
}
impl generic::ContextExt for Bridge {
    fn scope<F: Future<Output = T> + Send + 'static, T>(
        locals: TaskLocals,
        f: F,
    ) -> Pin<Box<dyn Future<Output = T> + Send>> {
        Box::pin(LOCALS.scope(locals, f))
    }
    fn get_task_locals() -> Option<TaskLocals> {
        LOCALS.try_with(Clone::clone).ok()
    }
}
