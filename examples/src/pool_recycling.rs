// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Count failed connection checks separately from planned retirement, then reset session state
//! before returning a connection to the pool. Connections simulate network operations.
//!
//! `WithRecycleDiagnostics` reports errors from an existing manager. The pool still discards
//! rejected connections and retries. The callback runs on the task calling `get()`, so keep it
//! quick and nonblocking. A cancelled check has no error to report.
//!
//! `ReturnAfterReset` owns the connection while the caller uses it. It returns the connection only
//! after a successful reset; an error or cancellation drops the guard and discards the connection.
//! The manager still pings reused connections because they can disconnect while idle.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded::Object;
use asyncband::pool::bounded::Pool;
use asyncband::pool::bounded::PoolConfig;

struct WithRecycleDiagnostics<M, F> {
    manager: M,
    report: F,
}

impl<M, F> ManageObject for WithRecycleDiagnostics<M, F>
where
    M: ManageObject,
    F: Fn(&M::Error, &ObjectStatus) + Send + Sync,
{
    type Object = M::Object;
    type Error = M::Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        self.manager.create().await
    }

    async fn is_recyclable(
        &self,
        object: &mut Self::Object,
        status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        let result = self.manager.is_recyclable(object, status).await;
        if let Err(error) = &result {
            (self.report)(error, status);
        }
        result
    }

    fn on_detached(&self, object: &mut Self::Object) {
        self.manager.on_detached(object);
    }
}

#[derive(Debug)]
enum RecycleError {
    PingFailed,
    Retired,
}

#[derive(Debug)]
struct ResetFailed;

struct Connection {
    connected: Arc<AtomicBool>,
    dirty: bool,
}

impl Connection {
    async fn ping(&self) -> Result<(), RecycleError> {
        tokio::task::yield_now().await;
        if self.connected.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err(RecycleError::PingFailed)
        }
    }

    async fn reset(&mut self) -> Result<(), ResetFailed> {
        tokio::task::yield_now().await;
        if !self.connected.load(Ordering::Relaxed) {
            return Err(ResetFailed);
        }
        self.dirty = false;
        Ok(())
    }
}

#[derive(Clone, Default)]
struct Manager {
    recycle_checks: Arc<AtomicUsize>,
    detached: Arc<AtomicUsize>,
}

impl ManageObject for Manager {
    type Object = Connection;
    type Error = RecycleError;

    async fn create(&self) -> Result<Connection, RecycleError> {
        Ok(Connection {
            connected: Arc::new(AtomicBool::new(true)),
            dirty: false,
        })
    }

    async fn is_recyclable(
        &self,
        connection: &mut Connection,
        status: &ObjectStatus,
    ) -> Result<(), RecycleError> {
        self.recycle_checks.fetch_add(1, Ordering::Relaxed);
        // A low usage limit makes intentional retirement easy to observe in this example.
        if status.recycle_count() >= 1 {
            return Err(RecycleError::Retired);
        }
        connection.ping().await
    }

    fn on_detached(&self, _: &mut Connection) {
        self.detached.fetch_add(1, Ordering::Relaxed);
    }
}

struct ReturnAfterReset<M: ManageObject>(Option<Object<M>>);

impl<M: ManageObject<Object = Connection>> ReturnAfterReset<M> {
    fn connection(&mut self) -> &mut Connection {
        self.0.as_mut().unwrap()
    }

    async fn reset_and_return(mut self) -> Result<(), ResetFailed> {
        self.connection().reset().await?;
        // Taking the wrapper disarms the guard only after the awaited reset succeeds.
        drop(self.0.take());
        Ok(())
    }
}

impl<M: ManageObject> Drop for ReturnAfterReset<M> {
    fn drop(&mut self) {
        if let Some(object) = self.0.take() {
            drop(object.detach());
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    recycling_diagnostics().await;
    cleanup_before_return().await;
    discard_broken_connection().await;
}

async fn recycling_diagnostics() {
    let failed_pings = AtomicUsize::new(0);
    let retirements = AtomicUsize::new(0);
    let manager = Manager::default();
    let observed = WithRecycleDiagnostics {
        manager: manager.clone(),
        report: |error: &RecycleError, _status: &ObjectStatus| match error {
            RecycleError::PingFailed => {
                failed_pings.fetch_add(1, Ordering::Relaxed);
            }
            RecycleError::Retired => {
                retirements.fetch_add(1, Ordering::Relaxed);
            }
        },
    };
    let pool = Pool::new(PoolConfig::new(1), observed);

    let first = pool.get().await.unwrap();
    assert_eq!(manager.recycle_checks.load(Ordering::Relaxed), 0);
    let remote_connection = first.connected.clone();
    drop(first);
    assert_eq!(manager.recycle_checks.load(Ordering::Relaxed), 0);
    // The remote end disconnects while the connection is idle.
    remote_connection.store(false, Ordering::Relaxed);
    let replacement = pool.get().await.unwrap();
    assert_eq!(manager.recycle_checks.load(Ordering::Relaxed), 1);
    assert_eq!(failed_pings.load(Ordering::Relaxed), 1);
    assert_eq!(manager.detached.load(Ordering::Relaxed), 1);
    assert!(!Arc::ptr_eq(&remote_connection, &replacement.connected));
    println!("No recycle check on creation or return; one check when reusing an idle connection");
    println!("Acquisition succeeded after reporting one failed ping");

    drop(replacement);
    assert_eq!(manager.recycle_checks.load(Ordering::Relaxed), 1);
    let reused = pool.get().await.unwrap();
    assert_eq!(manager.recycle_checks.load(Ordering::Relaxed), 2);
    assert_eq!(reused.status().recycle_count(), 1);
    drop(reused);
    let _replacement = pool.get().await.unwrap();
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
    assert_eq!(failed_pings.load(Ordering::Relaxed), 1);
    println!("Usage-limit retirement counted separately from failed pings");
}

async fn cleanup_before_return() {
    let manager = Manager::default();
    let pool = Pool::new(PoolConfig::new(1), manager.clone());
    let mut session = ReturnAfterReset(Some(pool.get().await.unwrap()));
    session.connection().dirty = true;
    let connection = session.connection().connected.clone();
    session.reset_and_return().await.unwrap();
    let reused = pool.get().await.unwrap();
    assert!(!reused.dirty);
    assert!(Arc::ptr_eq(&connection, &reused.connected));
    assert_eq!(manager.detached.load(Ordering::Relaxed), 0);
    println!("Successful reset returned the same connection with clean session state");

    let mut session = ReturnAfterReset(Some(reused));
    session.connection().dirty = true;
    session
        .connection()
        .connected
        .store(false, Ordering::Relaxed);
    let result = session.reset_and_return().await;
    assert!(matches!(result, Err(ResetFailed)));
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(manager.detached.load(Ordering::Relaxed), 1);
    println!("Failed reset discarded the connection");
}

async fn discard_broken_connection() {
    let manager = Manager::default();
    let pool = Pool::new(PoolConfig::new(1), manager.clone());
    let broken = pool.get().await.unwrap();
    broken.connected.store(false, Ordering::Relaxed);
    // A borrower that has already observed a failure can skip reset and another ping.
    drop(broken.detach());
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(manager.recycle_checks.load(Ordering::Relaxed), 0);
    assert_eq!(manager.detached.load(Ordering::Relaxed), 1);
    println!("Known-broken connection discarded without another check");
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::Context;
    use std::task::Waker;

    use super::*;

    #[test]
    fn diagnostics_and_cleanup() {
        main();
    }

    #[tokio::test]
    async fn cancellation_during_reset_detaches() {
        let manager = Manager::default();
        let pool = Pool::new(PoolConfig::new(1), manager.clone());

        {
            let mut session = ReturnAfterReset(Some(pool.get().await.unwrap()));
            session.connection().dirty = true;
            let mut reset = pin!(session.reset_and_return());
            assert!(
                reset
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert_eq!(pool.status().current_size, 0);
        assert_eq!(manager.detached.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cancellation_before_reset_detaches() {
        let manager = Manager::default();
        let pool = Pool::new(PoolConfig::new(1), manager.clone());

        // Cancellation can occur in borrower work, before reset is even requested.
        {
            let mut borrower = pin!(async {
                let mut session = ReturnAfterReset(Some(pool.get().await.unwrap()));
                session.connection().dirty = true;
                std::future::pending::<()>().await;
                session.reset_and_return().await.unwrap();
            });
            assert!(
                borrower
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert_eq!(pool.status().current_size, 0);
        assert_eq!(manager.detached.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn unpolled_reset_detaches() {
        let manager = Manager::default();
        let pool = Pool::new(PoolConfig::new(1), manager.clone());

        let mut session = ReturnAfterReset(Some(pool.get().await.unwrap()));
        session.connection().dirty = true;
        // The future owns the guard even if it is dropped without being polled.
        drop(session.reset_and_return());
        assert_eq!(pool.status().current_size, 0);
        assert_eq!(manager.detached.load(Ordering::Relaxed), 1);
    }
}
