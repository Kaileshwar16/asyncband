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

//! A service observes connection churn even when acquisition succeeds, and resets session state
//! before return.
//!
//! `WithRecycleDiagnostics` adapts an existing manager without changing its error type or retry
//! policy. Failed pings and intentional retirement have separate counters; the same adapter works
//! with either managed pool variant. The connections below simulate I/O outcomes rather than
//! contacting a database.
//!
//! `ReturnAfterReset` detaches by default so a failed or cancelled reset cannot return an unclean
//! connection. Only successful cleanup permits ordinary return. This guard is application policy,
//! independent of the pool's checkout cancellation strategy.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Waker;

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
enum Error {
    PingFailed,
    Retired,
    ResetFailed,
}

struct Connection {
    connected: Arc<AtomicBool>,
    dirty: bool,
}

impl Connection {
    async fn ping(&self) -> Result<(), Error> {
        tokio::task::yield_now().await;
        if self.connected.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err(Error::PingFailed)
        }
    }

    async fn reset(&mut self) -> Result<(), Error> {
        tokio::task::yield_now().await;
        if !self.connected.load(Ordering::Relaxed) {
            return Err(Error::ResetFailed);
        }
        self.dirty = false;
        Ok(())
    }
}

struct Manager {
    detached: Arc<AtomicUsize>,
}

impl ManageObject for Manager {
    type Object = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Connection, Error> {
        Ok(Connection {
            connected: Arc::new(AtomicBool::new(true)),
            dirty: false,
        })
    }

    async fn is_recyclable(
        &self,
        connection: &mut Connection,
        status: &ObjectStatus,
    ) -> Result<(), Error> {
        // A low usage limit makes intentional retirement easy to observe in this example.
        if status.recycle_count() >= 1 {
            return Err(Error::Retired);
        }
        connection.ping().await
    }

    fn on_detached(&self, _: &mut Connection) {
        self.detached.fetch_add(1, Ordering::Relaxed);
    }
}

struct ReturnAfterReset<M: ManageObject>(Option<Object<M>>);

impl<M: ManageObject<Object = Connection>> ReturnAfterReset<M> {
    async fn reset_and_return(mut self) -> Result<(), Error> {
        self.0.as_mut().unwrap().reset().await?;
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
    let failed_pings = AtomicUsize::new(0);
    let retirements = AtomicUsize::new(0);
    let detached = Arc::new(AtomicUsize::new(0));
    let manager = WithRecycleDiagnostics {
        manager: Manager {
            detached: detached.clone(),
        },
        report: |error: &Error, _status: &ObjectStatus| match error {
            Error::PingFailed => {
                failed_pings.fetch_add(1, Ordering::Relaxed);
            }
            Error::Retired => {
                retirements.fetch_add(1, Ordering::Relaxed);
            }
            Error::ResetFailed => unreachable!("caller cleanup is outside recycling"),
        },
    };
    let pool = Pool::new(PoolConfig::new(1), manager);

    let first = pool.get().await.unwrap();
    let remote_connection = first.connected.clone();
    drop(first);
    // The remote end disconnects while the connection is idle.
    remote_connection.store(false, Ordering::Relaxed);
    let replacement = pool.get().await.unwrap();
    assert_eq!(failed_pings.load(Ordering::Relaxed), 1);
    assert_eq!(detached.load(Ordering::Relaxed), 1);
    assert!(!Arc::ptr_eq(&remote_connection, &replacement.connected));
    println!("Acquisition succeeded after reporting one failed ping");

    drop(replacement);
    let reused = pool.get().await.unwrap();
    assert_eq!(reused.status().recycle_count(), 1);
    drop(reused);
    let mut clean = pool.get().await.unwrap();
    assert_eq!(retirements.load(Ordering::Relaxed), 1);
    assert_eq!(failed_pings.load(Ordering::Relaxed), 1);
    println!("Usage-limit retirement counted separately from failed pings");

    clean.dirty = true;
    let connection = clean.connected.clone();
    ReturnAfterReset(Some(clean))
        .reset_and_return()
        .await
        .unwrap();
    let mut reused = pool.get().await.unwrap();
    assert!(!reused.dirty);
    assert!(Arc::ptr_eq(&connection, &reused.connected));

    reused.dirty = true;
    reused.connected.store(false, Ordering::Relaxed);
    let result = ReturnAfterReset(Some(reused)).reset_and_return().await;
    assert!(matches!(result, Err(Error::ResetFailed)));
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(detached.load(Ordering::Relaxed), 3);

    let mut interrupted = pool.get().await.unwrap();
    interrupted.dirty = true;
    let mut reset = Box::pin(ReturnAfterReset(Some(interrupted)).reset_and_return());
    // Poll through yield_now to simulate cancellation during asynchronous cleanup.
    assert!(
        reset
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(reset);
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(detached.load(Ordering::Relaxed), 4);
    println!("Failed and cancelled resets both detached their connections");

    let broken = pool.get().await.unwrap();
    broken.connected.store(false, Ordering::Relaxed);
    // A borrower that has already observed a failure can skip reset and another ping.
    drop(broken.detach());
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(failed_pings.load(Ordering::Relaxed), 1);
    assert_eq!(detached.load(Ordering::Relaxed), 5);
}
