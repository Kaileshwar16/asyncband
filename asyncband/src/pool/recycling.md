<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Recycling diagnostics and check timing

Rejecting an idle object and failing an acquisition are separate decisions. Both managed pools call [`ManageObject::is_recyclable`] during checkout, detach a rejected object, and retry. A later attempt can succeed even if several health checks failed; only a creation error is returned by `get()`. Newly created objects are not checked again before their first checkout.

## Observing rejected objects

Record health-check or session-reset failures inside `is_recyclable` before returning the error. This makes connection churn visible through application logging or counters even when acquisition succeeds. [`ManageObject::on_detached`] alone cannot report the reason: it receives no error and also runs for explicit detachment, cancelled checks, and removal through `retain`.

If the manager comes from another library, an application can wrap it in another [`ManageObject`] implementation. Forward `create` and `on_detached` unchanged; in `is_recyclable`, await the inner check, report any error with the supplied [`ObjectStatus`], and return the original result. The diagnostic callback should be quick and nonblocking because it runs on the acquiring task. It need not require the error to implement `Clone` or `Debug`.

An age or usage limit is an ordinary retirement policy, which an application may count separately from a failed ping. The current `Result<(), Error>` contract uses an error for either rejection, so use distinct error variants when that distinction matters. A cancelled check produces no completed result to report.

The runnable [pool recycling example](https://github.com/apache/asyncband/blob/main/examples/src/pool_recycling.rs) demonstrates a manager adapter with a diagnostic callback, separate health-failure and retirement counters, and explicit cleanup before return. It uses simulated connections so that each lifecycle outcome is reproducible without a database.

## Choosing when to check

- **At checkout:** use `is_recyclable` to detect a connection lost while idle or to prepare it for its next borrower. Its asynchronous contract also permits an immediate local-state check without I/O.
- **Before return:** explicitly await application cleanup while owning the object. Dropping the pool wrapper returns it without validation or asynchronous cleanup. A successful reset does not guarantee the connection stays healthy while idle; whether checkout also needs a ping depends on the workload.
- **When already known to be broken:** use [`bounded::Object::detach`] or [`unbounded::Object::detach`] and dispose of the returned raw object. This removes it from pool management immediately, without another check.

Cleanup must also handle cancellation: merely awaiting a reset and detaching on `Err` leaves the ordinary pool wrapper to return the object if the future is dropped mid-reset. An application guard can own the wrapper, detach on drop by default, and allow return only after cleanup succeeds. The example demonstrates this pattern. [`RecycleCancelledStrategy`] governs checks inside `get()`, not cleanup performed by a borrower.

## Exploring an optional error sink

A small candidate API for discussion is an optional pool-level callback with the shape `Fn(&M::Error, &ObjectStatus) + Send + Sync`, defaulting to no action. It would observe each completed recycling rejection outside pool locks without changing discard-and-retry behavior, and would not receive creation errors or cancelled checks. The adapter in the example prototypes this contract using the existing API.

Such a sink could reduce repeated forwarding wrappers when an application configures diagnostics for several third-party managers. It would not infer intentional retirement from an opaque error, observe caller cleanup, or replace application-specific error classification. The example shows that centralized diagnostics are already possible; evidence that these adapters are a recurring burden would justify promoting the callback to the pool API. No new public hook is introduced here.

Other pools provide useful reference points: [bb8 0.9.1](https://docs.rs/bb8/0.9.1/bb8/trait.ErrorSink.html) offers an error sink, while [SQLx 0.9.0](https://docs.rs/sqlx/0.9.0/sqlx/pool/struct.PoolOptions.html#method.before_acquire) uses `Result<bool, Error>` callbacks to distinguish ordinary rejection from a failed check. Neither distinction requires making every rejected object fail acquisition.

A separate synchronous check would need a benefit such as automatic rejection of locally known broken state during return. It should not perform network I/O; [bb8's `has_broken`](https://docs.rs/bb8/0.9.1/bb8/trait.ManageConnection.html#tymethod.has_broken) is a useful reference for inspecting local state. For explicit caller cleanup, `detach` already avoids another ping. Automatic asynchronous return cleanup would additionally need a scheduling and cancellation contract because `Drop` cannot await.
