// SPDX-License-Identifier: AGPL-3.0-or-later

#![doc = include_str!("../README.md")]
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]
#![allow(clippy::uninlined_format_args)]
mod api;
mod bus;
mod config;
mod context;
mod db;
mod graphql;
mod http;
mod manager;
mod materializer;
mod network;
mod node;
#[allow(unused_qualifications)]
mod aquadoggo_rpc {
    tonic::include_proto!("rpc");
}
mod grpc;
#[cfg(all(test, feature = "proptests"))]
mod proptests;
mod replication;
mod schema;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;

use log::{info, log_enabled, Level};

pub use crate::api::{ConfigFile, LockFile, NodeEvent};
pub use crate::config::{AllowList, Configuration};
pub use crate::network::{NetworkConfiguration, Transport};
pub use node::Node;

/// Init env_logger before the test suite runs to handle logging outputs.
///
/// We output log information using the `log` crate. In itself this doesn't print
/// out any logging information, library users can capture and handle the emitted logs
/// using a log handler. Here we use `env_logger` to handle logs emitted
/// while running our tests.
///
/// This will also capture and output any logs emitted from our dependencies. This behaviour
/// can be customised at runtime. With eg. `RUST_LOG=aquadoggo=info cargo t -- --nocapture` or
/// `RUST_LOG=sqlx=debug cargo t -- --nocapture`.
///
/// The `ctor` crate is used to define a global constructor function. This method will be run
/// before any of the test suites.
#[cfg(test)]
#[ctor::ctor]
fn init() {
    // If the `RUST_LOG` env var is not set skip initiation as we don't want
    // to see any logs.
    if std::env::var("RUST_LOG").is_ok() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
}

/// Helper method for logging a message directly to standard out or via the `log` crate when any
/// logging level is enabled. We need this as some messages should be always printed, but when any
/// logging level is selected, we want the message to be printed with consistent formatting.
fn info_or_print(message: &str) {
    if log_enabled!(Level::Info) || log_enabled!(Level::Debug) || log_enabled!(Level::Trace) {
        info!("{message}");
    } else {
        println!("{message}");
    }
}
