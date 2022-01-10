use super::info::*;
use super::publisher::*;
use super::query::*;
use super::queryable::*;
use super::subscriber::*;
use super::Session;
use super::*;
use async_std::sync::Arc;
use async_std::task;
use flume::{bounded, Sender};
use log::{error, trace, warn};
use net::protocol::{
    core::{
        queryable, rname, AtomicZInt, Channel, CongestionControl, QueryConsolidation, QueryTarget,
        QueryableInfo, ResKey, ResourceId, SubInfo, ZInt,
    },
    io::ZBuf,
    proto::{DataInfo, RoutingContext},
};
use net::routing::face::Face;
use net::runtime::Runtime;
use net::transport::Primitives;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::Duration;
use uhlc::HLC;
use zenoh_util::core::{ZError, ZErrorKind, ZResult};
use zenoh_util::sync::zpinbox;

pub struct LBSession {
    resources: Vector<String>,
    publisher: HashMap<u64, String>,
    session: Session,
}

impl Loadbalancer for LBSession {
    fn register_balanced_resource(&self, path: &str) -> u64 {
        &self.session.register_resource(&path)
    }

    fn subscribe_to_resource_group(path: &str, gpid: u8) {
        &self.session.register_resource(&path);
    }

    fn ciao(&self) {
        &self.Session
    }
}
