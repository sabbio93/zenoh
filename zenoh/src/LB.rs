use log::debug;
use rand::Rng;
use zenoh_util::properties::Properties;

use crate::net::protocol::core::ResKey;
use crate::net::protocol::{
    core::{
        queryable, rname, AtomicZInt, Channel, CongestionControl, QueryConsolidation, QueryTarget,
        QueryableInfo, ResourceId, SubInfo, ZInt,
    },
    io::ZBuf,
    proto::{DataInfo, RoutingContext},
};
use crate::net::routing::rlb::{get_subs_id, myprint, BalancedResources};
use crate::prelude::*;
use crate::publisher::{Publisher, Writer};
use crate::subscriber::Subscriber;
use crate::{open, Session};
use futures::*;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};

//->Writer<'a>
pub async fn publish<'a, ResourceId, IntoValue>(
    session: &Session,
    rid: ResourceId,
    value: IntoValue,
) {
    let mut h = &session
        .runtime
        .state
        .router
        .tables
        .try_read()
        .unwrap()
        .peer_subs;

    myprint(h);

    let subs = get_subs_id(h);
    let mut rng = rand::thread_rng();
    println!("Integer: {}", rng.gen_range(0..10));

    let choosen = rng.gen_range(0..subs.len());
    println!("choosen:{:?}", subs.get(choosen));

    // &session.put(rid, value).await.unwrap();
}

use std::cell::RefCell;
pub struct LoadBalancer {
    zenoh_session: Session,
    registered_sub_resources: RefCell<HashMap<String, u64>>, // TODO: CAMBIARE IN ARC MUTEX
    blres: BalancedResources,
}

impl LoadBalancer {
    pub async fn new(config: Properties) -> LoadBalancer {
        let zenoh_session = open(config).await.unwrap();
        LoadBalancer {
            zenoh_session,
            registered_sub_resources: RefCell::new(HashMap::new()),
            blres: BalancedResources::new(),
        }
    }

    async fn register_resource(&self, path: String) -> u64 {
        let rid = self.zenoh_session.register_resource(&path).await.unwrap();
        for peerid in &self.get_peers_id().await {
            let subres = format!("{}/{}", path, peerid);
            let subresid = self.zenoh_session.register_resource(&subres).await.unwrap();
            self.registered_sub_resources
                .borrow_mut()
                .insert(subres.clone(), subresid);
        }
        rid
    }

    async fn get_peers_id(&self) -> Vec<PeerId> {
        let hh = &self
            .zenoh_session
            .runtime
            .state
            .router
            .tables
            .try_read()
            .unwrap()
            .peer_subs;

        get_subs_id(hh)
    }

    pub async fn publish<'a, IntoValue>(
        &'a self,
        path: String,
        value: IntoValue,
    ) -> Option<Writer<'a>>
    where
        IntoValue: Into<Value>,
    {
        //let choosen = self.get_peers_id().await.get(0).unwrap().clone();
        let peers = &self.get_peers_id().await;
        let choosen = choose_peer(peers);
        if choosen.is_none() {
            None
        } else {
            let choosen = choosen.unwrap();
            let pathsub = format!("{}/{}", path, choosen);
            debug!("publish to {}", pathsub.clone());

            if !self
                .registered_sub_resources
                .borrow()
                .contains_key(&pathsub)
            {
                let subresid = self
                    .zenoh_session
                    .register_resource(&pathsub)
                    .await
                    .unwrap();
                self.registered_sub_resources
                    .borrow_mut()
                    .insert(pathsub.clone(), subresid);
                let _pub = self.zenoh_session.publishing(subresid).await.unwrap();

                Some(self.zenoh_session.put(subresid, value))
            } else {
                let subresid = self
                    .registered_sub_resources
                    .borrow()
                    .get(&pathsub)
                    .unwrap()
                    .clone();
                let _pub = self.zenoh_session.publishing(subresid).await.unwrap();
                Some(self.zenoh_session.put(subresid, value))
            }
        }
    }

    pub async fn subscribe(&self, path: String) -> Subscriber<'_> {
        //let rid = self.zenoh_session.register_resource(&path).await.unwrap();
        let mut _subscriber = self.zenoh_session.subscribe(&path).await.unwrap();

        let mypid = self.zenoh_session.runtime.pid;
        let pathsub = format!("{}/{}", path, mypid);
        self.zenoh_session.subscribe(&pathsub).await.unwrap()
    }
}

fn choose_peer(peers: &Vec<PeerId>) -> Option<&PeerId> {
    let len = peers.len();
    match len {
        0 => None,
        1 => peers.get(0),
        _ => {
            let mut rng = rand::thread_rng();
            let choosen = rng.gen_range(0..len);
            peers.get(choosen)
        }
    }
}
