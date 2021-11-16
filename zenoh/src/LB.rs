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
use std::collections::HashMap;

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

pub struct LoadBalancer {
    zenoh_session: Session,
    resources: HashMap<u64, Vec<String>>,
    blres: BalancedResources,
}

impl LoadBalancer {
    pub async fn new(config: Properties) -> LoadBalancer {
        let zenoh_session = open(config).await.unwrap();

        LoadBalancer {
            zenoh_session,
            resources: HashMap::new(),
            blres: BalancedResources::new(),
        }
    }

    async fn register_resource(&self, path: String) -> u64 {
        self.zenoh_session
            .register_resource(&path)
            .await
            .unwrap()
            .clone()
    }

    pub async fn publish<'a, IntoValue>(&'a self, path: String, value: IntoValue) -> Writer<'a>
    where
        IntoValue: Into<Value>,
    {
        let rid = self.zenoh_session.register_resource(&path).await.unwrap();
        println!("register rid:{}", rid);
        let patha = format!("{}/{}", path, "choosen");
        let rida = &self.zenoh_session.register_resource(&patha).await.unwrap();
        println!("register rid:{}", rida);

        let peers: Vec<PeerId>;
        {
            let hh = &self
                .zenoh_session
                .runtime
                .state
                .router
                .tables
                .try_read()
                .unwrap()
                .peer_subs;

            peers = get_subs_id(hh);
        }

        //let peers = &self.blres.get_subs_per_res(rid);
        println!("register rid:{}, peerslen{}", rid, peers.len());

        let choosen = peers.get(0).unwrap();
        let pathsub = format!("{}/{}", path, choosen);
        println!("publish to {}", pathsub.clone());

        let ridsub = self
            .zenoh_session
            .register_resource(&pathsub)
            .await
            .unwrap();
        // println!("number of registerd resources:{}", h.len());
        println!("register rid:{}", ridsub);
        let _pub = self.zenoh_session.publishing(ridsub).await.unwrap();

        self.zenoh_session.put(ridsub, value)
    }

    pub async fn subscribe(&self, path: String) -> Subscriber<'_> {
        //let rid = self.zenoh_session.register_resource(&path).await.unwrap();
        let mut _subscriber = self.zenoh_session.subscribe(&path).await.unwrap();

        let mypid = self.zenoh_session.runtime.pid;
        let pathsub = format!("{}/{}", path, mypid);
        self.zenoh_session.subscribe(&pathsub).await.unwrap()
    }
}
