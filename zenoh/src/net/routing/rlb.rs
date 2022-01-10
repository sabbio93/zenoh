use crate::net::protocol::core::PeerId;

use super::router::Resource;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub fn myprint(res: &HashSet<Arc<Resource>>) {
    for rs in res {
        let mut m = &rs.context.as_ref().unwrap().peer_subs;
        for sub in m {
            log::debug!("peerid:{}", sub);
        }
        log::debug!("ns:{}", m.len());
    }
}

pub fn get_subs_id(res: &HashSet<Arc<Resource>>) -> Vec<PeerId> {
    let mut ids = Vec::new();
    for rs in res {
        let mut m = &rs.context.as_ref().unwrap().peer_subs;
        for sub in m {
            ids.push(sub.clone());
            log::debug!("peerid:{}", sub);
        }
        log::debug!("ns:{}", m.len());
    }

    ids
}

pub struct BalancedResources {
    resources: HashMap<u64, Resource>,
}

impl BalancedResources {
    pub fn new() -> BalancedResources {
        BalancedResources {
            resources: HashMap::new(),
        }
    }
    pub fn get_subs_per_res(&self, resid: u64) -> Vec<&PeerId> {
        let mut ids = Vec::new();
        let ress = &self.resources;
        let res = self.resources.get(&resid);
        println!("is none {},{}", res.is_none(), ress.len());
        let rs = self.resources.get(&resid).unwrap();
        let mut m = &rs.context.as_ref().unwrap().peer_subs;
        for sub in m {
            ids.push(sub);
            log::debug!("peerid:{}", sub);
        }

        ids
    }
}
