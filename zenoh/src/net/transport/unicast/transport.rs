//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//
use super::super::{TransportManager, TransportPeerEventHandler};
use super::common::{
    conduit::{TransportConduitRx, TransportConduitTx},
    pipeline::TransmissionPipeline,
};
use super::link::TransportLinkUnicast;
use super::protocol::core::{ConduitSn, PeerId, Priority, WhatAmI, ZInt};
use super::protocol::proto::{TransportMessage, ZenohMessage};
use crate::net::link::{Link, LinkUnicast};
use async_std::sync::{Arc as AsyncArc, Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use std::convert::TryInto;
#[cfg(feature = "stats")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use zenoh_util::core::{ZError, ZErrorKind, ZResult};
use zenoh_util::zerror;

macro_rules! zlinkget {
    ($guard:expr, $link:expr) => {
        $guard.iter().find(|l| l.get_link() == $link)
    };
}

macro_rules! zlinkgetmut {
    ($guard:expr, $link:expr) => {
        $guard.iter_mut().find(|l| l.get_link() == $link)
    };
}

macro_rules! zlinkindex {
    ($guard:expr, $link:expr) => {
        $guard.iter().position(|l| l.get_link() == $link)
    };
}

/*************************************/
/*              STATS                */
/*************************************/
#[cfg(feature = "stats")]
#[derive(Clone)]
pub(crate) struct TransportUnicastStatsInner {
    tx_msgs: Arc<AtomicUsize>,
    tx_bytes: Arc<AtomicUsize>,
    rx_msgs: Arc<AtomicUsize>,
    rx_bytes: Arc<AtomicUsize>,
}

#[cfg(feature = "stats")]
impl TransportUnicastStatsInner {
    pub(crate) fn inc_tx_msgs(&self, messages: usize) {
        self.tx_msgs.fetch_add(messages, Ordering::Relaxed);
    }

    pub(crate) fn get_tx_msgs(&self) -> usize {
        self.tx_msgs.load(Ordering::Relaxed)
    }

    pub(crate) fn inc_tx_bytes(&self, bytes: usize) {
        self.tx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn get_tx_bytes(&self) -> usize {
        self.tx_bytes.load(Ordering::Relaxed)
    }

    pub(crate) fn inc_rx_msgs(&self, messages: usize) {
        self.rx_msgs.fetch_add(messages, Ordering::Relaxed);
    }

    pub(crate) fn get_rx_msgs(&self) -> usize {
        self.rx_msgs.load(Ordering::Relaxed)
    }

    pub(crate) fn inc_rx_bytes(&self, bytes: usize) {
        self.rx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn get_rx_bytes(&self) -> usize {
        self.rx_bytes.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "stats")]
impl Default for TransportUnicastStatsInner {
    fn default() -> TransportUnicastStatsInner {
        TransportUnicastStatsInner {
            tx_msgs: Arc::new(AtomicUsize::new(0)),
            tx_bytes: Arc::new(AtomicUsize::new(0)),
            rx_msgs: Arc::new(AtomicUsize::new(0)),
            rx_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/*************************************/
/*             TRANSPORT             */
/*************************************/
#[derive(Clone)]
pub(crate) struct TransportUnicastInner {
    // The manager this channel is associated to
    pub(super) manager: TransportManager,
    // The remote peer id
    pub(super) pid: PeerId,
    // The remote whatami
    pub(super) whatami: WhatAmI,
    // The SN resolution
    pub(super) sn_resolution: ZInt,
    // Tx conduits
    pub(super) conduit_tx: Arc<[TransportConduitTx]>,
    // Rx conduits
    pub(super) conduit_rx: Arc<[TransportConduitRx]>,
    // The links associated to the channel
    pub(super) links: Arc<RwLock<Box<[TransportLinkUnicast]>>>,
    // The callback
    pub(super) callback: Arc<RwLock<Option<Arc<dyn TransportPeerEventHandler>>>>,
    // Mutex for notification
    pub(super) alive: AsyncArc<AsyncMutex<bool>>,
    // The transport can do shm
    pub(super) is_shm: bool,
    // Transport statistics
    #[cfg(feature = "stats")]
    pub(super) stats: TransportUnicastStatsInner,
}

pub(crate) struct TransportUnicastConfig {
    pub(crate) manager: TransportManager,
    pub(crate) pid: PeerId,
    pub(crate) whatami: WhatAmI,
    pub(crate) sn_resolution: ZInt,
    pub(crate) initial_sn_tx: ZInt,
    pub(crate) initial_sn_rx: ZInt,
    pub(crate) is_shm: bool,
    pub(crate) is_qos: bool,
}

impl TransportUnicastInner {
    pub(super) fn new(config: TransportUnicastConfig) -> TransportUnicastInner {
        let mut conduit_tx = vec![];
        let mut conduit_rx = vec![];

        // @TODO: potentially manager different initial SNs per conduit channel
        let conduit_sn_tx = ConduitSn {
            reliable: config.initial_sn_tx,
            best_effort: config.initial_sn_tx,
        };
        let conduit_sn_rx = ConduitSn {
            reliable: config.initial_sn_rx,
            best_effort: config.initial_sn_rx,
        };

        if config.is_qos {
            for c in 0..Priority::NUM {
                conduit_tx.push(TransportConduitTx::new(
                    (c as u8).try_into().unwrap(),
                    config.sn_resolution,
                    conduit_sn_tx,
                ));
            }

            for c in 0..Priority::NUM {
                conduit_rx.push(TransportConduitRx::new(
                    (c as u8).try_into().unwrap(),
                    config.sn_resolution,
                    conduit_sn_rx,
                    config.manager.config.defrag_buff_size,
                ));
            }
        } else {
            conduit_tx.push(TransportConduitTx::new(
                Priority::default(),
                config.sn_resolution,
                conduit_sn_tx,
            ));

            conduit_rx.push(TransportConduitRx::new(
                Priority::default(),
                config.sn_resolution,
                conduit_sn_rx,
                config.manager.config.defrag_buff_size,
            ));
        }

        TransportUnicastInner {
            manager: config.manager,
            pid: config.pid,
            whatami: config.whatami,
            sn_resolution: config.sn_resolution,
            conduit_tx: conduit_tx.into_boxed_slice().into(),
            conduit_rx: conduit_rx.into_boxed_slice().into(),
            links: Arc::new(RwLock::new(vec![].into_boxed_slice())),
            callback: Arc::new(RwLock::new(None)),
            alive: AsyncArc::new(AsyncMutex::new(true)),
            is_shm: config.is_shm,
            #[cfg(feature = "stats")]
            stats: TransportUnicastStatsInner::default(),
        }
    }

    pub(super) fn set_callback(&self, callback: Arc<dyn TransportPeerEventHandler>) {
        let mut guard = zwrite!(self.callback);
        *guard = Some(callback);
    }

    pub(super) async fn get_alive(&self) -> AsyncMutexGuard<'_, bool> {
        zasynclock!(self.alive)
    }

    /*************************************/
    /*           TERMINATION             */
    /*************************************/
    pub(super) async fn delete(&self) -> ZResult<()> {
        log::debug!("Closing transport with peer: {}", self.pid);

        // Mark the transport as no longer alive and keep the lock
        // to avoid concurrent new_transport and closing/closed notifications
        let mut a_guard = self.get_alive().await;
        *a_guard = false;

        // Notify the callback that we are going to close the transport
        let callback = zwrite!(self.callback).take();
        if let Some(cb) = callback.as_ref() {
            cb.closing();
        }

        // Delete the transport on the manager
        let _ = self.manager.del_transport_unicast(&self.pid).await;

        // Close all the links
        let mut links = {
            let mut l_guard = zwrite!(self.links);
            let links = l_guard.to_vec();
            *l_guard = vec![].into_boxed_slice();
            links
        };
        for l in links.drain(..) {
            let _ = l.close().await;
        }

        // Notify the callback that we have closed the transport
        if let Some(cb) = callback.as_ref() {
            cb.closed();
        }

        Ok(())
    }

    /*************************************/
    /*               LINK                */
    /*************************************/
    pub(super) fn add_link(&self, link: LinkUnicast) -> ZResult<()> {
        let mut guard = zwrite!(self.links);
        if guard.len() >= self.manager.config.unicast.max_links {
            return zerror!(ZErrorKind::InvalidLink {
                descr: format!(
                    "Can not add Link {} with peer {}: max num of links ({})",
                    link, self.pid, self.manager.config.unicast.max_links
                )
            });
        }

        if zlinkget!(guard, &link).is_some() {
            return zerror!(ZErrorKind::InvalidLink {
                descr: format!("Can not add Link {} with peer: {}", link, self.pid)
            });
        }

        // Create a channel link from a link
        let link = TransportLinkUnicast::new(self.clone(), link);

        // Add the link to the channel
        let mut links = Vec::with_capacity(guard.len() + 1);
        links.extend_from_slice(&guard);
        links.push(link);
        *guard = links.into_boxed_slice();

        Ok(())
    }

    pub(super) fn start_tx(
        &self,
        link: &LinkUnicast,
        keep_alive: Duration,
        batch_size: u16,
    ) -> ZResult<()> {
        let mut guard = zwrite!(self.links);
        match zlinkgetmut!(guard, link) {
            Some(l) => {
                assert!(!self.conduit_tx.is_empty());
                l.start_tx(keep_alive, batch_size, self.conduit_tx.clone());
                Ok(())
            }
            None => {
                zerror!(ZErrorKind::InvalidLink {
                    descr: format!("Can not start Link TX {} with peer: {}", link, self.pid)
                })
            }
        }
    }

    pub(super) fn stop_tx(&self, link: &LinkUnicast) -> ZResult<()> {
        let mut guard = zwrite!(self.links);
        match zlinkgetmut!(guard, link) {
            Some(l) => {
                l.stop_tx();
                Ok(())
            }
            None => {
                zerror!(ZErrorKind::InvalidLink {
                    descr: format!("Can not stop Link TX {} with peer: {}", link, self.pid)
                })
            }
        }
    }

    pub(super) fn start_rx(&self, link: &LinkUnicast, lease: Duration) -> ZResult<()> {
        let mut guard = zwrite!(self.links);
        match zlinkgetmut!(guard, link) {
            Some(l) => {
                l.start_rx(lease);
                Ok(())
            }
            None => {
                zerror!(ZErrorKind::InvalidLink {
                    descr: format!("Can not start Link RX {} with peer: {}", link, self.pid)
                })
            }
        }
    }

    pub(super) fn stop_rx(&self, link: &LinkUnicast) -> ZResult<()> {
        let mut guard = zwrite!(self.links);
        match zlinkgetmut!(guard, link) {
            Some(l) => {
                l.stop_rx();
                Ok(())
            }
            None => {
                zerror!(ZErrorKind::InvalidLink {
                    descr: format!("Can not stop Link RX {} with peer: {}", link, self.pid)
                })
            }
        }
    }

    pub(crate) async fn del_link(&self, link: &LinkUnicast) -> ZResult<()> {
        enum Target {
            Transport,
            Link(Box<TransportLinkUnicast>),
        }

        // Try to remove the link
        let target = {
            let mut guard = zwrite!(self.links);
            if let Some(index) = zlinkindex!(guard, link) {
                let is_last = guard.len() == 1;
                if is_last {
                    // Close the whole transport
                    drop(guard);
                    Target::Transport
                } else {
                    // Remove the link
                    let mut links = guard.to_vec();
                    let stl = links.remove(index);
                    *guard = links.into_boxed_slice();
                    drop(guard);
                    // Notify the callback
                    if let Some(callback) = zread!(self.callback).as_ref() {
                        callback.del_link(Link::from(link));
                    }
                    Target::Link(stl.into())
                }
            } else {
                return zerror!(ZErrorKind::InvalidLink {
                    descr: format!("Can not delete Link {} with peer: {}", link, self.pid)
                });
            }
        };

        match target {
            Target::Transport => self.delete().await,
            Target::Link(stl) => stl.close().await,
        }
    }
}

impl TransportUnicastInner {
    /*************************************/
    /*            ACCESSORS              */
    /*************************************/
    pub(crate) fn get_pid(&self) -> PeerId {
        self.pid
    }

    pub(crate) fn get_whatami(&self) -> WhatAmI {
        self.whatami
    }

    pub(crate) fn get_sn_resolution(&self) -> ZInt {
        self.sn_resolution
    }

    pub(crate) fn is_shm(&self) -> bool {
        self.is_shm
    }

    pub(crate) fn is_qos(&self) -> bool {
        self.conduit_tx.len() > 1
    }

    pub(crate) fn get_callback(&self) -> Option<Arc<dyn TransportPeerEventHandler>> {
        zread!(self.callback).clone()
    }

    /*************************************/
    /*           TERMINATION             */
    /*************************************/
    pub(crate) async fn close_link(&self, link: &LinkUnicast, reason: u8) -> ZResult<()> {
        log::trace!("Closing link {} with peer: {}", link, self.pid);

        let guard = zread!(self.links);
        if let Some(l) = zlinkget!(guard, link) {
            let mut pipeline = l.get_pipeline();
            // Drop the guard
            drop(guard);

            // Schedule the close message for transmission
            if let Some(pipeline) = pipeline.take() {
                // Close message to be sent on the target link
                let peer_id = Some(self.manager.pid());
                let reason_id = reason;
                let link_only = true; // This is should always be true when closing a link
                let attachment = None; // No attachment here
                let msg = TransportMessage::make_close(peer_id, reason_id, link_only, attachment);

                pipeline.push_transport_message(msg, Priority::Background);
            }

            // Remove the link from the channel
            self.del_link(link).await?;
        }

        Ok(())
    }

    pub(crate) async fn close(&self, reason: u8) -> ZResult<()> {
        log::trace!("Closing transport with peer: {}", self.pid);

        let mut pipelines: Vec<Arc<TransmissionPipeline>> = zread!(self.links)
            .iter()
            .filter_map(|sl| sl.get_pipeline())
            .collect();
        for p in pipelines.drain(..) {
            // Close message to be sent on all the links
            let peer_id = Some(self.manager.pid());
            let reason_id = reason;
            // link_only should always be false for user-triggered close. However, in case of
            // multiple links, it is safer to close all the links first. When no links are left,
            // the transport is then considered closed.
            let link_only = true;
            let attachment = None; // No attachment here
            let msg = TransportMessage::make_close(peer_id, reason_id, link_only, attachment);

            p.push_transport_message(msg, Priority::Background);
        }
        // Terminate and clean up the transport
        self.delete().await
    }

    /*************************************/
    /*        SCHEDULE AND SEND TX       */
    /*************************************/
    /// Schedule a Zenoh message on the transmission queue    
    #[cfg(feature = "zero-copy")]
    pub(crate) fn schedule(&self, mut message: ZenohMessage) {
        let res = if self.is_shm {
            message.map_to_shminfo()
        } else {
            message.map_to_shmbuf(self.manager.shmr.clone())
        };
        if let Err(e) = res {
            log::trace!("Failed SHM conversion: {}", e);
            return;
        }
        self.schedule_first_fit(message);
    }

    #[cfg(not(feature = "zero-copy"))]
    pub(crate) fn schedule(&self, message: ZenohMessage) {
        self.schedule_first_fit(message);
    }

    pub(crate) fn get_links(&self) -> Vec<LinkUnicast> {
        zread!(self.links)
            .iter()
            .map(|l| l.get_link().clone())
            .collect()
    }
}
