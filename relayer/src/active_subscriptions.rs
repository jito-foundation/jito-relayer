use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use std::thread::spawn;
use std::time::SystemTime;

// use jito_cache::leader_schedule::cache::LeaderScheduleCache;
use jito_protos::{
    shared::Header,
    packet::{PacketBatchList as PbPacketBatchList},
    relayer::{
        HeartbeatResponse, HeartbeatSubscriptionRequest,
        PacketSubscriptionRequest, PacketSubscriptionResponse,
    }    // validator_interface::{
    //     subscribe_packets_response::Msg::{BatchList, Heartbeat},
    //     SubscribeBundlesResponse, SubscribePacketsResponse,
    // },
};
use log::*;
// use solana_metrics::{datapoint_info, datapoint_warn};
use solana_sdk::{
    clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use tokio::sync::mpsc::{UnboundedSender};
use tonic::Status;
use prost_types::Timestamp;
// use tonic::transport::Channel;
use crate::leader_schedule::LeaderScheduleCache;

type PacketsResultSender = UnboundedSender<Result<PacketSubscriptionResponse, Status>>;
type HeartbeatSender = UnboundedSender<Result<HeartbeatResponse, Status>>;



/// [ActiveSubscriptions] keeps track of data pipes b/w validators and the [ValidatorInterfaceService].
/// Responsible for sending packets over TCP to connected validators and over UDP to validators not connected.
pub struct ActiveSubscriptions {
    packet_subs: Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
    heartbeat_subs: Arc<RwLock<HashMap<Pubkey, HeartbeatSubscription>>>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
}

pub struct PacketSubscription {
    tx: PacketsResultSender,
}

pub struct HeartbeatSubscription {
    tx: HeartbeatSender,
}

impl ActiveSubscriptions {
    pub fn new(leader_schedule_cache: Arc<LeaderScheduleCache>) -> Self {

        Self {
            packet_subs: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_subs: Arc::new(RwLock::new(HashMap::new())),
            leader_schedule_cache,
        }
    }

    /// Sends periodic heartbeats to all active subscribers regardless
    /// of leader schedule.
    pub fn send_heartbeat(&self) -> Vec<Pubkey> {
        let active_hb_subs = self.heartbeat_subs.read().unwrap();
        let mut failed_hb_subs = Vec::new();
        for (pk, subscription) in active_hb_subs.iter() {

            if let Err(e) = subscription.tx.send(Ok(HeartbeatResponse::default())) {
                warn!("error sending heartbeat to subscriber [{}]", e);
                // datapoint_info!(
                //     "validator_subscription",
                //     ("subscriber", pk.to_string(), String),
                //     ("heartbeat_error", 1, i64)
                // );
                failed_hb_subs.push(*pk);
            } else {
                debug!("sent heartbeat to subscriber [{}]", pk);
                // datapoint_info!(
                //     "validator_subscription",
                //     ("subscriber", pk.to_string(), String),
                //     ("heartbeat", 1, i64)
                // );
            }
        }
        failed_hb_subs
    }

    /// Sends packet streams to eligible connections over TCP.
    /// Eligible connections are ones where the `Leader` (connected node) has a slot scheduled
    /// between `start_slot` & `end_slot`.
    ///
    /// returns a tuple where:
    ///     tuple.0 = list of connection ids (pubkey) where an error was encountered on stream attempt
    ///     tuple.1 = a set of slots that were streamed for
    pub fn stream_batch_list(
        &self,
        batch_list: &PbPacketBatchList,
        start_slot: Slot,
        end_slot: Slot,
    ) -> (Vec<Pubkey>, HashSet<Slot>) {
        let mut failed_stream_pks: Vec<Pubkey> = Vec::new();
        let mut slots_sent: HashSet<Slot> = HashSet::new();

        let active_subscriptions = self.packet_subs.read().unwrap();
        if active_subscriptions.is_empty() {
            return (failed_stream_pks, slots_sent);
        }

        let validators_to_send =
            Self::validators_in_slot_range(start_slot, end_slot, &self.leader_schedule_cache);
        let iter = active_subscriptions.iter();
        for (pk, subscription) in iter {
            let slot_to_send = validators_to_send.get(pk);
            if let Some(slot) = slot_to_send {
                if let Err(e) = subscription.tx.send(Ok(PacketSubscriptionResponse {
                    batch_list: Some(
                        PbPacketBatchList {
                            header: Some(Header {
                                ts: Some(Timestamp::from(SystemTime::now())),
                            }),
                            batch_list: batch_list.batch_list.clone(),
                        }),
                })) {
                    // datapoint_warn!(
                    //     "validator_interface_stream_batch_list",
                    //     ("subscriber", pk.to_string(), String),
                    //     ("batch_stream_error", 1, i64),
                    //     ("error", e.to_string(), String)
                    // );
                    failed_stream_pks.push(*pk);
                } else {
                    debug!("slot sent {}", slot);
                    // datapoint_info!(
                    //     "validator_interface_stream_batch_list",
                    //     ("subscriber", pk.to_string(), String),
                    //     ("batches_streamed", batch_list.batch_list.len(), i64)
                    // );
                    slots_sent.insert(*slot);
                }
            }
        }

        (failed_stream_pks, slots_sent)
    }



    /// returns true if subscription was added successfully, false if the given pubkey is already connected
    pub(crate) fn add_packet_subscription(
        &self,
        pk: &Pubkey,
        tx: UnboundedSender<Result<PacketSubscriptionResponse, Status>>,
    ) -> bool {
        let mut active_subs = self.packet_subs.write().unwrap();

        if active_subs.contains_key(pk) {
            // datapoint_warn!(
            //     "validator_interface_add_packet_subscription",
            //     ("subscriber", pk.to_string(), String),
            //     ("add_packet_subscription_error", 1, i64)
            // );
            false
        } else {
            active_subs.insert(Pubkey::new(&pk.to_bytes()), PacketSubscription { tx });
            // datapoint_info!(
            //     "validator_interface_add_packet_subscription",
            //     ("subscriber", pk.to_string(), String),
            //     ("added", 1, i64),
            //     ("num_subs", active_subs.keys().len(), i64)
            // );
            true
        }
    }

    pub(crate) fn add_heartbeat_subscription(&self, pk: &Pubkey, tx: HeartbeatSender) -> bool {
        let mut active_hb_subs = self.heartbeat_subs.write().unwrap();

        if active_hb_subs.contains_key(pk) {
            false
        } else {
            active_hb_subs.insert(Pubkey::new(&pk.to_bytes()), HeartbeatSubscription { tx });
            // datapoint_info!(
            //     "validator_interface_add_bundle_subscription",
            //     ("subscriber", pk.to_string(), String),
            //     ("added", 1, i64),
            //     ("num_subs", active_subs.keys().len(), i64)
            // );
            true
        }
    }

    pub(crate) fn disconnect(&self, keys: &[Pubkey]) {
        if keys.is_empty() {
            return;
        }
        self.remove_heartbeat_subscriptions(keys);
        self.remove_packet_subscription(keys);
    }

    pub(crate) fn remove_packet_subscription(&self, keys: &[Pubkey]) {
        if keys.is_empty() {
            return;
        }
        let mut active_subs = self.packet_subs.write().unwrap();
        for pk in keys {
            active_subs.remove(pk);
            // datapoint_info!(
            //     "validator_interface_remove_packet_subscription",
            //     ("subscriber", pk.to_string(), String),
            //     ("removed", 1, i64),
            //     ("num_subs", active_subs.keys().len(), i64)
            // );
        }
    }

    pub(crate) fn remove_heartbeat_subscriptions(&self, keys: &[Pubkey]) {
        if keys.is_empty() {
            return;
        }
        let mut active_hb_subs = self.heartbeat_subs.write().unwrap();
        for pk in keys {
            active_hb_subs.remove(pk);
            // datapoint_info!(
            //     "validator_interface_remove_bundle_subscriptions",
            //     ("subscriber", pk.to_string(), String),
            //     ("removed", 1, i64),
            //     ("num_subs", active_subs.keys().len(), i64)
            // );
        }
    }

    fn validators_in_slot_range(
        start_slot: Slot,
        end_slot: Slot,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
    ) -> HashMap<Pubkey, Slot> {
        let n_leader_slots = NUM_CONSECUTIVE_LEADER_SLOTS as usize;
        let mut validators_to_send = HashMap::new();
        for slot in (start_slot..end_slot).step_by(n_leader_slots) {
            if let Some(pubkey) = leader_schedule_cache.fetch_scheduled_validator(&slot) {
                validators_to_send.insert(pubkey, slot);
            }
        }

        validators_to_send
    }
}
