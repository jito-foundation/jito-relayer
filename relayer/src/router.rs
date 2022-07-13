use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use crossbeam_channel::Receiver;
use jito_protos::{
    self,
    packet::{
        Meta as PbMeta, Packet as PbPacket, PacketBatch as PbPacketBatch, PacketBatchList,
        PacketFlags as PbPacketFlags,
    },
    shared::Header,
    validator_interface_service::{
        packet_stream_msg::Msg::{BatchList, Heartbeat},
        PacketStreamMsg,
    },
};
use log::{debug, info, warn};
use solana_core::banking_stage::BankingPacketBatch;
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_perf::packet::PacketBatch;
use solana_sdk::{
    clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use tokio::sync::mpsc::UnboundedSender;
use tonic::Status;

use crate::schedule_cache::LeaderScheduleCache;

type PacketsResultSender = UnboundedSender<Result<PacketStreamMsg, Status>>;

#[derive(Clone)]
pub struct PacketSubscription {
    tx: PacketsResultSender,
}

pub struct Router {
    packet_subs: RwLock<HashMap<Pubkey, PacketSubscription>>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub slot_receiver: Receiver<Slot>,
    pub packet_receiver: Receiver<PacketBatchList>,
    pub current_slot: RwLock<Slot>,
}

impl Router {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<PacketBatchList>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
    ) -> Router {
        Router {
            packet_subs: RwLock::new(HashMap::new()),
            leader_schedule_cache,
            slot_receiver,
            packet_receiver,
            current_slot: RwLock::new(0_u64),
        }
    }

    /// Sends periodic heartbeats to all active subscribers regardless
    /// of leader schedule.
    pub fn send_heartbeat(&self) -> Vec<Pubkey> {
        let active_subscriptions = self.packet_subs.read().unwrap().clone();
        let mut failed_subscriptions = Vec::new();

        let ts = prost_types::Timestamp::from(SystemTime::now());
        let header = Header { ts: Some(ts) };
        for (pk, subscription) in active_subscriptions.iter() {
            if let Err(e) = subscription.tx.send(Ok(PacketStreamMsg {
                msg: Some(Heartbeat(header.clone())),
            })) {
                warn!("error sending heartbeat to subscriber [{}]", e);
                datapoint_info!(
                    "validator_subscription",
                    ("subscriber", pk.to_string(), String),
                    ("heartbeat_error", 1, i64)
                );
                failed_subscriptions.push(*pk);
            } else {
                debug!("sent heartbeat to subscriber [{}]", pk);
                datapoint_info!(
                    "validator_subscription",
                    ("subscriber", pk.to_string(), String),
                    ("heartbeat", 1, i64)
                );
            }
        }
        failed_subscriptions
    }

    /// returns true if subscription was added successfully, false if the given pubkey is already connected
    pub(crate) fn add_packet_subscription(
        &self,
        pk: &Pubkey,
        tx: UnboundedSender<Result<PacketStreamMsg, Status>>,
    ) -> bool {
        let mut active_subs = self.packet_subs.write().unwrap();

        if active_subs.contains_key(pk) {
            datapoint_warn!(
                "validator_interface_add_packet_subscription",
                ("subscriber", pk.to_string(), String),
                ("add_packet_subscription_error", 1, i64)
            );
            false
        } else {
            active_subs.insert(Pubkey::new(&pk.to_bytes()), PacketSubscription { tx });
            info!(
                "validator_interface_add_packet_subscription - Subscriber: {}",
                pk.to_string()
            );

            datapoint_info!(
                "validator_interface_add_packet_subscription",
                ("subscriber", pk.to_string(), String),
                ("added", 1, i64),
                ("num_subs", active_subs.keys().len(), i64)
            );
            true
        }
    }

    pub(crate) fn disconnect(&self, keys: &[Pubkey]) {
        if keys.is_empty() {
            return;
        }
        self.remove_packet_subscription(keys);
    }

    pub(crate) fn remove_packet_subscription(&self, keys: &[Pubkey]) {
        if keys.is_empty() {
            return;
        }
        let mut active_subs = self.packet_subs.write().unwrap();
        for pk in keys {
            active_subs.remove(pk);
            datapoint_info!(
                "validator_interface_remove_packet_subscription",
                ("subscriber", pk.to_string(), String),
                ("removed", 1, i64),
                ("num_subs", active_subs.keys().len(), i64)
            );
        }
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
        batch_list: &PacketBatchList,
        start_slot: Slot,
        end_slot: Slot,
    ) -> (Vec<Pubkey>, HashSet<Slot>) {
        let mut failed_stream_pks: Vec<Pubkey> = Vec::new();
        let mut slots_sent: HashSet<Slot> = HashSet::new();

        let active_subscriptions = self.packet_subs.read().unwrap();
        if active_subscriptions.is_empty() {
            warn!("stream_batch_list: No Active Subscriptions");
            return (failed_stream_pks, slots_sent);
        }

        let validators_to_send =
            Self::validators_in_slot_range(start_slot, end_slot, &self.leader_schedule_cache);
        let iter = active_subscriptions.iter();
        for (pk, subscription) in iter {
            let slot_to_send = validators_to_send.get(pk);
            debug!("Slot to Send: {:?}", slot_to_send);
            if let Some(slot) = slot_to_send {
                if let Err(e) = subscription.tx.send(Ok(PacketStreamMsg {
                    msg: Some(BatchList(batch_list.clone())),
                })) {
                    datapoint_warn!(
                        "validator_interface_stream_batch_list",
                        ("subscriber", pk.to_string(), String),
                        ("batch_stream_error", 1, i64),
                        ("error", e.to_string(), String)
                    );
                    failed_stream_pks.push(*pk);
                } else {
                    datapoint_info!(
                        "validator_interface_stream_batch_list",
                        ("subscriber", pk.to_string(), String),
                        ("batches_streamed", batch_list.batch_list.len(), i64)
                    );
                    slots_sent.insert(*slot);
                }
            }
        }

        (failed_stream_pks, slots_sent)
    }

    fn validators_in_slot_range(
        start_slot: Slot,
        end_slot: Slot,
        leader_schedule_cache: &LeaderScheduleCache,
    ) -> HashMap<Pubkey, Slot> {
        let n_leader_slots = NUM_CONSECUTIVE_LEADER_SLOTS as usize;
        let mut validators_to_send = HashMap::new();
        for slot in (start_slot..end_slot).step_by(n_leader_slots) {
            if let Some(pk) = leader_schedule_cache.fetch_scheduled_validator(&slot) {
                validators_to_send.insert(pk, slot);
            }
        }
        debug!(
            "validators_in_slot_range: {}  -  {},   val: {:?}",
            start_slot, end_slot, validators_to_send
        );

        validators_to_send
    }
}
