use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use crossbeam_channel::{select, Receiver};
use jito_protos::{
    packet::{
        Meta as PbMeta, Packet as PbPacket, PacketBatch as PbPacketBatch,
        PacketBatchWrapper as PbPacketBatchWrapper, PacketFlags as PbPacketFlags,
    },
    validator_interface_service::{
        subscribe_packets_response::Msg::{BatchList, Heartbeat},
        SubscribePacketsResponse,
    },
};
use log::{debug, error, info, warn};
use solana_core::banking_stage::BankingPacketBatch;
use solana_perf::packet::PacketBatch;
use solana_sdk::{
    clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use tokio::sync::mpsc::UnboundedSender;
use tonic::Status;

use crate::schedule_cache::LeaderScheduleCache;

type PacketsResultSender = UnboundedSender<Result<SubscribePacketsResponse, Status>>;

pub struct PacketSubscription {
    tx: PacketsResultSender,
}

pub struct Router {
    packet_subs: Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    slot_receiver: Receiver<Slot>,
    packet_receiver: Receiver<BankingPacketBatch>,
}

impl Router {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
    ) -> Router {
        // Must Call init externally after creating
        let router = Router {
            packet_subs: Arc::new(RwLock::new(HashMap::new())),
            leader_schedule_cache,
            slot_receiver,
            packet_receiver,
        };

        router.packets_receiver_loop(3, 0);

        return router;
    }

    /// Sends periodic heartbeats to all active subscribers regardless
    /// of leader schedule.
    pub fn send_heartbeat(&self) -> Vec<Pubkey> {
        let active_subscriptions = self.packet_subs.read().unwrap();
        let mut failed_subscriptions = Vec::new();
        for (pk, subscription) in active_subscriptions.iter() {
            if let Err(e) = subscription.tx.send(Ok(SubscribePacketsResponse {
                msg: Some(Heartbeat(true)),
            })) {
                warn!("error sending heartbeat to subscriber [{}]", e);
                // datapoint_info!(
                //     "validator_subscription",
                //     ("subscriber", pk.to_string(), String),
                //     ("heartbeat_error", 1, i64)
                // );
                failed_subscriptions.push(*pk);
            } else {
                debug!("sent heartbeat to subscriber [{}]", pk);
                // datapoint_info!(
                //     "validator_subscription",
                //     ("subscriber", pk.to_string(), String),
                //     ("heartbeat", 1, i64)
                // );
            }
        }
        failed_subscriptions
    }

    /// returns true if subscription was added successfully, false if the given pubkey is already connected
    pub(crate) fn add_packet_subscription(
        &self,
        pk: &Pubkey,
        tx: UnboundedSender<Result<SubscribePacketsResponse, Status>>,
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
            // datapoint_info!(
            //     "validator_interface_remove_packet_subscription",
            //     ("subscriber", pk.to_string(), String),
            //     ("removed", 1, i64),
            //     ("num_subs", active_subs.keys().len(), i64)
            // );
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
        batch_list: &PbPacketBatchWrapper,
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
                if let Err(_e) = subscription.tx.send(Ok(SubscribePacketsResponse {
                    msg: Some(BatchList(batch_list.clone())),
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

    fn validators_in_slot_range(
        start_slot: Slot,
        end_slot: Slot,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
    ) -> HashMap<Pubkey, Slot> {
        let n_leader_slots = NUM_CONSECUTIVE_LEADER_SLOTS as usize;
        let mut validators_to_send = HashMap::new();
        for slot in (start_slot..end_slot).step_by(n_leader_slots) {
            if let Some(pk) = leader_schedule_cache.fetch_scheduled_validator(&slot) {
                validators_to_send.insert(pk, slot);
            }
        }

        validators_to_send
    }

    /// Receive packet batches via Receiver and stream them out over UDP or TCP to nodes.
    #[allow(clippy::too_many_arguments)]
    fn packets_receiver_loop(
        &self,
        // num leaders ahead to send packets to
        look_ahead: u64,
        // num leaders behind to send packets to
        look_behind: u64,
    ) {
        // let udp_sender = UdpSocket::bind("0.0.0.0:0").expect("bind to udp socket");
        let mut current_slot: Slot = 0;

        loop {
            select! {
                recv(self.slot_receiver) -> maybe_slot => {
                    match maybe_slot {
                        Ok(slot) => {
                            current_slot = slot;
                        }
                        Err(_) => {
                            error!("error receiving slot");
                            break;
                        }
                    }
                }

                recv(self.packet_receiver) -> maybe_bp_batch => {
                    match maybe_bp_batch {
                        Ok(bp_batch) =>  {
                            let batches = bp_batch.0;
                            info!(
                                "Got Batch of length {} x {}",
                                batches.len(),
                                batches[0].len()
                            );
                            let proto_bl = Self::sol_batchlist_to_proto(batches);

                            let start_slot = current_slot - (NUM_CONSECUTIVE_LEADER_SLOTS * look_behind);
                            let end_slot = current_slot + (NUM_CONSECUTIVE_LEADER_SLOTS * look_ahead);
                            let (failed_stream_pks, _slots_sent) = self.stream_batch_list(&proto_bl, start_slot, end_slot);

                            // Todo: Might add udp later?
                            // Self::send_udp_streams(
                            //     batch_list.batch_list,
                            //     &leader_schedule_cache,
                            //     jito_tpu,
                            //     slots_sent,
                            //     &udp_sender,
                            //     start_slot,
                            //     end_slot,
                            // );

                            // close the connections
                            self.disconnect(&failed_stream_pks);
                        }
                        Err(_) => {
                            error!("error receiving packets");
                            break;
                        }
                    }
                }
            }
        }
    }

    fn sol_batchlist_to_proto(batches: Vec<PacketBatch>) -> PbPacketBatchWrapper {
        // ToDo: Turn this back into a map
        let mut proto_batch_vec: Vec<PbPacketBatch> = Vec::new();
        for batch in batches.into_iter() {
            let mut proto_pkt_vec: Vec<PbPacket> = Vec::new();
            for p in batch.iter() {
                if !p.meta.discard() {
                    proto_pkt_vec.push(PbPacket {
                        data: p.data[0..p.meta.size].to_vec(),
                        meta: Some(PbMeta {
                            size: p.meta.size as u64,
                            addr: p.meta.addr.to_string(),
                            port: p.meta.port as u32,
                            flags: Some(PbPacketFlags {
                                discard: p.meta.discard(),
                                forwarded: p.meta.forwarded(),
                                repair: p.meta.repair(),
                                simple_vote_tx: p.meta.is_simple_vote_tx(),
                                // tracer_tx: p.meta.is_tracer_tx(),  // Couldn't get this to work?
                                tracer_tx: false,
                            }),
                        }),
                    })
                }
            }
            proto_batch_vec.push(PbPacketBatch {
                packets: proto_pkt_vec,
            })
        }

        PbPacketBatchWrapper {
            // ToDo: Perf - Clone here?
            batch_list: proto_batch_vec.clone(),
        }
    }
}
