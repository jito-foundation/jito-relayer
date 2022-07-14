use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use crossbeam_channel::{unbounded, Receiver, RecvError, SendError, Sender};
use jito_protos::{
    convert::packet_to_proto_packet,
    packet::{Packet as ProtoPacket, PacketBatch as ProtoPacketBatch},
    relayer::{subscribe_packets_response::Msg, SubscribePacketsResponse},
    shared::{Header, Heartbeat},
};
use log::{error, info};
use prost_types::Timestamp;
use solana_perf::packet::PacketBatch;
use solana_sdk::{
    clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use thiserror::Error;
use tokio::sync::mpsc::Sender as TokioSender;
use tonic::Status;

use crate::schedule_cache::LeaderScheduleUpdatingHandle;

pub enum Subscription {
    ValidatorPacketSubscription {
        pubkey: Pubkey,
        sender: TokioSender<result::Result<SubscribePacketsResponse, Status>>,
    },
}

#[derive(Error, Debug)]
pub enum RouterError {
    #[error("shutdown")]
    Shutdown(#[from] RecvError),
}

pub type Result<T> = result::Result<T, RouterError>;

pub struct Router {
    threads: Vec<JoinHandle<()>>,
    subscription_sender: Sender<Subscription>,
}

impl Router {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<Vec<PacketBatch>>,
        leader_schedule_cache: LeaderScheduleUpdatingHandle,
        exit: Arc<AtomicBool>,
    ) -> Router {
        const LEADER_LOOKAHEAD: u64 = 2;

        let (subscription_sender, subscription_receiver) = unbounded();
        let threads = vec![Self::start_router_thread(
            slot_receiver,
            subscription_receiver,
            packet_receiver,
            leader_schedule_cache,
            exit,
            LEADER_LOOKAHEAD,
        )];
        Router {
            threads,
            subscription_sender,
        }
    }

    /// Adds a subscription to the router
    pub fn add_subscription(
        &self,
        subscription: Subscription,
    ) -> result::Result<(), SendError<Subscription>> {
        self.subscription_sender.send(subscription)
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.threads {
            t.join()?
        }
        Ok(())
    }

    fn start_router_thread(
        slot_receiver: Receiver<Slot>,
        subscription_receiver: Receiver<Subscription>,
        packet_receiver: Receiver<Vec<PacketBatch>>,
        leader_schedule_cache: LeaderScheduleUpdatingHandle,
        exit: Arc<AtomicBool>,
        leader_lookahead: u64,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("jito-packet-router".into())
            .spawn(move || {
                let _ = Self::run_event_loop(
                    slot_receiver,
                    subscription_receiver,
                    packet_receiver,
                    leader_schedule_cache,
                    exit,
                    leader_lookahead,
                );
            })
            .unwrap()
    }

    fn run_event_loop(
        slot_receiver: Receiver<Slot>,
        subscription_receiver: Receiver<Subscription>,
        packet_receiver: Receiver<Vec<PacketBatch>>,
        leader_schedule_cache: LeaderScheduleUpdatingHandle,
        exit: Arc<AtomicBool>,
        leader_lookahead: u64,
    ) -> Result<()> {
        let mut highest_slot = Slot::default();
        let mut packet_subscriptions: HashMap<
            Pubkey,
            TokioSender<result::Result<SubscribePacketsResponse, Status>>,
        > = HashMap::default();

        let mut heartbeat_count = 0;
        let heartbeat_tick = crossbeam_channel::tick(Duration::from_millis(500));
        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(slot_receiver) -> maybe_slot => {
                    Self::update_highest_slot(maybe_slot, &mut highest_slot)?;
                },
                recv(packet_receiver) -> maybe_packet_batches => {
                    let failed_forwards = Self::forward_packets(maybe_packet_batches, &packet_subscriptions, &leader_schedule_cache, &highest_slot, &leader_lookahead)?;
                    Self::drop_connections(failed_forwards, &mut packet_subscriptions);
                },
                recv(subscription_receiver) -> maybe_subscription => {
                    Self::handle_subscription(maybe_subscription, &mut packet_subscriptions, &leader_schedule_cache)?;
                }
                recv(heartbeat_tick) -> stamp => {
                    let failed_heartbeats = Self::handle_heartbeat(&packet_subscriptions, &heartbeat_count);
                    Self::drop_connections(failed_heartbeats, &mut packet_subscriptions);

                    heartbeat_count += 1;
                }
            }
        }
        Ok(())
    }

    fn drop_connections(
        disconnected_pubkeys: Vec<Pubkey>,
        subscriptions: &mut HashMap<
            Pubkey,
            TokioSender<result::Result<SubscribePacketsResponse, Status>>,
        >,
    ) {
        for failed in disconnected_pubkeys {
            if let Some(sender) = subscriptions.remove(&failed) {
                info!("dropping subscription: {:?}", failed);
                drop(sender);
            }
        }
    }

    fn handle_heartbeat(
        subscriptions: &HashMap<
            Pubkey,
            TokioSender<result::Result<SubscribePacketsResponse, Status>>,
        >,
        heartbeat_count: &u64,
    ) -> Vec<Pubkey> {
        let failed_pubkey_updates = subscriptions
            .iter()
            .filter_map(|(pubkey, sender)| {
                // TODO: don't want to use blocking_send here
                if let Err(e) = sender.blocking_send(Ok(SubscribePacketsResponse {
                    header: None,
                    msg: Some(Msg::Heartbeat(Heartbeat {
                        ts: Some(Timestamp::from(SystemTime::now())),
                        count: *heartbeat_count,
                    })),
                })) {
                    error!("failed to heartbeat: {:?}", e);
                    Some(*pubkey)
                } else {
                    None
                }
            })
            .collect();

        failed_pubkey_updates
    }

    fn forward_packets(
        maybe_packet_batches: result::Result<Vec<PacketBatch>, RecvError>,
        subscriptions: &HashMap<
            Pubkey,
            TokioSender<result::Result<SubscribePacketsResponse, Status>>,
        >,
        leader_schedule_cache: &LeaderScheduleUpdatingHandle,
        highest_slot: &u64,
        leader_lookahead: &u64,
    ) -> Result<Vec<Pubkey>> {
        let packet_batches = maybe_packet_batches?;
        let slots: Vec<_> = (*highest_slot
            ..highest_slot + leader_lookahead * NUM_CONSECUTIVE_LEADER_SLOTS)
            .collect();
        let slot_leaders = leader_schedule_cache.leaders_for_slots(&slots);

        let proto_batches: Vec<ProtoPacketBatch> = packet_batches
            .into_iter()
            .map(|b| ProtoPacketBatch {
                packets: b.iter().filter_map(packet_to_proto_packet).collect(),
            })
            .collect();

        let failed_forwards = slot_leaders
            .iter()
            .filter_map(|pubkey| {
                let sender = subscriptions.get(pubkey)?;

                for batch in &proto_batches {
                    if let Err(e) = sender.try_send(Ok(SubscribePacketsResponse {
                        header: Some(Header {
                            ts: Some(Timestamp::from(SystemTime::now())),
                        }),
                        msg: Some(Msg::Batch(batch.clone())),
                    })) {
                        error!("error sending packets to pubkey: {:?}", pubkey);
                        return Some(*pubkey);
                    }
                }

                None
            })
            .collect();
        Ok(failed_forwards)
    }

    fn handle_subscription(
        maybe_subscription: result::Result<Subscription, RecvError>,
        subscriptions: &mut HashMap<
            Pubkey,
            TokioSender<result::Result<SubscribePacketsResponse, Status>>,
        >,
        leader_schedule_cache: &LeaderScheduleUpdatingHandle,
    ) -> Result<()> {
        match maybe_subscription? {
            Subscription::ValidatorPacketSubscription { pubkey, sender } => {
                match subscriptions.entry(pubkey) {
                    Entry::Vacant(entry) => {
                        info!("new subscription: {:?}", pubkey);
                        entry.insert(sender);
                    }
                    Entry::Occupied(_) => {
                        error!("already connected, dropping new connection: {:?}", pubkey);
                        let _ = sender.try_send(Err(Status::resource_exhausted(
                            "validator already connected",
                        )));
                        drop(sender);
                    }
                }
            }
        }
        Ok(())
    }

    fn update_highest_slot(
        maybe_slot: result::Result<u64, RecvError>,
        highest_slot: &mut Slot,
    ) -> Result<()> {
        *highest_slot = maybe_slot?;
        Ok(())
    }

    // Sends packet streams to eligible connections over TCP.
    // Eligible connections are ones where the `Leader` (connected node) has a slot scheduled
    // between `start_slot` & `end_slot`.
    //
    // returns a tuple where:
    //     tuple.0 = list of connection ids (pubkey) where an error was encountered on stream attempt
    //     tuple.1 = a set of slots that were streamed for
    // pub fn stream_batch_list(
    //     &self,
    //     batch_list: &ExpiringPacketBatches,
    //     start_slot: Slot,
    //     end_slot: Slot,
    // ) -> (Vec<Pubkey>, HashSet<Slot>) {
    //     let mut failed_stream_pks: Vec<Pubkey> = Vec::new();
    //     let mut slots_sent: HashSet<Slot> = HashSet::new();
    //
    //     let active_subscriptions = self.packet_subs.read().unwrap();
    //     if active_subscriptions.is_empty() {
    //         warn!("stream_batch_list: No Active Subscriptions");
    //         return (failed_stream_pks, slots_sent);
    //     }
    //
    //     let validators_to_send =
    //         Self::validators_in_slot_range(start_slot, end_slot, &self.leader_schedule_cache);
    //     let iter = active_subscriptions.iter();
    //     for (pk, subscription) in iter {
    //         let slot_to_send = validators_to_send.get(pk);
    //         debug!("Slot to Send: {:?}", slot_to_send);
    //         if let Some(slot) = slot_to_send {
    //             if let Err(e) = subscription.tx.send(Ok(PacketStreamMsg {
    //                 msg: Some(Msg::Batches(batch_list.clone())),
    //             })) {
    //                 datapoint_warn!(
    //                     "validator_interface_stream_batch_list",
    //                     ("subscriber", pk.to_string(), String),
    //                     ("batch_stream_error", 1, i64),
    //                     ("error", e.to_string(), String)
    //                 );
    //                 failed_stream_pks.push(*pk);
    //             } else {
    //                 datapoint_info!(
    //                     "validator_interface_stream_batch_list",
    //                     ("subscriber", pk.to_string(), String),
    //                     ("batches_streamed", batch_list.batch_list.len(), i64)
    //                 );
    //                 slots_sent.insert(*slot);
    //             }
    //         }
    //     }
    //
    //     (failed_stream_pks, slots_sent)
    // }

    // fn validators_in_slot_range(
    //     start_slot: Slot,
    //     end_slot: Slot,
    //     leader_schedule_cache: &LeaderScheduleCache,
    // ) -> HashMap<Pubkey, Slot> {
    //     let n_leader_slots = NUM_CONSECUTIVE_LEADER_SLOTS as usize;
    //     let mut validators_to_send = HashMap::new();
    //     for slot in (start_slot..end_slot).step_by(n_leader_slots) {
    //         if let Some(pk) = leader_schedule_cache.fetch_scheduled_validator(&slot) {
    //             validators_to_send.insert(pk, slot);
    //         }
    //     }
    //     debug!(
    //         "validators_in_slot_range: {}  -  {},   val: {:?}",
    //         start_slot, end_slot, validators_to_send
    //     );
    //
    //     validators_to_send
    // }
}
