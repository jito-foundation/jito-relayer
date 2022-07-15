use std::{
    cmp::max,
    collections::{hash_map::Entry, HashMap},
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant, SystemTime},
};

use crossbeam_channel::{unbounded, Receiver, RecvError, SendError, Sender};
use histogram::Histogram;
use jito_protos::{
    convert::packet_to_proto_packet,
    packet::PacketBatch as ProtoPacketBatch,
    relayer::{subscribe_packets_response::Msg, SubscribePacketsResponse},
    shared::{Header, Heartbeat},
};
use log::{error, warn};
use prost_types::Timestamp;
use solana_metrics::datapoint_info;
use solana_perf::packet::PacketBatch;
use solana_sdk::{
    clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use thiserror::Error;
use tokio::sync::mpsc::{error::TrySendError, Sender as TokioSender};
use tonic::Status;

use crate::schedule_cache::LeaderScheduleUpdatingHandle;

pub struct RouterPacketBatches {
    pub stamp: Instant,
    pub batches: Vec<PacketBatch>,
}

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

#[derive(Default)]
struct RouterMetrics {
    pub highest_slot: u64,
    pub num_added_connections: u64,
    pub num_removed_connections: u64,
    pub num_current_connections: u64,
    pub num_heartbeats: u64,
    pub num_batches_forwarded: u64,
    pub num_packets_forwarded: u64,
    pub max_heartbeat_tick_latency_us: u64,
    pub metrics_latency_us: u64,
    pub num_try_send_channel_full: u64,
    pub packet_latencies_us: Histogram,
}

impl RouterMetrics {
    fn report(&self) {
        datapoint_info!(
            "router_metrics",
            ("highest_slot", self.highest_slot, i64),
            ("num_added_connections", self.num_added_connections, i64),
            ("num_removed_connections", self.num_removed_connections, i64),
            ("num_current_connections", self.num_current_connections, i64),
            ("num_heartbeats", self.num_heartbeats, i64),
            ("num_batches_forwarded", self.num_batches_forwarded, i64),
            ("num_packets_forwarded", self.num_packets_forwarded, i64),
            (
                "num_try_send_channel_full",
                self.num_try_send_channel_full,
                i64
            ),
            ("metrics_latency_us", self.metrics_latency_us, i64),
            (
                "max_heartbeat_tick_latency_us",
                self.max_heartbeat_tick_latency_us,
                i64
            ),
            (
                "packet_latencies_us_min",
                self.packet_latencies_us.minimum().unwrap_or_default(),
                i64
            ),
            (
                "packet_latencies_us_max",
                self.packet_latencies_us.maximum().unwrap_or_default(),
                i64
            ),
            (
                "packet_latencies_us_p50",
                self.packet_latencies_us
                    .percentile(50.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "packet_latencies_us_p90",
                self.packet_latencies_us
                    .percentile(90.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "packet_latencies_us_p99",
                self.packet_latencies_us
                    .percentile(99.0)
                    .unwrap_or_default(),
                i64
            ),
        );
    }
}

pub struct Router {
    threads: Vec<JoinHandle<()>>,
    subscription_sender: Sender<Subscription>,
}

impl Router {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<RouterPacketBatches>,
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
        packet_receiver: Receiver<RouterPacketBatches>,
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
        packet_receiver: Receiver<RouterPacketBatches>,
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
        let metrics_tick = crossbeam_channel::tick(Duration::from_millis(1000));

        let mut router_metrics = RouterMetrics::default();

        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(slot_receiver) -> maybe_slot => {
                    Self::update_highest_slot(maybe_slot, &mut highest_slot, &mut router_metrics)?;
                },
                recv(packet_receiver) -> maybe_packet_batches => {
                    let failed_forwards = Self::forward_packets(maybe_packet_batches, &packet_subscriptions, &leader_schedule_cache, &highest_slot, &leader_lookahead, &mut router_metrics)?;
                    Self::drop_connections(failed_forwards, &mut packet_subscriptions, &mut router_metrics);
                },
                recv(subscription_receiver) -> maybe_subscription => {
                    Self::handle_subscription(maybe_subscription, &mut packet_subscriptions, &mut router_metrics)?;
                }
                recv(heartbeat_tick) -> time_generated => {
                    if let Ok(time_generated) = time_generated {
                        router_metrics.max_heartbeat_tick_latency_us = max(router_metrics.max_heartbeat_tick_latency_us, Instant::now().duration_since(time_generated).as_micros() as u64);
                    }

                    let failed_heartbeats = Self::handle_heartbeat(&packet_subscriptions, &heartbeat_count, &mut router_metrics);
                    Self::drop_connections(failed_heartbeats, &mut packet_subscriptions, &mut router_metrics);

                    heartbeat_count += 1;
                }
                recv(metrics_tick) -> time_generated => {
                    router_metrics.num_current_connections = packet_subscriptions.len() as u64;
                    if let Ok(time_generated) = time_generated {
                        router_metrics.metrics_latency_us = time_generated.elapsed().as_micros() as u64;
                    }

                    router_metrics.report();
                    router_metrics = RouterMetrics::default();
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
        router_metrics: &mut RouterMetrics,
    ) {
        router_metrics.num_removed_connections += disconnected_pubkeys.len() as u64;

        for failed in disconnected_pubkeys {
            if let Some(sender) = subscriptions.remove(&failed) {
                datapoint_info!(
                    "router-removed-subscription",
                    ("pubkey", failed.to_string(), String)
                );
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
        router_metrics: &mut RouterMetrics,
    ) -> Vec<Pubkey> {
        let failed_pubkey_updates = subscriptions
            .iter()
            .filter_map(|(pubkey, sender)| {
                match sender.try_send(Ok(SubscribePacketsResponse {
                    header: None,
                    msg: Some(Msg::Heartbeat(Heartbeat {
                        count: *heartbeat_count,
                    })),
                })) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => return Some(*pubkey),
                    Err(TrySendError::Full(_)) => {
                        router_metrics.num_try_send_channel_full += 1;
                        warn!("heartbeat channel is full for: {:?}", pubkey);
                    }
                }
                None
            })
            .collect();

        router_metrics.num_heartbeats += 1;

        failed_pubkey_updates
    }

    fn forward_packets(
        maybe_packet_batches: result::Result<RouterPacketBatches, RecvError>,
        subscriptions: &HashMap<
            Pubkey,
            TokioSender<result::Result<SubscribePacketsResponse, Status>>,
        >,
        leader_schedule_cache: &LeaderScheduleUpdatingHandle,
        highest_slot: &u64,
        leader_lookahead: &u64,
        router_metrics: &mut RouterMetrics,
    ) -> Result<Vec<Pubkey>> {
        let packet_batches = maybe_packet_batches?;

        let proto_batches: Vec<ProtoPacketBatch> = packet_batches
            .batches
            .into_iter()
            .map(|b| ProtoPacketBatch {
                packets: b.iter().filter_map(packet_to_proto_packet).collect(),
            })
            .collect();

        let _ = router_metrics
            .packet_latencies_us
            .increment(packet_batches.stamp.elapsed().as_micros() as u64);

        let slots: Vec<_> = (*highest_slot
            ..highest_slot + leader_lookahead * NUM_CONSECUTIVE_LEADER_SLOTS)
            .collect();
        let slot_leaders = leader_schedule_cache.leaders_for_slots(&slots);

        let failed_forwards = slot_leaders
            .iter()
            .filter_map(|pubkey| {
                let sender = subscriptions.get(pubkey)?;

                for batch in &proto_batches {
                    match sender.try_send(Ok(SubscribePacketsResponse {
                        header: Some(Header {
                            ts: Some(Timestamp::from(SystemTime::now())),
                        }),
                        msg: Some(Msg::Batch(batch.clone())),
                    })) {
                        Ok(_) => {}
                        Err(TrySendError::Full(_)) => {
                            error!("packet channel is full for pubkey: {:?}", pubkey);
                            router_metrics.num_try_send_channel_full += 1;
                            break;
                        }
                        Err(TrySendError::Closed(_)) => {
                            error!("channel is closed for pubkey: {:?}", pubkey);
                            return Some(*pubkey);
                        }
                    }
                }
                None
            })
            .collect();

        router_metrics.num_batches_forwarded += proto_batches.len() as u64;
        router_metrics.num_packets_forwarded += proto_batches
            .iter()
            .map(|b| b.packets.len() as u64)
            .sum::<u64>();

        Ok(failed_forwards)
    }

    fn handle_subscription(
        maybe_subscription: result::Result<Subscription, RecvError>,
        subscriptions: &mut HashMap<
            Pubkey,
            TokioSender<result::Result<SubscribePacketsResponse, Status>>,
        >,
        router_metrics: &mut RouterMetrics,
    ) -> Result<()> {
        match maybe_subscription? {
            Subscription::ValidatorPacketSubscription { pubkey, sender } => {
                match subscriptions.entry(pubkey) {
                    Entry::Vacant(entry) => {
                        entry.insert(sender);

                        router_metrics.num_added_connections += 1;
                        datapoint_info!(
                            "router-new-subscription",
                            ("pubkey", pubkey.to_string(), String)
                        );
                    }
                    Entry::Occupied(_) => {
                        datapoint_info!(
                            "router-duplicate-subscription",
                            ("pubkey", pubkey.to_string(), String)
                        );
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
        router_metrics: &mut RouterMetrics,
    ) -> Result<()> {
        *highest_slot = maybe_slot?;
        router_metrics.highest_slot = *highest_slot;
        Ok(())
    }
}
