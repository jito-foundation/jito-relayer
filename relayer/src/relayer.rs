use std::{
    cmp::max,
    collections::{hash_map::Entry, HashMap},
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

use crossbeam_channel::{unbounded, Receiver, RecvError, Sender};
use histogram::Histogram;
use jito_protos::{
    convert::packet_to_proto_packet,
    packet::{Packet as ProtoPacket, PacketBatch as ProtoPacketBatch},
    relayer::{
        relayer_server::Relayer, subscribe_packets_response, GetTpuConfigsRequest,
        GetTpuConfigsResponse, SubscribePacketsRequest, SubscribePacketsResponse,
    },
    shared::{Header, Heartbeat, Socket},
};
use log::*;
use prost_types::Timestamp;
use solana_metrics::datapoint_info;
use solana_perf::packet::PacketBatch;
use solana_sdk::{
    clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use thiserror::Error;
use tokio::sync::mpsc::{channel, error::TrySendError, Sender as TokioSender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{health_manager::HealthState, schedule_cache::LeaderScheduleUpdatingHandle};

#[derive(Default)]
struct RelayerMetrics {
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

impl RelayerMetrics {
    fn report(&self, cluster: &str, region: &str) {
        datapoint_info!(
            "router_metrics",
            "cluster" => cluster,
            "region" => region,
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

pub struct RouterPacketBatches {
    pub stamp: Instant,
    pub batches: Vec<PacketBatch>,
}

pub enum Subscription {
    ValidatorPacketSubscription {
        pubkey: Pubkey,
        sender: TokioSender<Result<SubscribePacketsResponse, Status>>,
    },
}

#[derive(Error, Debug)]
pub enum RouterError {
    #[error("shutdown")]
    Shutdown(#[from] RecvError),
}

pub type RelayerResult<T> = Result<T, RouterError>;

pub struct RelayerImpl {
    tpu_port: u16,
    tpu_fwd_port: u16,
    public_ip: IpAddr,

    subscription_sender: Sender<Subscription>,
    threads: Vec<JoinHandle<()>>,
    health_state: Arc<RwLock<HealthState>>,
}

impl RelayerImpl {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<RouterPacketBatches>,
        leader_schedule_cache: LeaderScheduleUpdatingHandle,
        exit: &Arc<AtomicBool>,
        public_ip: IpAddr,
        tpu_port: u16,
        tpu_fwd_port: u16,
        health_state: Arc<RwLock<HealthState>>,
        cluster: String,
        region: String,
    ) -> Self {
        const LEADER_LOOKAHEAD: u64 = 2;

        let (subscription_sender, subscription_receiver) = unbounded();
        let threads = vec![Self::start_router_thread(
            slot_receiver,
            subscription_receiver,
            packet_receiver,
            leader_schedule_cache,
            exit,
            LEADER_LOOKAHEAD,
            health_state.clone(),
            cluster,
            region,
        )];

        Self {
            tpu_port,
            tpu_fwd_port,
            subscription_sender,
            public_ip,
            threads,
            health_state,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.threads {
            t.join()?;
        }
        Ok(())
    }

    fn start_router_thread(
        slot_receiver: Receiver<Slot>,
        subscription_receiver: Receiver<Subscription>,
        packet_receiver: Receiver<RouterPacketBatches>,
        leader_schedule_cache: LeaderScheduleUpdatingHandle,
        exit: &Arc<AtomicBool>,
        leader_lookahead: u64,
        health_state: Arc<RwLock<HealthState>>,
        cluster: String,
        region: String,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        thread::Builder::new()
            .name("jito-packet-router".into())
            .spawn(move || {
                let _ = Self::run_event_loop(
                    slot_receiver,
                    subscription_receiver,
                    packet_receiver,
                    leader_schedule_cache,
                    exit,
                    leader_lookahead,
                    health_state,
                    cluster,
                    region,
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
        health_state: Arc<RwLock<HealthState>>,
        cluster: String,
        region: String,
    ) -> RelayerResult<()> {
        let mut highest_slot = Slot::default();
        let mut packet_subscriptions: HashMap<
            Pubkey,
            TokioSender<Result<SubscribePacketsResponse, Status>>,
        > = HashMap::default();

        let mut heartbeat_count = 0;

        let heartbeat_tick = crossbeam_channel::tick(Duration::from_millis(500));
        let metrics_tick = crossbeam_channel::tick(Duration::from_millis(1000));

        let mut router_metrics = RelayerMetrics::default();

        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(slot_receiver) -> maybe_slot => {
                    Self::update_highest_slot(maybe_slot, &mut highest_slot, &mut router_metrics)?;
                },
                recv(packet_receiver) -> maybe_packet_batches => {
                    let failed_forwards = Self::forward_packets(maybe_packet_batches, &packet_subscriptions, &leader_schedule_cache, &highest_slot, &leader_lookahead, &mut router_metrics)?;
                    Self::drop_connections(failed_forwards, &mut packet_subscriptions, &mut router_metrics, &cluster, &region);
                },
                recv(subscription_receiver) -> maybe_subscription => {
                    Self::handle_subscription(maybe_subscription, &mut packet_subscriptions, &mut router_metrics, &region, &cluster)?;
                }
                recv(heartbeat_tick) -> time_generated => {
                    if let Ok(time_generated) = time_generated {
                        router_metrics.max_heartbeat_tick_latency_us = max(router_metrics.max_heartbeat_tick_latency_us, Instant::now().duration_since(time_generated).as_micros() as u64);
                    }

                    // heartbeat if state is healthy, drop all connections on unhealthy
                    let pubkeys_to_drop = if *health_state.read().unwrap() != HealthState::Healthy {
                        packet_subscriptions.keys().cloned().collect()
                    } else {
                        heartbeat_count += 1;
                        Self::handle_heartbeat(&packet_subscriptions, &heartbeat_count, &mut router_metrics)
                    };
                    Self::drop_connections(pubkeys_to_drop, &mut packet_subscriptions, &mut router_metrics, &cluster, &region);
                }
                recv(metrics_tick) -> time_generated => {
                    router_metrics.num_current_connections = packet_subscriptions.len() as u64;
                    if let Ok(time_generated) = time_generated {
                        router_metrics.metrics_latency_us = time_generated.elapsed().as_micros() as u64;
                    }

                    router_metrics.report(&cluster, &region);
                    router_metrics = RelayerMetrics::default();
                }
            }
        }
        Ok(())
    }

    fn drop_connections(
        disconnected_pubkeys: Vec<Pubkey>,
        subscriptions: &mut HashMap<Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>>,
        router_metrics: &mut RelayerMetrics,
        cluster: &str,
        region: &str,
    ) {
        router_metrics.num_removed_connections += disconnected_pubkeys.len() as u64;

        for failed in disconnected_pubkeys {
            if let Some(sender) = subscriptions.remove(&failed) {
                datapoint_info!(
                    "router-removed-subscription",
                    "cluster" => cluster,
                    "region" => region,
                    ("pubkey", failed.to_string(), String)
                );
                drop(sender);
            }
        }
    }

    fn handle_heartbeat(
        subscriptions: &HashMap<Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>>,
        heartbeat_count: &u64,
        router_metrics: &mut RelayerMetrics,
    ) -> Vec<Pubkey> {
        let failed_pubkey_updates = subscriptions
            .iter()
            .filter_map(|(pubkey, sender)| {
                // try send because it's a bounded channel and we don't want to block if the channel is full
                match sender.try_send(Ok(SubscribePacketsResponse {
                    header: None,
                    msg: Some(subscribe_packets_response::Msg::Heartbeat(Heartbeat {
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
        maybe_packet_batches: Result<RouterPacketBatches, RecvError>,
        subscriptions: &HashMap<Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>>,
        leader_schedule_cache: &LeaderScheduleUpdatingHandle,
        highest_slot: &u64,
        leader_lookahead: &u64,
        router_metrics: &mut RelayerMetrics,
    ) -> RelayerResult<Vec<Pubkey>> {
        let packet_batches = maybe_packet_batches?;

        let _ = router_metrics
            .packet_latencies_us
            .increment(packet_batches.stamp.elapsed().as_micros() as u64);

        let slots: Vec<_> = (*highest_slot
            ..highest_slot + leader_lookahead * NUM_CONSECUTIVE_LEADER_SLOTS)
            .collect();
        let slot_leaders = leader_schedule_cache.leaders_for_slots(&slots);

        let proto_packet_batch = ProtoPacketBatch {
            packets: packet_batches
                .batches
                .into_iter()
                .flat_map(|b| {
                    b.iter()
                        .filter_map(packet_to_proto_packet)
                        .collect::<Vec<ProtoPacket>>()
                })
                .collect(),
        };

        let failed_forwards = slot_leaders
            .iter()
            .filter_map(|pubkey| {
                let sender = subscriptions.get(pubkey)?;

                if proto_packet_batch.packets.is_empty() {
                    return None;
                }

                // try send because it's a bounded channel and we don't want to block if the channel is full
                match sender.try_send(Ok(SubscribePacketsResponse {
                    header: Some(Header {
                        ts: Some(Timestamp::from(SystemTime::now())),
                    }),
                    msg: Some(subscribe_packets_response::Msg::Batch(
                        proto_packet_batch.clone(),
                    )),
                })) {
                    Ok(_) => {
                        router_metrics.num_batches_forwarded += 1;
                        router_metrics.num_packets_forwarded +=
                            proto_packet_batch.packets.len() as u64;
                        None
                    }
                    Err(TrySendError::Full(_)) => {
                        error!("packet channel is full for pubkey: {:?}", pubkey);
                        router_metrics.num_try_send_channel_full += 1;
                        None
                    }
                    Err(TrySendError::Closed(_)) => {
                        error!("channel is closed for pubkey: {:?}", pubkey);
                        Some(*pubkey)
                    }
                }
            })
            .collect();
        Ok(failed_forwards)
    }

    fn handle_subscription(
        maybe_subscription: Result<Subscription, RecvError>,
        subscriptions: &mut HashMap<Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>>,
        router_metrics: &mut RelayerMetrics,
        cluster: &str,
        region: &str,
    ) -> RelayerResult<()> {
        match maybe_subscription? {
            Subscription::ValidatorPacketSubscription { pubkey, sender } => {
                match subscriptions.entry(pubkey) {
                    Entry::Vacant(entry) => {
                        entry.insert(sender);

                        router_metrics.num_added_connections += 1;
                        datapoint_info!(
                            "router-new-subscription",
                            "cluster" => cluster,
                            "region" => region,
                            ("pubkey", pubkey.to_string(), String)
                        );
                    }
                    Entry::Occupied(mut entry) => {
                        datapoint_info!(
                            "router-duplicate-subscription",
                            "cluster" => cluster,
                            "region" => region,
                            ("pubkey", pubkey.to_string(), String)
                        );
                        error!("already connected, dropping old connection: {:?}", pubkey);
                        entry.insert(sender);
                    }
                }
            }
        }
        Ok(())
    }

    fn update_highest_slot(
        maybe_slot: Result<u64, RecvError>,
        highest_slot: &mut Slot,
        router_metrics: &mut RelayerMetrics,
    ) -> RelayerResult<()> {
        *highest_slot = maybe_slot?;
        router_metrics.highest_slot = *highest_slot;
        Ok(())
    }

    /// Prevent validators from authenticating if the relayer is unhealthy
    fn check_health(health_state: &Arc<RwLock<HealthState>>) -> Result<(), Status> {
        if *health_state.read().unwrap() != HealthState::Healthy {
            Err(Status::internal("relayer is unhealthy"))
        } else {
            Ok(())
        }
    }
}

#[tonic::async_trait]
impl Relayer for RelayerImpl {
    /// Validator calls this to get the public IP of the relayers TPU and TPU forward sockets.
    async fn get_tpu_configs(
        &self,
        _: Request<GetTpuConfigsRequest>,
    ) -> Result<Response<GetTpuConfigsResponse>, Status> {
        return Ok(Response::new(GetTpuConfigsResponse {
            tpu: Some(Socket {
                ip: self.public_ip.to_string(),
                port: self.tpu_port as i64,
            }),
            tpu_forward: Some(Socket {
                ip: self.public_ip.to_string(),
                port: self.tpu_fwd_port as i64,
            }),
        }));
    }

    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;

    /// Validator calls this to subscribe to packets
    async fn subscribe_packets(
        &self,
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        Self::check_health(&self.health_state)?;

        let pubkey: &Pubkey = request
            .extensions()
            .get()
            .ok_or_else(|| Status::internal("internal error fetching public key"))?;

        let (sender, receiver) = channel(50_000);
        self.subscription_sender
            .send(Subscription::ValidatorPacketSubscription {
                pubkey: *pubkey,
                sender,
            })
            .map_err(|_| Status::internal("internal error adding subscription"))?;
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
