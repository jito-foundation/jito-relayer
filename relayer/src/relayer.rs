use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::task::{Context, Poll};
use std::thread::{JoinHandle, spawn};

use crossbeam_channel::{unbounded, Receiver, Sender, select};
use jito_protos::relayer::{
    relayer_service_server::RelayerService, HeartbeatResponse, HeartbeatSubscriptionRequest,
    PacketSubscriptionRequest, PacketSubscriptionResponse,
};
use jito_protos::packet::{Meta as PbMeta, Packet as PbPacket, PacketBatch as PbPacketBatch, PacketFlags as PbPacketFlags,
};

use log::{debug, error, info, warn};
use solana_core::banking_stage::BankingPacketBatch;
use solana_sdk::clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS};
use solana_sdk::pubkey::Pubkey;
use tokio::{sync::mpsc::{channel, unbounded_channel}, time::sleep};
use tokio::task::spawn_blocking;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::{active_subscriptions::ActiveSubscriptions, auth::extract_pubkey};
use crate::leader_schedule::LeaderScheduleCache;


pub struct Relayer {
    active_subscriptions: Arc<ActiveSubscriptions>,
    client_disconnect_sender: Sender<Pubkey>,
    disconnects_hdl: JoinHandle<()>,
    hb_hdl: JoinHandle<()>,
    pkt_hdl: JoinHandle<()>,
}

impl Relayer {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
        leader_sched: Arc<LeaderScheduleCache>,
        exit: &Arc<AtomicBool>,
    ) -> Relayer {
        let active_subscriptions = Arc::new(ActiveSubscriptions::new(leader_sched.clone()));

        let hb_hdl = Self::broadcast_heartbeats(&active_subscriptions, exit);

        // ToDo: Pipe this sender where it needs to go for signalling disconnect
        let (client_disconnect_sender, closed_disconnect_receiver) = unbounded();
        let disconnects_hdl =
            Self::handle_disconnects_loop(closed_disconnect_receiver, &active_subscriptions);

        let pkt_hdl = Self::packets_receiver_loop(
                    packet_receiver,
                    slot_receiver,
                    &active_subscriptions,
                    3, // Default look_ahead
                    0, // Default look_behind
        );

        Relayer {
            active_subscriptions,
            client_disconnect_sender,
            disconnects_hdl,
            hb_hdl,
            pkt_hdl,
        }
    }

    pub fn join(self) {
        self.disconnects_hdl.join().expect("task panicked");
        self.hb_hdl.join().expect("task panicked");
        self.pkt_hdl.join().expect("task panicked");
    }

    pub fn broadcast_heartbeats(
        active_subscriptions: &Arc<ActiveSubscriptions>,
        exit: &Arc<AtomicBool>
    ) -> JoinHandle<()> {

        let finished = exit.clone();
        let active_subs = active_subscriptions.clone();
        spawn(move || {
            while !finished.load(Ordering::Relaxed) {
                let failed_heartbeats = active_subs.send_heartbeat();
                active_subs.disconnect(&failed_heartbeats);

                std::thread::sleep(Duration::from_millis(500));
            }
        })
    }

    /// Receive packet batches via Receiver and stream them out over UDP or TCP to nodes.
    #[allow(clippy::too_many_arguments)]
    fn packets_receiver_loop(
        packets_rx: Receiver<BankingPacketBatch>,
        slots_rx: Receiver<Slot>,
        active_subscriptions: &Arc<ActiveSubscriptions>,
        // num leaders ahead to send packets to
        look_ahead: u64,
        // num leaders behind to send packets to
        look_behind: u64,
    ) -> JoinHandle<()> {
        let mut current_slot: Slot = 0;
        let active_subs = active_subscriptions.clone();

        spawn(move || {
            loop {
                select! {
                    recv(slots_rx) -> pb_slot => {
                        match pb_slot {
                            Ok(slot) => {
                                current_slot = slot;
                            }
                            Err(_) => {
                                error!("error receiving slot");
                                break;
                            }
                        }
                    }

                    recv(packets_rx) -> bp_batch => {
                        match bp_batch {
                            Ok(bp_batch) => {
                                let batches = bp_batch.0;

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
                                    proto_batch_vec.push(PbPacketBatch {packets: proto_pkt_vec})
                                }

                                let start_slot =
                                    current_slot - (NUM_CONSECUTIVE_LEADER_SLOTS * look_behind);
                                let end_slot =
                                    current_slot + (NUM_CONSECUTIVE_LEADER_SLOTS * look_ahead);

                                let (failed_stream_pks, slots_sent) = active_subs
                                    .stream_batch_list(proto_batch_vec, start_slot, end_slot);
                            }

                             Err(_) => {
                                error!("error receiving slot");
                                break;
                             }
                         }
                    }
                }
            }
        })
    }


    // listen for client disconnects and remove from subscriptions map
    pub fn handle_disconnects_loop(
        rx: Receiver<Pubkey>,
        active_subscriptions: &Arc<ActiveSubscriptions>,
    ) -> JoinHandle<()> {
        let active_subs = active_subscriptions.clone();
        spawn(move || loop {
            match rx.recv() {
                Ok(pk) => {
                    debug!("client [pk={}] disconnected", pk);
                    active_subs.disconnect(&[pk]);
                }
                Err(_) => {
                    warn!("closed connection channel disconnected");
                    break;
                }
            }
        })
    }
}

pub struct ValidatorSubscriberStream<T> {
    inner: ReceiverStream<Result<T, Status>>,
    tx: Sender<Pubkey>,
    client_pubkey: Pubkey,
}

impl<T> Stream for ValidatorSubscriberStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T> Drop for ValidatorSubscriberStream<T> {
    fn drop(&mut self) {
        let _ = self.tx.send(self.client_pubkey);
    }
}

#[tonic::async_trait]
impl RelayerService for Relayer {

    type SubscribeHeartbeatStream = ValidatorSubscriberStream<HeartbeatResponse>;

    async fn subscribe_heartbeat(
        &self,
        req: Request<HeartbeatSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeHeartbeatStream>, Status> {

        let pubkey = extract_pubkey(req.metadata())?;
        let (subscription_sender, mut subscription_receiver) = unbounded_channel();

        let active_subs = self.active_subscriptions.clone();
        let connected = spawn_blocking(move || {
            active_subs.add_heartbeat_subscription(&pubkey, subscription_sender)
        })
            .await
            .map_err(|_| Status::internal("system error adding subscription"))?;

        if !connected {
            return Err(Status::resource_exhausted("user already connected"));
        }

        let (client_sender, client_receiver) = channel(10);
        tokio::spawn(async move {
            info!("validator connected [pubkey={:?}]", pubkey);
            loop {
                match subscription_receiver.recv().await {
                    Some(msg) => {
                        if let Err(e) = client_sender.send(msg).await {
                            debug!("client disconnected [err={}] [pk={}]", e, pubkey);
                            break;
                        }
                    }
                    None => {
                        debug!("unsubscribed [pk={}]", pubkey);
                        let _ = client_sender
                            .send(Err(Status::aborted("disconnected")))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ValidatorSubscriberStream {
            inner: ReceiverStream::new(client_receiver),
            tx: self.client_disconnect_sender.clone(),
            client_pubkey: pubkey,
        }))

    }

    type SubscribePacketsStream = ValidatorSubscriberStream<PacketSubscriptionResponse>;

    async fn subscribe_packets(
        &self,
        req: Request<PacketSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {

        let pubkey = extract_pubkey(req.metadata())?;
        let (subscription_sender, mut subscription_receiver) = unbounded_channel();

        let active_subs = self.active_subscriptions.clone();
        let connected = spawn_blocking(move || {
            active_subs.add_packet_subscription(&pubkey, subscription_sender)
        })
            .await
            .map_err(|_| Status::internal("system error adding subscription"))?;

        if !connected {
            return Err(Status::resource_exhausted("user already connected"));
        }

        // Jed Note: Replace this with shared hashmap of senders
        // see tokio shared state documentation

        let (client_sender, client_receiver) = channel(1_000_000);
        tokio::spawn(async move {
            info!("validator connected [pubkey={:?}]", pubkey);
            loop {
                match subscription_receiver.recv().await {
                    Some(msg) => {
                        if let Err(e) = client_sender.send(msg).await {
                            debug!("client disconnected [err={}] [pk={}]", e, pubkey);
                            break;
                        }
                    }
                    None => {
                        debug!("unsubscribed [pk={}]", pubkey);
                        let _ = client_sender
                            .send(Err(Status::aborted("disconnected")))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ValidatorSubscriberStream {
            inner: ReceiverStream::new(client_receiver),
            tx: self.client_disconnect_sender.clone(),
            client_pubkey: pubkey,
        }))

    }
}
