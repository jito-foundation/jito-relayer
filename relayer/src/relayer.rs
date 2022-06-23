use std::{
    net::IpAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    task::{Context, Poll},
    thread::{spawn, JoinHandle},
    time::Duration,
};

use crossbeam_channel::{select, unbounded, Receiver, Sender};
use jito_protos::{
    shared::Socket,
    validator_interface_service::{
        validator_interface_server::ValidatorInterface, GetTpuConfigsRequest,
        GetTpuConfigsResponse, SubscribeBundlesRequest, SubscribeBundlesResponse,
        SubscribePacketsRequest, SubscribePacketsResponse,
    },
};
use log::*;
use solana_core::banking_stage::BankingPacketBatch;
use solana_sdk::{
    clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use tokio::{
    sync::mpsc::{channel, unbounded_channel},
    task::spawn_blocking,
};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::{auth::extract_pubkey, router::Router, schedule_cache::LeaderScheduleCache};

pub struct Relayer {
    router: Arc<Router>,
    public_ip: IpAddr,
    tpu_port: u16,
    tpu_fwd_port: u16,
    client_disconnect_sender: Sender<Pubkey>,
    disconnects_hdl: JoinHandle<()>,
    hb_hdl: JoinHandle<()>,
    pkt_loop_hdl: JoinHandle<()>,
}

impl Relayer {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
        leader_schedule_cache: Arc<RwLock<LeaderScheduleCache>>,
        exit: Arc<AtomicBool>,
        public_ip: IpAddr,
        tpu_port: u16,
        tpu_fwd_port: u16,
    ) -> Self {
        let router = Arc::new(Router::new(
            slot_receiver,
            packet_receiver,
            leader_schedule_cache,
        ));

        // ToDo: hb loop contains hacky leader schedule update
        let hb_hdl = Self::start_heartbeat_loop(router.clone(), exit.clone());
        let pkt_loop_hdl = Self::start_packets_receiver_loop(router.clone(), exit, 3, 0);

        let (client_disconnect_sender, closed_disconnect_receiver) = unbounded();
        let disconnects_hdl =
            Self::handle_disconnects_loop(closed_disconnect_receiver, router.clone());

        Self {
            router,
            public_ip,
            tpu_port,
            tpu_fwd_port,
            client_disconnect_sender,
            disconnects_hdl,
            hb_hdl,
            pkt_loop_hdl,
        }
    }

    pub fn join(self) {
        self.disconnects_hdl.join().expect("task panicked");
        self.hb_hdl.join().expect("task panicked");
        self.pkt_loop_hdl.join().expect("task panicked");
    }

    // listen for client disconnects and remove from subscriptions map
    pub fn handle_disconnects_loop(rx: Receiver<Pubkey>, router: Arc<Router>) -> JoinHandle<()> {
        spawn(move || loop {
            match rx.recv() {
                Ok(pk) => {
                    debug!("client [pk={}] disconnected", pk);
                    router.disconnect(&[pk]);
                }
                Err(_) => {
                    warn!("closed connection channel disconnected");
                    break;
                }
            }
        })
    }

    pub fn start_heartbeat_loop(router: Arc<Router>, exit: Arc<AtomicBool>) -> JoinHandle<()> {
        info!("Started Heartbeat");

        spawn(move || {
            let mut n_hb = 0usize;
            while !exit.load(Ordering::Relaxed) {
                // Hacky!!!!: Update Leader Cache every 30 seconds
                if n_hb % 30 == 0 {
                    router
                        .leader_schedule_cache
                        .write()
                        .unwrap()
                        .update_leader_cache();
                };
                let failed_heartbeats = router.send_heartbeat();
                router.disconnect(&failed_heartbeats);

                std::thread::sleep(Duration::from_millis(500));
                n_hb += 1;
            }
        })
    }

    /// Receive packet batches via Receiver and stream them out over UDP or TCP to nodes.
    #[allow(clippy::too_many_arguments)]
    pub fn start_packets_receiver_loop(
        router: Arc<Router>,
        exit: Arc<AtomicBool>,
        // num leaders ahead to send packets to
        look_ahead: u64,
        // num leaders behind to send packets to
        look_behind: u64,
    ) -> JoinHandle<()> {
        let mut current_slot: Slot = 0;

        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                select! {
                    recv(router.slot_receiver) -> maybe_slot => {
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

                    recv(router.packet_receiver) -> maybe_bp_batch => {
                        match maybe_bp_batch {
                            Ok(bp_batch) =>  {
                                let batches = bp_batch.0;
                                if !batches.is_empty() {
                                    info!(
                                        "Got Batch of length {} x {}",
                                        batches.len(),
                                        batches[0].len()
                                    );
                                }

                                let proto_bl = Router::batchlist_to_proto(batches);

                                let start_slot = current_slot - (NUM_CONSECUTIVE_LEADER_SLOTS * look_behind);
                                let end_slot = current_slot + (NUM_CONSECUTIVE_LEADER_SLOTS * look_ahead);
                                let (failed_stream_pks, _slots_sent) = router.stream_batch_list(&proto_bl, start_slot, end_slot);

                                // close the connections
                                router.disconnect(&failed_stream_pks);
                            }
                            Err(_) => {
                                error!("error receiving packets");
                                break;
                            }
                        }
                    }
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
impl ValidatorInterface for Relayer {
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

    type SubscribeBundlesStream = ValidatorSubscriberStream<SubscribeBundlesResponse>;

    async fn subscribe_bundles(
        &self,
        _: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        error!("Bundle Subscriptions not yet available on relayer!!!");
        unimplemented!();
    }

    type SubscribePacketsStream = ValidatorSubscriberStream<SubscribePacketsResponse>;

    async fn subscribe_packets(
        &self,
        req: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        info!("Validator Connected!!!!!!!!!");
        // ToDo: Which One of the following is best?  Depends on Auth?
        let pubkey = extract_pubkey(req.metadata())?;
        // let pubkey = *req
        //     .extensions()
        //     .get::<Pubkey>()
        //     .ok_or_else(|| Status::internal("pubkey error"))?;

        let (subscription_sender, mut subscription_receiver) = unbounded_channel();

        let router = self.router.clone();
        let connected =
            spawn_blocking(move || router.add_packet_subscription(&pubkey, subscription_sender))
                .await
                .map_err(|_| Status::internal("system error adding subscription"))?;

        if !connected {
            return Err(Status::resource_exhausted("user already connected"));
        }

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
