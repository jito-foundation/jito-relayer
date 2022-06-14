use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::task::{Context, Poll};
use std::thread::{JoinHandle, spawn};

use crossbeam_channel::{unbounded, Receiver, Sender};
use jito_protos::relayer::{
    relayer_service_server::RelayerService, HeartbeatResponse, HeartbeatSubscriptionRequest,
    PacketSubscriptionRequest, PacketSubscriptionResponse,
};
use log::{debug, error, info, warn};
use solana_client::rpc_client::RpcClient;
use solana_core::banking_stage::BankingPacketBatch;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use tokio::{sync::mpsc::{channel, unbounded_channel}, time::sleep};
use tokio::task::spawn_blocking;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::{active_subscriptions::ActiveSubscriptions, auth::{extract_pubkey,AuthenticationInterceptor}};
use crate::leader_schedule::LeaderScheduleCache;


pub struct Relayer {
    active_subscriptions: Arc<ActiveSubscriptions>,
    client_disconnect_sender: Sender<Pubkey>,
    disconnects_hdl: JoinHandle<()>,
}

impl Relayer {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
        leader_sched: Arc<LeaderScheduleCache>,
        exit: &Arc<AtomicBool>,
    ) -> Relayer {
        let active_subscriptions = Arc::new(ActiveSubscriptions::new(leader_sched.clone()));

        //********* Broadcast Heartbeats ***************
        let active_subs = active_subscriptions.clone();
        let finished = exit.clone();
        spawn(move || {
            while !finished.load(Ordering::Relaxed) {
                let failed_heartbeats = active_subs.send_heartbeat();
                active_subs.disconnect(&failed_heartbeats);

                std::thread::sleep(Duration::from_millis(500));
            }
        });
        //************************************************


        let (client_disconnect_sender, closed_disconnect_receiver) = unbounded();
        let disconnects_hdl =
            Self::handle_disconnects_loop(closed_disconnect_receiver, active_subscriptions.clone());

        Relayer {
            active_subscriptions,
            client_disconnect_sender,
            disconnects_hdl
        }
    }

    pub fn join(self) {
        self.disconnects_hdl.join().expect("task panicked");
    }

    // listen for client disconnects and remove from subscriptions map
    pub fn handle_disconnects_loop(
        rx: Receiver<Pubkey>,
        active_subscriptions: Arc<ActiveSubscriptions>,
    ) -> JoinHandle<()> {
        spawn(move || loop {
            match rx.recv() {
                Ok(pk) => {
                    debug!("client [pk={}] disconnected", pk);
                    active_subscriptions.disconnect(&[pk]);
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

        // ToDo: Fix this in auth.rs
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

        let pubkey = *req
            .extensions()
            .get::<Pubkey>()
            .ok_or_else(|| Status::internal("pubkey error"))?;
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
