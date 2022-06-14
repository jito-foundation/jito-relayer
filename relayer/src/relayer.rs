use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::task::{Context, Poll};
use std::thread::{JoinHandle, spawn};

use crossbeam_channel::{unbounded, Receiver, Sender};
use jito_protos::relayer::{
    relayer_service_server::RelayerService, HeartbeatResponse, HeartbeatSubscriptionRequest,
    PacketSubscriptionRequest, PacketSubscriptionResponse,
};
use log::{debug, error, info, warn};
use solana_core::banking_stage::BankingPacketBatch;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use tokio::{sync::mpsc::{channel, unbounded_channel}, time::sleep};
use tokio::task::spawn_blocking;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::router::Router;
use crate::{active_subscriptions::{ActiveSubscriptions, LeaderScheduleCache}, auth::extract_pubkey};

pub struct Relayer {
    router: Router,
    active_subscriptions: Arc<ActiveSubscriptions>,
    client_disconnect_sender: Sender<Pubkey>,
    disconnects_hdl: JoinHandle<()>,
}

impl Relayer {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
        rpc_list: Vec<String>,
    ) -> Relayer {
        let router = Router::new(slot_receiver, packet_receiver, rpc_list);
        // ToDo: New LeaderScheduleCache here from rpc
        let active_subscriptions = Arc::new(ActiveSubscriptions::new(Arc::new(LeaderScheduleCache {})));
        // Broadcast Heartbeats
        let active_subs = active_subscriptions.clone();
        spawn(move || {
            // ToDo: add proper exit here
            loop {
                let failed_heartbeats = active_subs.send_heartbeat();
                active_subscriptions.disconnect(&failed_heartbeats);

                std::thread::sleep(Duration::from_millis(500));
            }
        });

        let (client_disconnect_sender, closed_disconnect_receiver) = unbounded();
        let disconnects_hdl =
            Self::handle_disconnects_loop(closed_disconnect_receiver, active_subscriptions.clone());

        Relayer {
            router,
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

    // type HeartbeatSender = UnboundedSender<Result<HeartbeatResponse, Status>>
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


        // let (sender, receiver) = channel(2);
        //
        //
        // tokio::spawn(async move {
        //     if let Err(e) = sender.send(Ok(HeartbeatResponse::default())).await {
        //         error!("subscribe_heartbeat error sending response: {:?}", e);
        //     }
        //     sleep(Duration::from_millis(500)).await;
        // });
        //
        // Ok(Response::new(ReceiverStream::new(receiver)))
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


        // let (sender, receiver) = channel(100);
        //
        //
        // // Jed Note: Replace this with shared hashmap of senders
        // // see tokio
        //
        //
        //
        // tokio::spawn(async move {
        //     if let Err(e) = sender.send(Ok(PacketSubscriptionResponse::default())).await {
        //         error!("subscribe_packets error sending response: {:?}", e);
        //     }
        //     sleep(Duration::from_millis(500)).await;
        // });
        //
        // Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
