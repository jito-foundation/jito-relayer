use std::{
    collections::HashMap,
    net::IpAddr,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use crossbeam_channel::Sender;
use jito_protos::{
    shared::Socket,
    validator_interface::{
        validator_interface_server::ValidatorInterface, AoiSubRequest, AoiSubResponse,
        GetTpuConfigsRequest, GetTpuConfigsResponse, PacketStreamMsg, SubscribeBundlesRequest,
        SubscribeBundlesResponse,
    },
};
use log::{debug, info};
use solana_sdk::pubkey::Pubkey;
use tokio::{
    sync::mpsc::{channel, unbounded_channel},
    task::spawn_blocking,
};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status, Streaming};

use crate::{
    auth::extract_pubkey,
    router::{PacketSubscription, Router},
};

pub struct ValidatorInterfaceServiceImpl {
    packet_subs: Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
    client_disconnect_sender: Sender<Pubkey>,
    pub(crate) public_ip: IpAddr,
    pub(crate) tpu_port: u16,
    pub(crate) tpu_fwd_port: u16,
}

pub struct SubscriberStream<T> {
    inner: ReceiverStream<Result<T, Status>>,
    tx: Sender<Pubkey>,
    client_pubkey: Pubkey,
}

impl<T> Stream for SubscriberStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T> Drop for SubscriberStream<T> {
    fn drop(&mut self) {
        let _ = self.tx.send(self.client_pubkey);
    }
}

impl ValidatorInterfaceServiceImpl {
    pub fn new(
        packet_subs: Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
        client_disconnect_sender: Sender<Pubkey>,
        public_ip: IpAddr,
        tpu_port: u16,
        tpu_fwd_port: u16,
    ) -> Self {
        Self {
            packet_subs,
            client_disconnect_sender,
            public_ip,
            tpu_port,
            tpu_fwd_port,
        }
    }
}

#[tonic::async_trait]
impl ValidatorInterface for ValidatorInterfaceServiceImpl {
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

    type SubscribeBundlesStream = SubscriberStream<SubscribeBundlesResponse>;

    async fn subscribe_bundles(
        &self,
        _: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        Err(Status::unimplemented(
            "subscribe_bundles is not implemented for the relayer",
        ))
    }

    type StartBiDirectionalPacketStreamStream = SubscriberStream<PacketStreamMsg>;

    async fn start_bi_directional_packet_stream(
        &self,
        req: Request<Streaming<PacketStreamMsg>>,
    ) -> Result<Response<Self::StartBiDirectionalPacketStreamStream>, Status> {
        let pubkey = extract_pubkey(req.metadata())?;
        info!("Validator Connected - {}", pubkey);

        let (subscription_sender, mut subscription_receiver) = unbounded_channel();

        let packet_subs = self.packet_subs.clone();
        let connected = spawn_blocking(move || {
            Router::add_packet_subscription(packet_subs, &pubkey, subscription_sender)
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

        Ok(Response::new(SubscriberStream {
            inner: ReceiverStream::new(client_receiver),
            tx: self.client_disconnect_sender.clone(),
            client_pubkey: pubkey,
        }))
    }
    type SubscribeAOIStream = SubscriberStream<AoiSubResponse>;

    async fn subscribe_aoi(
        &self,
        _: Request<AoiSubRequest>,
    ) -> Result<Response<Self::SubscribeAOIStream>, Status> {
        Err(Status::unimplemented(
            "subscribe_aoi is not implemented yet",
        ))
    }
}
