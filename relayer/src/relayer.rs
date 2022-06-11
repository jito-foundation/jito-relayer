use std::time::Duration;

use crossbeam_channel::Receiver;
use jito_protos::relayer::{
    relayer_service_server::RelayerService, HeartbeatResponse, HeartbeatSubscriptionRequest,
    PacketSubscriptionRequest, PacketSubscriptionResponse,
};
use log::error;
use solana_core::banking_stage::BankingPacketBatch;
use solana_sdk::clock::Slot;
use tokio::{sync::mpsc::channel, time::sleep};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::router::Router;

pub struct Relayer {
    router: Router,
}

impl Relayer {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
    ) -> Relayer {
        let router = Router::new(slot_receiver, packet_receiver);
        Relayer { router }
    }
}

#[tonic::async_trait]
impl RelayerService for Relayer {
    type SubscribeHeartbeatStream = ReceiverStream<Result<HeartbeatResponse, Status>>;

    async fn subscribe_heartbeat(
        &self,
        _request: Request<HeartbeatSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeHeartbeatStream>, Status> {
        let (sender, receiver) = channel(2);

        tokio::spawn(async move {
            if let Err(e) = sender.send(Ok(HeartbeatResponse::default())).await {
                error!("subscribe_heartbeat error sending response: {:?}", e);
            }
            sleep(Duration::from_millis(500)).await;
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    type SubscribePacketsStream = ReceiverStream<Result<PacketSubscriptionResponse, Status>>;

    async fn subscribe_packets(
        &self,
        _request: Request<PacketSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let (sender, receiver) = channel(100);

        tokio::spawn(async move {
            if let Err(e) = sender.send(Ok(PacketSubscriptionResponse::default())).await {
                error!("subscribe_packets error sending response: {:?}", e);
            }
            sleep(Duration::from_millis(500)).await;
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
