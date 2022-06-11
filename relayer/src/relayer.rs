use jito_protos::relayer::{
    relayer_service_server::RelayerService, HeartbeatResponse, HeartbeatSubscriptionRequest,
    PacketSubscriptionRequest, PacketSubscriptionResponse,
};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::router::Router;

pub struct Relayer {
    router: Router,
}

impl Relayer {
    pub fn new() -> Relayer {
        let router = Router::new();
        Relayer { router }
    }

    pub fn subscribe_heartbeat(
        &self,
        pubkey: Pubkey,
        sender: Sender<Result<HeartbeatResponse, Status>>,
        uuid: Uuid,
    ) -> Result<(), Status> {
        Ok(())
    }

    pub fn unsubscribe(&mut self, pubkey: Pubkey, uuid: Uuid) -> Result<(), Status> {
        Ok(())
    }
}

#[tonic::async_trait]
impl RelayerService for Relayer {
    type SubscribeHeartbeatStream = ReceiverStream<Result<HeartbeatResponse, Status>>;

    async fn subscribe_heartbeat(
        &self,
        request: Request<HeartbeatSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeHeartbeatStream>, Status> {
        let (sender, receiver) = channel(2);
        self.subscribe_heartbeat(Pubkey::new_unique(), sender, Uuid::new_v4())?;

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    type SubscribePacketsStream = ReceiverStream<Result<PacketSubscriptionResponse, Status>>;

    async fn subscribe_packets(
        &self,
        request: Request<PacketSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let (sender, receiver) = channel(100);

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
