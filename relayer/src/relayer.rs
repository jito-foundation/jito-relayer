use jito_protos::relayer::{
    relayer_service_server::RelayerService, HeartbeatResponse, HeartbeatSubscriptionRequest,
    PacketSubscriptionRequest, PacketSubscriptionResponse,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct Relayer {}

#[tonic::async_trait]
impl RelayerService for Relayer {
    type SubscribeHeartbeatStream = ReceiverStream<Result<HeartbeatResponse, Status>>;

    async fn subscribe_heartbeat(
        &self,
        request: Request<HeartbeatSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeHeartbeatStream>, Status> {
        todo!()
    }

    type SubscribePacketsStream = ReceiverStream<Result<PacketSubscriptionResponse, Status>>;

    async fn subscribe_packets(
        &self,
        request: Request<PacketSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        todo!()
    }
}
