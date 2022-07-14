use std::{
    net::IpAddr,
    sync::{atomic::AtomicBool, Arc},
    thread,
};

use crossbeam_channel::Receiver;
use jito_protos::{
    relayer::{
        relayer_server::Relayer, GetTpuConfigsRequest, GetTpuConfigsResponse,
        SubscribePacketsRequest, SubscribePacketsResponse,
    },
    shared::Socket,
};
use solana_perf::packet::PacketBatch;
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    router::{Router, Subscription},
    schedule_cache::LeaderScheduleUpdatingHandle,
};

pub struct RelayerImpl {
    router: Router,
    public_ip: IpAddr,
    tpu_port: u16,
    tpu_fwd_port: u16,
}

impl RelayerImpl {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<Vec<PacketBatch>>,
        leader_schedule_cache: LeaderScheduleUpdatingHandle,
        exit: Arc<AtomicBool>,
        public_ip: IpAddr,
        tpu_port: u16,
        tpu_fwd_port: u16,
    ) -> Self {
        let router = Router::new(slot_receiver, packet_receiver, leader_schedule_cache, exit);
        Self {
            router,
            public_ip,
            tpu_port,
            tpu_fwd_port,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.router.join()
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
        _request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let (sender, receiver) = channel(1_000);
        self.router
            .add_subscription(Subscription::ValidatorPacketSubscription {
                pubkey: Pubkey::new_unique(), // TODO fill this out
                sender,
            })
            .map_err(|e| Status::internal("internal error adding subscription"))?;
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
