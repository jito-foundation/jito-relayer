use std::{
    net::{IpAddr, SocketAddr},
    sync::{atomic::AtomicBool, Arc},
    thread,
};

use crossbeam_channel::Receiver;
use jito_protos::{
    relayer::{
        relayer_server::{Relayer, RelayerServer},
        GetTpuConfigsRequest, GetTpuConfigsResponse, SubscribePacketsRequest,
        SubscribePacketsResponse,
    },
    shared::Socket,
};
use log::info;
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::{runtime::Builder, sync::mpsc::channel};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    auth::AuthenticationInterceptor,
    router::{Router, RouterPacketBatches, Subscription},
    schedule_cache::LeaderScheduleUpdatingHandle,
};

pub struct RelayerImpl {
    server_addr: SocketAddr,
    leader_schedule_cache: LeaderScheduleUpdatingHandle,
    router: Router,
    public_ip: IpAddr,
    tpu_port: u16,
    tpu_fwd_port: u16,
}

impl RelayerImpl {
    pub fn new(
        server_addr: SocketAddr,
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<RouterPacketBatches>,
        leader_schedule_cache: LeaderScheduleUpdatingHandle,
        exit: Arc<AtomicBool>,
        public_ip: IpAddr,
        tpu_port: u16,
        tpu_fwd_port: u16,
    ) -> Self {
        let router = Router::new(
            slot_receiver,
            packet_receiver,
            leader_schedule_cache.clone(),
            exit,
        );
        Self {
            server_addr,
            leader_schedule_cache,
            router,
            public_ip,
            tpu_port,
            tpu_fwd_port,
        }
    }

    pub fn start_server(self) {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let addr = self.server_addr;
            let auth_interceptor =
                AuthenticationInterceptor::new(self.leader_schedule_cache.clone());
            let svc = RelayerServer::with_interceptor(self, auth_interceptor);

            info!("starting relayer at: {:?}", addr);
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .expect("serve server");
        });
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
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let pubkey: &Pubkey = request
            .extensions()
            .get()
            .ok_or(Status::internal("internal error fetching public key"))?;

        let (sender, receiver) = channel(1_000);
        self.router
            .add_subscription(Subscription::ValidatorPacketSubscription {
                pubkey: *pubkey,
                sender,
            })
            .map_err(|_| Status::internal("internal error adding subscription"))?;
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
