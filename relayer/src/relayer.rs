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
use solana_sdk::clock::Slot;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{router::Router, schedule_cache::LeaderScheduleCache};

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
        leader_schedule_cache: Arc<LeaderScheduleCache>,
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

    // listen for client disconnects and remove from subscriptions map
    // pub fn handle_disconnects_loop(rx: Receiver<Pubkey>, router: Arc<Router>) -> JoinHandle<()> {
    //     spawn(move || loop {
    //         match rx.recv() {
    //             Ok(pk) => {
    //                 debug!("client [pk={}] disconnected", pk);
    //                 router.disconnect(&[pk]);
    //             }
    //             Err(_) => {
    //                 warn!("closed connection channel disconnected");
    //                 break;
    //             }
    //         }
    //     })
    // }

    // pub fn start_heartbeat_loop(router: Arc<Router>, exit: Arc<AtomicBool>) -> JoinHandle<()> {
    //     info!("Started Heartbeat");
    //
    //     spawn(move || {
    //         let mut count: u64 = 0;
    //         while !exit.load(Ordering::Relaxed) {
    //             let failed_heartbeats = router.send_heartbeat(&count);
    //             router.disconnect(&failed_heartbeats);
    //             sleep(Duration::from_millis(500));
    //             count += 1;
    //         }
    //     })
    // }

    // Receive packet batches via Receiver and stream them out over grpc to nodes.
    // #[allow(clippy::too_many_arguments)]
    // pub fn start_packets_receiver_loop(
    //     router: Arc<Router>,
    //     exit: Arc<AtomicBool>,
    //     // num leaders ahead to send packets to
    //     look_ahead: u64,
    //     // num leaders behind to send packets to
    //     look_behind: u64,
    // ) -> Vec<JoinHandle<()>> {
    //     let mut handles = Vec::new();
    //
    //     let router_l = router.clone();
    //     let exit_l = exit.clone();
    //     let slt_hdl = spawn(move || {
    //         while !exit_l.load(Ordering::Relaxed) {
    //             match router_l.slot_receiver.recv() {
    //                 Ok(slot) => {
    //                     *router_l.current_slot.write().unwrap() = slot;
    //                 }
    //                 Err(_) => {
    //                     error!("error receiving slot");
    //                     break;
    //                 }
    //             }
    //         }
    //     });
    //     handles.push(slt_hdl);
    //
    //     let pkt_hdl = spawn(move || {
    //         while !exit.load(Ordering::Relaxed) {
    //             match router.packet_receiver.recv() {
    //                 Ok(packet_batch) => {
    //                     let current_slot = *router.current_slot.read().unwrap();
    //                     let start_slot =
    //                         current_slot - (NUM_CONSECUTIVE_LEADER_SLOTS * look_behind);
    //                     let end_slot = current_slot + (NUM_CONSECUTIVE_LEADER_SLOTS * look_ahead);
    //                     let (failed_stream_pks, _slots_sent) =
    //                         router.stream_batch_list(&packet_batch, start_slot, end_slot);
    //
    //                     // close the connections
    //                     router.disconnect(&failed_stream_pks);
    //                 }
    //                 Err(_) => {
    //                     error!("error receiving packets");
    //                     break;
    //                 }
    //             }
    //         }
    //     });
    //     handles.push(pkt_hdl);
    //
    //     handles
    // }
}

#[tonic::async_trait]
impl Relayer for RelayerImpl {
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

    async fn subscribe_packets(
        &self,
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        Err(Status::unimplemented("subscribe_packets"))
    }

    //
    // type StartBiDirectionalPacketStreamStream = ValidatorSubscriberStream<PacketStreamMsg>;
    //
    // async fn start_bi_directional_packet_stream(
    //     &self,
    //     request: Request<Streaming<PacketStreamMsg>>,
    // ) -> Result<Response<Self::StartBiDirectionalPacketStreamStream>, Status> {
    //     let pubkey = extract_pubkey(request.metadata())?;
    //     info!("Validator Connected - {}", pubkey);
    //
    //     let (subscription_sender, mut subscription_receiver) = unbounded_channel();
    //
    //     let router = self.router.clone();
    //     let connected =
    //         spawn_blocking(move || router.add_packet_subscription(&pubkey, subscription_sender))
    //             .await
    //             .map_err(|_| Status::internal("system error adding subscription"))?;
    //
    //     if !connected {
    //         return Err(Status::resource_exhausted("user already connected"));
    //     }
    //
    //     let (client_sender, client_receiver) = channel(1_000_000);
    //     tokio::spawn(async move {
    //         info!("validator connected [pubkey={:?}]", pubkey);
    //         loop {
    //             match subscription_receiver.recv().await {
    //                 Some(msg) => {
    //                     if let Err(e) = client_sender.send(msg).await {
    //                         debug!("client disconnected [err={}] [pk={}]", e, pubkey);
    //                         break;
    //                     }
    //                 }
    //                 None => {
    //                     debug!("unsubscribed [pk={}]", pubkey);
    //                     let _ = client_sender
    //                         .send(Err(Status::aborted("disconnected")))
    //                         .await;
    //                     break;
    //                 }
    //             }
    //         }
    //     });
    //
    //     Ok(Response::new(ValidatorSubscriberStream {
    //         inner: ReceiverStream::new(client_receiver),
    //         tx: self.client_disconnect_sender.clone(),
    //         client_pubkey: pubkey,
    //     }))
    // }
}
