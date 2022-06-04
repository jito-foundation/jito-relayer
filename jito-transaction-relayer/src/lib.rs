use log::*;
use std::{
    io,
    // iter::repeat,
    net::{IpAddr, /*Ipv4Addr,*/ SocketAddr, UdpSocket},
    pin::Pin,
    task::{Context, Poll},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::channel,
        Arc, Mutex,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant, }, //SystemTime},
};

use itertools::Itertools;
use bincode::serialize;
use solana_perf::{packet::PacketBatch, test_tx::test_tx};

use tokio::runtime::Builder as tokio_Builder;
use tokio::sync::mpsc::{channel as tokio_channel, unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle as tokio_JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use jito_protos::{
    shared::Header,
    packet::{Meta as PbMeta, Packet as PbPacket, PacketBatch as PbPacketBatch, PacketFlags as PbPacketFlags,
             PacketBatchWrapper, },
    relayer::{ValPacketSubRequest,
              // AoiPacketSubRequest,
              SubscribePacketsResponse,
              subscribe_packets_response::Msg,
              subscribe_packets_server::{SubscribePackets, SubscribePacketsServer},
              subscribe_packets_client::SubscribePacketsClient,
    },
};

const DEFAULT_BATCH_SIZE: usize = 128;

pub struct ValidatorSubscriberStream<T> {
    inner: ReceiverStream<Result<T, Status>>,
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

struct SubscribePacketsServiceImpl {
    verified_rx: Arc<Mutex<UnboundedReceiver<Vec<PacketBatch>>>>,
}

impl SubscribePacketsServiceImpl {
    pub fn new(mut verified_receiver: UnboundedReceiver<Vec<PacketBatch>>) -> Self {
        let verified_rx = Arc::new(Mutex::new(verified_receiver));
        Self {verified_rx}
    }

    // ///Given the max number of packets per batch and the max payload size of the messaging system,
    // ///determine the worst case max batches per messages
    // fn calculate_max_batches_per_msg(batch_size: usize, max_payload_size: usize) -> usize {
    //     // Build the worst case scenario for packets in a batch
    //     let packet_data = [0_u8; PACKET_DATA_SIZE].to_vec();
    //     let batch_list = PbPacketBatch {
    //         packets: (0..batch_size)
    //             .map(|_| PbPacket {
    //                 data: packet_data.clone(),
    //                 meta: Some(PbMeta {
    //                     size: packet_data.len() as u64,
    //                     addr: "255.255.255.255:65535".to_string(),
    //                     port: 0,
    //                     flags: Some(PbPacketFlags::default()),
    //                 }),
    //             })
    //             .collect(),
    //     };
    //
    //     // Determine overhead of protobuf serialization.
    //     let encoded_batch_list_size = Publisher::serialize(batch_list.clone()).len();
    //
    //     let ts = prost_types::Timestamp::from(SystemTime::now());
    //     let header = Some(Header { ts: Some(ts) });
    //     let encoded_packet_list_size = Publisher::serialize(PbPacketBatchList {
    //         header,
    //         batch_list: vec![batch_list],
    //     })
    //         .len();
    //
    //     let pb_packet_batch_list_overhead = encoded_packet_list_size - encoded_batch_list_size;
    //
    //     (max_payload_size - pb_packet_batch_list_overhead) / encoded_batch_list_size
    // }
}

#[tonic::async_trait]
impl SubscribePackets for SubscribePacketsServiceImpl {
    // type SubscribeAOIPacketsStream = ValidatorSubscriberStream<SubscribePacketsResponse>;
    type SubscribeValPacketsStream = ValidatorSubscriberStream<SubscribePacketsResponse>;

    // async fn subscribe_aoi_packets(
    //     &self,
    //     _: Request<AoiPacketSubRequest>,
    // ) -> Result<Response<Self::SubscribeAOIPacketsStream>, Status> {
    //     unimplemented!();
    // }

    async fn subscribe_val_packets(
        &self,
        _: Request<ValPacketSubRequest>,
    ) -> Result<Response<Self::SubscribeValPacketsStream>, Status> {

        let batch_size = DEFAULT_BATCH_SIZE;
        // let max_batches_per_msg = Self::calculate_max_batches_per_msg(batch_size, max_payload_size);
        let max_batches_per_msg = 1;  // ToDo: Fix this

        let (client_sender, client_receiver) = tokio_channel(1);
        // let (client_sender, client_receiver) = channel(1_000_000);
        tokio::spawn(async move {
            // info!("validator connected [pubkey={:?}]", pubkey);
            loop {
                let mut maybe_batch: Option<Vec<PacketBatch>>;
                {
                    let mut lock = self.verified_rx.lock().unwrap();
                    maybe_batch = lock.blocking_recv();
                }
                match maybe_batch {
                    None => {
                        error!("channel closed");
                        break;
                    }
                    Some(batch_list) => {
                        let start = Instant::now();
                        let mut batches = batch_list;
                        while let Ok(batch_list) = self.verified_rx.lock().unwrap().try_recv() {
                            batches.extend(batch_list);
                        }

                        let pb_packets = batches.into_iter().flat_map(|b| {
                            b.packets
                                .iter()
                                .filter_map(|p| {
                                    (!p.meta.discard()).then(|| PbPacket {
                                        data: p.data[0..p.meta.size].to_vec(),
                                        meta: Some(PbMeta {
                                            size: p.meta.size as u64,
                                            addr: p.meta.addr.to_string(),
                                            port: p.meta.port as u32,
                                            flags: Some(PbPacketFlags {
                                                discard: p.meta.discard(),
                                                forwarded: p.meta.forwarded(),
                                                repair: p.meta.repair(),
                                                simple_vote_tx: p.meta.is_simple_vote_tx(),
                                                tracer_tx: p.meta.is_tracer_tx(),
                                            }),
                                        }),
                                    })
                                })
                                .collect::<Vec<PbPacket>>()
                        });



                        // publish in batches + publish excess
                        let mut batches = Vec::with_capacity(max_batches_per_msg);
                        for chunk in &pb_packets.into_iter().chunks(batch_size) {
                            if batches.len() < max_batches_per_msg {
                                batches.push(PbPacketBatch {
                                    packets: chunk.collect(),
                                });
                            } else {
                                // if let Err(e) = Self::publish_batch_list(
                                //     &publisher,
                                //     header.clone(),
                                //     batches,
                                // ) {
                                //     error!("error publishing packet batch [error={}]", e);
                                // }
                                if let Ok(permit) = client_sender.reserve().await {
                                    let response = SubscribePacketsResponse {
                                        // header: Some(Header {
                                        //     ts: Some(prost_types::Timestamp::from(SystemTime::now())),
                                        // }),
                                        msg: Some(Msg::BatchList(PacketBatchWrapper {
                                            batch_list: batches.clone(),
                                        })),
                                    };
                                    permit.send(Ok(response));
                                } else {
                                    break;
                                }

                                batches = Vec::with_capacity(max_batches_per_msg);
                            }
                        }
                        if let Ok(permit) = client_sender.reserve().await {
                            let response = SubscribePacketsResponse {
                                // header: Some(Header {
                                //     ts: Some(prost_types::Timestamp::from(SystemTime::now())),
                                // }),
                                msg: Some(Msg::BatchList(PacketBatchWrapper {
                                    batch_list: batches.clone(),
                                })),
                            };
                            permit.send(Ok(response));
                        } else {
                            break;
                        }

                        // debug!("publish took {} ns", start.elapsed().as_nanos());
                    }

                }
            }
        });

        Ok(Response::new(ValidatorSubscriberStream {
            inner: ReceiverStream::new(client_receiver),
            // inner: ReceiverStream::new(self.verified_rx),
        }))
    }
}


pub fn publish_verified_grpc(mut verified_rx: UnboundedReceiver<Vec<PacketBatch>>,
                             exit: &Arc<AtomicBool>,
                             bind_ip: IpAddr)
    -> () {


}


pub fn test_fetch_verify(mut verified_rx: UnboundedReceiver<Vec<PacketBatch>>,
                         exit: &Arc<AtomicBool>,
                         tpu_ip: IpAddr,
                         tpu_port: u16)
    -> () {

    const NUM_THREADS: usize = 4;
    const NUM_PACKETS_PER_ITER: usize = 10_000;
    const PACKET_RATE_PER_SEC_PER_SERVER: usize = 500_000;
    const PACKET_RATE_PER_THREAD: usize =
        ((PACKET_RATE_PER_SEC_PER_SERVER as f32) / (NUM_THREADS as f32)) as usize;
    const LOOP_DURATION: f32 = (NUM_PACKETS_PER_ITER as f32) / (PACKET_RATE_PER_THREAD as f32);

    let (stats_sender, stats_receiver) = channel();
    let stats_sender = Arc::new(Mutex::new(stats_sender));
    let tpu_addr = SocketAddr::new(tpu_ip, tpu_port);
    let send_threads: Vec<JoinHandle<()>> = (0..NUM_THREADS)
        .map(|_| {
            let send_exit = exit.clone();
            let stats_sender = stats_sender.clone();
            Builder::new()
                .name("send_thread".into())
                .spawn(move || {
                    let udp_sender = UdpSocket::bind("0.0.0.0:0").unwrap();
                    loop {
                        let exec_start = Instant::now();
                        if send_exit.load(Ordering::Acquire) {
                            break;
                        }

                        // TODO (LB): need to generate this faster
                        let serialized_txs: Vec<Vec<u8>> = (0..NUM_PACKETS_PER_ITER)
                            .map(|_| serialize(&test_tx()).unwrap())
                            .collect();

                        let _: Vec<io::Result<usize>> = serialized_txs
                            .iter()
                            .map(|tx| udp_sender.send_to(tx, tpu_addr))
                            .collect();
                        if let Err(e) = stats_sender.lock().unwrap().send(serialized_txs.len()) {
                            error!("error sending stats: {}", e);
                        }

                        let sleep_duration = Duration::from_secs_f32(LOOP_DURATION)
                            .checked_sub(exec_start.elapsed())
                            .unwrap_or_else(|| Duration::from_secs(0));
                        sleep(sleep_duration);
                    }
                })
                .unwrap()
        })
        .collect();

    let stats_receiver = Arc::new(Mutex::new(stats_receiver));
    let stats_thread = Builder::new()
        .name("stats_thread".into())
        .spawn(move || {
            let mut count = 0;
            let mut prev_count = 0;
            let mut start = Instant::now();
            loop {
                match stats_receiver
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(1))
                {
                    Ok(txs_sent) => {
                        count += txs_sent;
                    }
                    Err(_) => {
                        break;
                    }
                }

                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(10) {
                    let diff = count - prev_count;
                    let packets_sent_per_second = ((diff as f64) / elapsed.as_secs_f64()) as f32;
                    println!("packets_sent_per_second={}", packets_sent_per_second);
                    prev_count = count;
                    start = Instant::now();
                }
            }
        })
        .unwrap();

    let recv_exit = exit.clone();
    let recv_thread = Builder::new()
        .name("verified_receiver".into())
        .spawn(move || {
            let mut prev_count: usize = 0;
            let mut count: usize = 0;
            let mut good_packet_count: usize = 0;
            let mut start = Instant::now();

            loop {
                if recv_exit.load(Ordering::Acquire) {
                    break;
                }

                if start.elapsed() > Duration::from_secs(10) {
                    let elapsed = start.elapsed().as_secs_f64();
                    let diff = count - prev_count;
                    let packets_verified_per_sec = ((diff as f64) / elapsed) as f32;
                    println!(
                        "packets_verified_per_second={}, good_packet_percent={:.2}%",
                        packets_verified_per_sec,
                        (good_packet_count as f32 / diff as f32) * 100.0
                    );
                    prev_count = count;
                    good_packet_count = 0;
                    start = Instant::now();
                }
                match verified_rx.blocking_recv() {
                    Some(verified_packets) => {
                        count += verified_packets
                            .iter()
                            .map(|batch| batch.packets.len())
                            .sum::<usize>();
                        good_packet_count += verified_packets
                            .iter()
                            .map(|batch| {
                                batch
                                    .packets
                                    .iter()
                                    .filter_map(|packet| (!packet.meta.discard()).then(|| 1))
                                    .sum::<usize>()
                            })
                            .sum::<usize>();
                    }
                    None => {
                        error!("recv error");
                    }
                }
            }
        })
        .unwrap();

    sleep(Duration::from_secs(30));

    exit.store(true, Ordering::Relaxed);

    for s in send_threads {
        let _ = s.join();
    }
    let _ = recv_thread.join();
    let _ = stats_thread.join();

}