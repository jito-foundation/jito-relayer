use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crossbeam_channel::{select, Receiver};
use jito_protos::{
    packet::{
        Meta as PbMeta, Packet as PbPacket, PacketBatch as PbPacketBatch,
        PacketBatchWrapper as PbPacketBatchWrapper, PacketFlags as PbPacketFlags,
    },
    validator_interface_service::{
        subscribe_packets_response::Msg::{BatchList, Heartbeat},
        validator_interface_server::ValidatorInterface,
        GetTpuConfigsRequest, GetTpuConfigsResponse, SubscribeBundlesRequest,
        SubscribeBundlesResponse, SubscribePacketsRequest, SubscribePacketsResponse,
    },
};
use log::error;
use solana_core::banking_stage::BankingPacketBatch;
use solana_perf::packet::PacketBatch;
use solana_sdk::clock::Slot;
use tokio::{sync::mpsc::channel, time::sleep};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

pub struct Relayer {
    _slot_receiver: Receiver<Slot>,
    packet_receiver: Receiver<BankingPacketBatch>,
}

impl Relayer {
    pub fn new(
        _slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
    ) -> Relayer {
        Relayer {
            _slot_receiver,
            packet_receiver,
        }
    }
}

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

impl<T> Drop for ValidatorSubscriberStream<T> {
    fn drop(&mut self) {
        // ToDo: Need anything here?
    }
}

#[tonic::async_trait]
impl ValidatorInterface for Relayer {
    async fn get_tpu_configs(
        &self,
        _: Request<GetTpuConfigsRequest>,
    ) -> Result<Response<GetTpuConfigsResponse>, Status> {
        unimplemented!();
    }

    type SubscribeBundlesStream = ValidatorSubscriberStream<SubscribeBundlesResponse>;

    async fn subscribe_bundles(
        &self,
        _: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        unimplemented!();
    }

    type SubscribePacketsStream = ValidatorSubscriberStream<SubscribePacketsResponse>;

    async fn subscribe_packets(
        &self,
        _request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        println!("Validator Connected!!!!!!!!!");

        let (sender, receiver) = channel(100);
        let closed = Arc::new(AtomicBool::new(false));

        // Send Heartbeats
        let sender_l = sender.clone();
        let closed_l = closed.clone();
        tokio::spawn(async move {
            let mut hb_misses = 0u8;
            while !closed_l.load(Ordering::Relaxed) {
                if let Err(e) = sender_l
                    .send(Ok(SubscribePacketsResponse {
                        msg: Some(Heartbeat(true)),
                    }))
                    .await
                {
                    error!("subscribe_packets error sending heartbeat: {:?}", e);
                    hb_misses += 1;
                    if hb_misses > 3 {
                        error!("Too many failed heartbeat sends.  Disconnecting");
                        closed_l.store(true, Ordering::Relaxed);
                        break;
                    }
                } else {
                    hb_misses = 0;
                }
                sleep(Duration::from_millis(500)).await;
            }
        });

        // Send Packets
        // let sender_l = sender.clone();
        let pkt_receiver = self.packet_receiver.clone();
        let closed_l = closed.clone();
        tokio::spawn(async move {
            let sender_l = sender;
            while !closed_l.load(Ordering::Relaxed) {
                // Gonna leave this select for now, in case it's needed for slots later
                select! {
                    recv(pkt_receiver) -> bp_batch => {
                        match bp_batch {
                            Ok(bp_batch) => {
                                let batches = bp_batch.0;
                                // println!("Got Batch of length {}", batches.len());
                                let proto_bl = Self::sol_batchlist_to_proto(batches);

                                // Send over Grpc
                                if let Err(_e) = sender_l
                                    .send_timeout(
                                        Ok(SubscribePacketsResponse {
                                            msg: Some(BatchList(proto_bl)),
                                        }),
                                        Duration::from_millis(1000),
                                    )
                                    .await
                                {
                                    // error!("subscribe_packets error sending response: {:?}", e);
                                    error!("subscribe_packets error sending response!!");
                                }
                            }
                            Err(e) => {
                                error!("packets_receiver channel closed {}", e);
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Response::new(ValidatorSubscriberStream {
            inner: ReceiverStream::new(receiver),
        }))
    }
}

impl Relayer {
    fn sol_batchlist_to_proto(batches: Vec<PacketBatch>) -> PbPacketBatchWrapper {
        println!("sending packet batch!!");
        // ToDo: Turn this back into a map
        let mut proto_batch_vec: Vec<PbPacketBatch> = Vec::new();
        for batch in batches.into_iter() {
            let mut proto_pkt_vec: Vec<PbPacket> = Vec::new();
            for p in batch.iter() {
                if !p.meta.discard() {
                    proto_pkt_vec.push(PbPacket {
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
                                // tracer_tx: p.meta.is_tracer_tx(),  // Couldn't get this to work?
                                tracer_tx: false,
                            }),
                        }),
                    })
                }
            }
            proto_batch_vec.push(PbPacketBatch {
                packets: proto_pkt_vec,
            })
        }

        PbPacketBatchWrapper {
            // ToDo: Perf - Clone here?
            batch_list: proto_batch_vec.clone(),
        }
    }
}
