use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{sleep, spawn, JoinHandle},
    time::{Duration, SystemTime},
};

use bitvec::prelude::*;
use crossbeam_channel::{unbounded, Receiver, Sender};
use jito_protos::{
    packet::{
        Meta as PbMeta, Packet as PbPacket, PacketBatch as PbPacketBatch,
        PacketBatchList as PbPacketBatchList, PacketFlags as PbPacketFlags,
    },
    shared::Header as PbHeader,
};
use log::*;
use prost_types::Timestamp as ProstTimestamp;
use solana_core::banking_stage::BankingPacketBatch;
use solana_perf::packet::PacketBatch;
use solana_sdk::clock::Slot;

use crate::{block_engine::BlockEngine, router::Router, schedule_cache::LeaderScheduleCache};

pub struct Relayer {
    pub router: Arc<RwLock<Router>>,
    thread_handles: Vec<JoinHandle<()>>,
}

impl Relayer {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        exit: Arc<AtomicBool>,
        public_ip: IpAddr,
        tpu_port: u16,
        tpu_fwd_port: u16,
        block_eng_addr: SocketAddr,
    ) -> Self {
        // Create Router to send packets to Validators on schedule
        let router = Arc::new(RwLock::new(Router::new(
            leader_schedule_cache,
            slot_receiver,
            3,
            0,
            public_ip,
            tpu_port,
            tpu_fwd_port,
            exit.clone(),
        )));

        // Create Block Engine to send interested packets to block engine
        let block_engine = Arc::new(BlockEngine::new(block_eng_addr));

        // **** Event Loops ****
        // Spawn Loops and save handles for joining
        let mut thread_handles = Vec::new();

        //  Router Stream
        let (router_sender, router_receiver) = unbounded();
        Self::start_router_stream_loop(router.clone(), router_receiver, exit.clone());

        // Block Engine Stream
        let (block_engine_sender, block_engine_receiver) = unbounded();
        Self::start_engine_stream_loop(block_engine.clone(), block_engine_receiver, exit.clone());

        // Incoming Packet Processing Loop
        let packet_loop_hdl = Self::start_packets_receiver_loop(
            packet_receiver,
            router_sender,
            block_engine,
            block_engine_sender,
            exit,
        );
        thread_handles.push(packet_loop_hdl);

        Self {
            router,
            thread_handles,
        }
    }

    pub fn join(self) {
        for hdl in self.thread_handles {
            hdl.join().expect("task panicked");
        }
        // ToDo (JL): This seems overkill, but it's the only thing I could get to compile
        let router_threads = self.router.write().unwrap().thread_handles.take().unwrap();
        for hdl in router_threads {
            hdl.join().expect("task panicked");
        }
    }

    /// Receive packet batches via Receiver and stream them out over grpc to nodes.
    #[allow(clippy::too_many_arguments)]
    fn start_packets_receiver_loop(
        packet_receiver: Receiver<BankingPacketBatch>,
        router_sender: Sender<PbPacketBatchList>,
        block_engine: Arc<BlockEngine>,
        block_engine_sender: Sender<(PbPacketBatchList, BitVec)>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        // Spawn Packet Receiver Loop
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                match packet_receiver.recv() {
                    // Get Batch List out of PacketBatch
                    Ok(bp_batch) => {
                        let batches = bp_batch.0;
                        if !batches.is_empty() {
                            debug!(
                                "Got Batch of length {} x {}",
                                batches.len(),
                                batches[0].len()
                            );
                        }

                        // Put in protobuff and add timestamp, delta
                        let (proto_bl, packet_mask) =
                            Self::batchlist_to_proto(batches, &block_engine);

                        router_sender
                            .send(proto_bl.clone())
                            .expect("Couldn't Send PacketBatch to Router");

                        block_engine_sender
                            .send((proto_bl, packet_mask))
                            .expect("Couldn't send PacketBatch to Block Engine");
                    }
                    Err(_) => {
                        error!("error receiving packets");
                        break;
                    }
                }
            }
        })
    }

    fn start_router_stream_loop(
        router: Arc<RwLock<Router>>,
        packet_receiver: Receiver<PbPacketBatchList>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                if let Ok(proto_bl) = packet_receiver.recv() {
                    let ts_now = ProstTimestamp::from(SystemTime::now());
                    let ts_packet = proto_bl.header.clone().unwrap().ts.unwrap() as ProstTimestamp;
                    let ts_diff_millis = (ts_now.seconds - ts_packet.seconds) * 1000
                        + (ts_now.nanos as i64 - ts_packet.nanos as i64) / 1000000;

                    if ts_diff_millis < proto_bl.expiry as i64 {
                        sleep(Duration::from_millis(
                            ts_diff_millis as u64 - proto_bl.expiry as u64,
                        ))
                    }

                    let (failed_stream_pks, _slots_sent) =
                        router.read().unwrap().stream_batch_list(&proto_bl);

                    // close failed connections
                    Router::disconnect(&router.read().unwrap().packet_subs, &failed_stream_pks);
                } // else {}  ToDo (JL): Handle Packet packet receiver errors
            }
        })
    }

    fn start_engine_stream_loop(
        block_engine: Arc<BlockEngine>,
        packet_receiver: Receiver<(PbPacketBatchList, BitVec)>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                if let Ok(batchlist_with_mask) = packet_receiver.recv() {
                    block_engine.stream_aoi_batch_list(&batchlist_with_mask);
                }
            } // else {}  ToDo (JL): Handle Packet packet receiver errors
        })
    }

    fn batchlist_to_proto(
        batches: Vec<PacketBatch>,
        block_engine: &BlockEngine,
    ) -> (PbPacketBatchList, BitVec) {
        // ToDo (JL): Turn this back into a map
        let mut proto_batch_vec: Vec<PbPacketBatch> = Vec::new();

        let n_packets: usize = batches.iter().map(|b| b.len()).sum();
        let mut aoi_mask = BitVec::with_capacity(n_packets);
        let mut packet_i = 0usize;

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
                                tracer_packet: p.meta.is_tracer_packet(),
                            }),
                            sender_stake: p.meta.sender_stake,
                        }),
                    });
                    // Todo: Implement aoi filter HERE
                    let _aoi = block_engine.aoi.read().unwrap();
                    // aoi_mask.set(packet_i, true);
                    packet_i += 1;
                }
            }
            proto_batch_vec.push(PbPacketBatch {
                packets: proto_pkt_vec,
            })
        }

        let ts = ProstTimestamp::from(SystemTime::now());
        let header = Some(PbHeader { ts: Some(ts) });
        (
            PbPacketBatchList {
                header,
                batch_list: proto_batch_vec,
                expiry: *block_engine.delta.read().unwrap(),
            },
            aoi_mask,
        )
    }
}
