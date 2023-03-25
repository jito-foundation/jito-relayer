use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{Builder, JoinHandle},
    time::{Duration, Instant, SystemTime},
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use jito_block_engine::block_engine::BlockEnginePackets;
use jito_relayer::relayer::RouterPacketBatches;
use solana_core::banking_stage::BankingPacketBatch;
use solana_metrics::datapoint_info;
use solana_perf::packet::PacketBatch;
use tokio::sync::mpsc::error::TrySendError;

/// Forwards packets to the Block Engine handler thread.
/// Delays transactions for packet_delay_ms before forwarding them to the validator.
#[allow(clippy::too_many_arguments)]
pub fn start_forward_and_delay_thread(
    packet_receiver: Receiver<BankingPacketBatch>,
    delay_sender: Sender<RouterPacketBatches>,
    packet_delay_ms: u32,
    block_engine_sender: tokio::sync::mpsc::Sender<BlockEnginePackets>,
    num_threads: u64,
    exit: &Arc<AtomicBool>,
    cluster: String,
    region: String,
) -> Vec<JoinHandle<()>> {
    const SLEEP_DURATION: Duration = Duration::from_millis(5);
    let packet_delay = Duration::from_millis(packet_delay_ms as u64);

    (0..num_threads)
        .map(|thread_id| {
            let packet_receiver = packet_receiver.clone();
            let delay_sender = delay_sender.clone();
            let block_engine_sender = block_engine_sender.clone();

            let cluster = cluster.clone();
            let region = region.clone();
            let exit = exit.clone();
            Builder::new()
                .name(format!("forwarder_thread_{thread_id}"))
                .spawn(move || {
                    let metrics_interval = Duration::from_secs(1);
                    let mut forwarder_metrics = ForwarderMetrics::default();
                    let mut last_metrics_upload = Instant::now();

                    let mut buffered_packet_batches = VecDeque::with_capacity(100_000);

                    while !exit.load(Ordering::Relaxed) {
                        if last_metrics_upload.elapsed() >= metrics_interval {
                            forwarder_metrics.report(thread_id, packet_delay_ms, &cluster, &region);

                            forwarder_metrics = ForwarderMetrics::default();
                            last_metrics_upload = Instant::now();
                        }

                        match packet_receiver.recv_timeout(SLEEP_DURATION) {
                            Ok(banking_packet_batch) => {
                                let mut packet_batches = banking_packet_batch.0;
                                while let Ok((batches, _)) = packet_receiver.try_recv() {
                                    packet_batches.extend(batches.into_iter());
                                }

                                let total_packets: u64 =
                                    packet_batches.iter().map(|b| b.len() as u64).sum();

                                let packet_batches: Vec<PacketBatch> = packet_batches
                                    .into_iter()
                                    .map(|b| {
                                        PacketBatch::new(
                                            b.iter()
                                                .filter(|p| !p.meta.discard())
                                                .cloned()
                                                .collect(),
                                        )
                                    })
                                    .collect();

                                let instant = Instant::now();
                                let system_time = SystemTime::now();

                                let num_packets_received =
                                    packet_batches.iter().map(|b| b.len() as u64).sum::<u64>();
                                let num_batches_received = packet_batches.len() as u64;

                                forwarder_metrics.num_packets_received += total_packets;
                                forwarder_metrics.num_packets_filtered +=
                                    total_packets - num_packets_received;
                                forwarder_metrics.num_batches_received += num_batches_received;

                                // try_send because the block engine receiver only drains when it's connected
                                // and we don't want to OOM on packet_receiver
                                match block_engine_sender.try_send(BlockEnginePackets {
                                    packet_batches: packet_batches.clone(),
                                    stamp: system_time,
                                    expiration: packet_delay_ms,
                                }) {
                                    Ok(_) => {
                                        forwarder_metrics.num_be_packets_forwarded +=
                                            num_packets_received;
                                    }
                                    Err(TrySendError::Closed(_)) => {
                                        panic!(
                                            "error sending packet batch to block engine handler"
                                        );
                                    }
                                    Err(TrySendError::Full(_)) => {
                                        // block engine most likely not connected
                                        forwarder_metrics.num_be_packets_dropped +=
                                            num_packets_received;
                                        forwarder_metrics.num_be_sender_full += 1;
                                    }
                                }
                                buffered_packet_batches.push_back(RouterPacketBatches {
                                    stamp: instant,
                                    batches: packet_batches,
                                });
                            }
                            Err(RecvTimeoutError::Timeout) => {}
                            Err(RecvTimeoutError::Disconnected) => {
                                panic!("packet receiver disconnected");
                            }
                        }

                        while let Some(packet_batches) = buffered_packet_batches.front() {
                            if packet_batches.stamp.elapsed() < packet_delay {
                                break;
                            }
                            let batch = buffered_packet_batches.pop_front().unwrap();

                            let num_packets =
                                batch.batches.iter().map(|b| b.len() as u64).sum::<u64>();
                            forwarder_metrics.num_relayer_packets_forwarded += num_packets;

                            delay_sender
                                .send(batch)
                                .expect("exiting forwarding delayed packets");
                        }

                        forwarder_metrics.update(
                            buffered_packet_batches.len(),
                            buffered_packet_batches.capacity(),
                        );
                    }
                })
                .unwrap()
        })
        .collect()
}

#[derive(Default)]
struct ForwarderMetrics {
    pub num_batches_received: u64,
    pub num_packets_received: u64,
    pub num_packets_filtered: u64,

    pub num_be_packets_forwarded: u64,
    pub num_be_packets_dropped: u64,
    pub num_be_sender_full: u64,

    pub num_relayer_packets_forwarded: u64,

    pub buffered_packet_batches_max_len: usize,
    pub buffered_packet_batches_max_capacity: usize,
}

impl ForwarderMetrics {
    pub fn update(
        &mut self,
        buffered_packet_batches_len: usize,
        buffered_packet_batches_capacity: usize,
    ) {
        self.buffered_packet_batches_max_len = std::cmp::max(
            self.buffered_packet_batches_max_len,
            buffered_packet_batches_len,
        );
        self.buffered_packet_batches_max_capacity = std::cmp::max(
            self.buffered_packet_batches_max_capacity,
            buffered_packet_batches_capacity,
        );
    }

    pub fn report(&self, thread_id: u64, delay: u32, cluster: &str, region: &str) {
        datapoint_info!(
            "forwarder_metrics",
            "cluster" => cluster,
            "region" => region,
            ("thread_id", thread_id, i64),
            ("delay", delay, i64),
            ("num_batches_received", self.num_batches_received, i64),
            ("num_packets_received", self.num_packets_received, i64),
            ("num_packets_filtered", self.num_packets_filtered, i64),
            // Relayer -> Block Engine Metrics
            (
                "num_be_packets_forwarded",
                self.num_be_packets_forwarded,
                i64
            ),
            ("num_be_packets_dropped", self.num_be_packets_dropped, i64),
            ("num_be_sender_full", self.num_be_sender_full, i64),
            // Relayer -> validator metrics
            (
                "num_relayer_packets_forwarded",
                self.num_relayer_packets_forwarded,
                i64
            ),
            // Channel stats
            (
                "buffered_packet_batches-len",
                self.buffered_packet_batches_max_len,
                i64
            ),
            (
                "buffered_packet_batches-capacity",
                self.buffered_packet_batches_max_capacity,
                i64
            ),
        );
    }
}
