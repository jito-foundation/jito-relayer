//! The `fetch_stage` batches input from a UDP socket and sends it to a channel.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, Builder, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{RecvError, RecvTimeoutError, SendError};
use solana_metrics::{datapoint_error, datapoint_info};
use solana_perf::packet::PacketBatch;
use solana_sdk::packet::{Packet, PacketFlags};
use solana_streamer::streamer::{PacketBatchReceiver, PacketBatchSender};

#[derive(Debug, thiserror::Error)]
pub enum FetchStageError {
    #[error("send error: {0}")]
    Send(#[from] SendError<PacketBatch>),
    #[error("recv timeout: {0}")]
    RecvTimeout(#[from] RecvTimeoutError),
    #[error("recv error: {0}")]
    Recv(#[from] RecvError),
}

pub type FetchStageResult<T> = Result<T, FetchStageError>;

pub struct FetchStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl FetchStage {
    pub fn new(
        tpu_forwards_receiver: PacketBatchReceiver,
        tpu_sender: PacketBatchSender,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let fwd_thread_hdl = Builder::new()
            .name("fetch_stage-forwarder_thread".to_string())
            .spawn(move || {
                let metrics_interval = Duration::from_secs(1);
                let mut start = Instant::now();
                let mut tpu_forwards_receiver_max_len = 0usize;
                let mut tpu_sender_max_len = 0usize;
                while !exit.load(Ordering::Relaxed) {
                    match Self::handle_forwarded_packets(&tpu_forwards_receiver, &tpu_sender) {
                        Ok(()) | Err(FetchStageError::RecvTimeout(RecvTimeoutError::Timeout)) => {}
                        Err(e) => {
                            datapoint_error!(
                                "fetch_stage-handle_forwarded_packets_error",
                                ("error", e.to_string(), String)
                            );
                            panic!("Failed to handle forwarded packets. Error: {e}")
                        }
                    };

                    if start.elapsed() >= metrics_interval {
                        datapoint_info!(
                            "fetch_stage-channel_stats",
                            (
                                "tpu_forwards_receiver-len",
                                tpu_forwards_receiver_max_len,
                                i64
                            ),
                            ("tpu_sender-len", tpu_sender_max_len, i64),
                        );
                        start = Instant::now();
                        tpu_forwards_receiver_max_len = 0;
                        tpu_sender_max_len = 0;
                    }
                    tpu_forwards_receiver_max_len =
                        std::cmp::max(tpu_forwards_receiver_max_len, tpu_forwards_receiver.len());
                    tpu_sender_max_len = std::cmp::max(tpu_sender_max_len, tpu_sender.len());
                }
            })
            .unwrap();

        Self {
            thread_hdls: vec![fwd_thread_hdl],
        }
    }

    /// Copies tpu_forward packets to tpu channel. marks as forwarded
    fn handle_forwarded_packets(
        tpu_forwards_receiver: &PacketBatchReceiver,
        tpu_sender: &PacketBatchSender,
    ) -> FetchStageResult<()> {
        let mark_forwarded = |packet: &mut Packet| {
            packet.meta.flags |= PacketFlags::FORWARDED;
        };

        let mut packet_batch = tpu_forwards_receiver.recv()?;
        let mut num_packets = packet_batch.len();
        packet_batch.iter_mut().for_each(mark_forwarded);

        let mut packet_batches = vec![packet_batch];
        while let Ok(mut packet_batch) = tpu_forwards_receiver.try_recv() {
            num_packets += packet_batch.len();
            packet_batch.iter_mut().for_each(mark_forwarded);
            packet_batches.push(packet_batch);
            // Read at most 1K transactions in a loop
            if num_packets > 1024 {
                break;
            }
        }

        for packet_batch in packet_batches {
            if let Err(e) = tpu_sender.send(packet_batch) {
                return Err(FetchStageError::Send(e));
            }
        }

        Ok(())
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}
