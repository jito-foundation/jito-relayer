//! The `fetch_stage` batches input from a UDP socket and sends it to a channel.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, Builder, JoinHandle},
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
    #[error("recv timeout")]
    RecvTimeout(#[from] RecvTimeoutError),
    #[error("recv error: {0}")]
    Recv(#[from] RecvError),
}

pub type FetchStageResult<T> = Result<T, FetchStageError>;

pub struct FetchStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl FetchStage {
    const REPORT_INTERVAL: usize = 100;
    pub fn new_with_sender(
        forward_sender: &PacketBatchSender,
        forward_receiver: PacketBatchReceiver,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let sender = forward_sender.clone();
        let fwd_thread_hdl = Builder::new()
            .name("solana-fetch-stage-fwd-rcvr".to_string())
            .spawn(move || {
                let mut iter_count = 0usize;
                while !exit.load(Ordering::Relaxed) {
                    match Self::handle_forwarded_packets(&forward_receiver, &sender) {
                        Ok(()) | Err(FetchStageError::RecvTimeout(RecvTimeoutError::Timeout)) => {
                            if iter_count % FetchStage::REPORT_INTERVAL == 0 {
                                datapoint_info!(
                                    "fetch_stage-handle_forwarded_packets",
                                    ("sender_queue_len", sender.len(), i64)
                                );
                            }
                        }
                        Err(e) => {
                            datapoint_error!(
                                "fetch_stage-handle_forwarded_packets_error",
                                ("error", e.to_string(), String)
                            );
                            panic!("Failed to handle forwarded packets. Error: {e}")
                        }
                    };
                    iter_count += 1;
                }
            })
            .unwrap();

        Self {
            thread_hdls: vec![fwd_thread_hdl],
        }
    }

    fn handle_forwarded_packets(
        recvr: &PacketBatchReceiver, /* incoming */
        sendr: &PacketBatchSender,   /* outgoing */
    ) -> FetchStageResult<()> {
        let mark_forwarded = |packet: &mut Packet| {
            packet.meta.flags |= PacketFlags::FORWARDED;
        };

        let mut packet_batch = recvr.recv()?;
        let mut num_packets = packet_batch.len();
        packet_batch.iter_mut().for_each(mark_forwarded);
        let mut packet_batches = vec![packet_batch];
        while let Ok(mut packet_batch) = recvr.try_recv() {
            packet_batch.iter_mut().for_each(mark_forwarded);
            num_packets += packet_batch.len();
            packet_batches.push(packet_batch);
            // Read at most 1K transactions in a loop
            if num_packets > 1024 {
                break;
            }
        }

        for packet_batch in packet_batches {
            if let Err(e) = sendr.send(packet_batch) {
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
