//! The `fetch_stage` batches input from a UDP socket and sends it to a channel.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, Builder, JoinHandle},
};

use crossbeam_channel::{RecvError, RecvTimeoutError};
use solana_sdk::packet::{Packet, PacketFlags};
use solana_streamer::streamer::{PacketBatchReceiver, PacketBatchSender};

#[derive(Debug, thiserror::Error)]
pub enum FetchStageError {
    #[error("send error")]
    Send,
    #[error("recv timeout")]
    RecvTimeout(#[from] RecvTimeoutError),
    #[error("recv error")]
    Recv(#[from] RecvError),
}

pub type FetchStageResult<T> = Result<T, FetchStageError>;

pub struct FetchStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl FetchStage {
    pub fn new_with_sender(
        forward_sender: &PacketBatchSender,
        forward_receiver: PacketBatchReceiver,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let sender = forward_sender.clone();
        let fwd_thread_hdl = Builder::new()
            .name("solana-fetch-stage-fwd-rcvr".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    match Self::handle_forwarded_packets(&forward_receiver, &sender) {
                        Ok(_) | Err(FetchStageError::RecvTimeout(RecvTimeoutError::Timeout)) => (),
                        Err(FetchStageError::RecvTimeout(RecvTimeoutError::Disconnected)) => break,
                        Err(FetchStageError::Recv(_)) => break,
                        Err(FetchStageError::Send) => break,
                    }
                }
            })
            .unwrap();

        Self {
            thread_hdls: vec![fwd_thread_hdl],
        }
    }

    fn handle_forwarded_packets(
        recvr: &PacketBatchReceiver,
        sendr: &PacketBatchSender,
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
            if sendr.send(packet_batch).is_err() {
                return Err(FetchStageError::Send);
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
