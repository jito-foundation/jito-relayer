//! The `fetch_stage` batches input from a UDP socket and sends it to a channel.

use std::{
    net::UdpSocket,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, sleep, Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::{RecvError, RecvTimeoutError};
use solana_perf::{packet::PacketBatchRecycler, recycler::Recycler};
use solana_sdk::packet::{Packet, PacketFlags};
use solana_streamer::streamer::{
    self, PacketBatchReceiver, PacketBatchSender, StreamerReceiveStats,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("send error")]
    Send,
    #[error("recv timeout")]
    RecvTimeout(#[from] RecvTimeoutError),
    #[error("recv error")]
    Recv(#[from] RecvError),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct FetchStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl FetchStage {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_sender(
        sockets: Vec<UdpSocket>,
        tpu_forwards_sockets: Vec<UdpSocket>,
        exit: &Arc<AtomicBool>,
        sender: &PacketBatchSender,
        forward_sender: &PacketBatchSender,
        forward_receiver: PacketBatchReceiver,
        coalesce_ms: u64,
        in_vote_only_mode: Option<Arc<AtomicBool>>,
    ) -> Self {
        let tx_sockets = sockets.into_iter().map(Arc::new).collect();
        let tpu_forwards_sockets = tpu_forwards_sockets.into_iter().map(Arc::new).collect();
        Self::new_multi_socket(
            tx_sockets,
            tpu_forwards_sockets,
            exit,
            sender,
            forward_sender,
            forward_receiver,
            coalesce_ms,
            in_vote_only_mode,
        )
    }

    fn handle_forwarded_packets(
        recvr: &PacketBatchReceiver,
        sendr: &PacketBatchSender,
    ) -> Result<()> {
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
            #[allow(clippy::question_mark)]
            if sendr.send(packet_batch).is_err() {
                return Err(Error::Send);
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn new_multi_socket(
        tpu_sockets: Vec<Arc<UdpSocket>>,
        tpu_forwards_sockets: Vec<Arc<UdpSocket>>,
        exit: &Arc<AtomicBool>,
        sender: &PacketBatchSender,
        forward_sender: &PacketBatchSender,
        forward_receiver: PacketBatchReceiver,
        coalesce_ms: u64,
        in_vote_only_mode: Option<Arc<AtomicBool>>,
    ) -> Self {
        let recycler: PacketBatchRecycler = Recycler::warmed(1000, 1024);

        let tpu_stats = Arc::new(StreamerReceiveStats::new("tpu_receiver"));
        let tpu_threads: Vec<_> = tpu_sockets
            .into_iter()
            .map(|socket| {
                streamer::receiver(
                    socket,
                    exit.clone(),
                    sender.clone(),
                    recycler.clone(),
                    tpu_stats.clone(),
                    coalesce_ms,
                    true,
                    in_vote_only_mode.clone(),
                )
            })
            .collect();

        let tpu_forward_stats = Arc::new(StreamerReceiveStats::new("tpu_forwards_receiver"));
        let tpu_forwards_threads: Vec<_> = tpu_forwards_sockets
            .into_iter()
            .map(|socket| {
                streamer::receiver(
                    socket,
                    exit.clone(),
                    forward_sender.clone(),
                    recycler.clone(),
                    tpu_forward_stats.clone(),
                    coalesce_ms,
                    true,
                    in_vote_only_mode.clone(),
                )
            })
            .collect();

        let sender = sender.clone();

        let exit_l = exit.clone();
        let fwd_thread_hdl = Builder::new()
            .name("solana-fetch-stage-fwd-rcvr".to_string())
            .spawn(move || loop {
                if let Err(e) = Self::handle_forwarded_packets(&forward_receiver, &sender) {
                    match e {
                        Error::RecvTimeout(RecvTimeoutError::Disconnected) => break,
                        Error::RecvTimeout(RecvTimeoutError::Timeout) => (),
                        Error::Recv(_) => break,
                        Error::Send => break,
                    }
                }

                if exit_l.load(Ordering::Relaxed) {
                    return;
                }
            })
            .unwrap();

        let exit_l = exit.clone();
        let metrics_thread_hdl = Builder::new()
            .name("solana-fetch-stage-metrics".to_string())
            .spawn(move || loop {
                sleep(Duration::from_secs(1));

                tpu_stats.report();
                tpu_forward_stats.report();

                if exit_l.load(Ordering::Relaxed) {
                    return;
                }
            })
            .unwrap();

        Self {
            thread_hdls: [
                tpu_threads,
                tpu_forwards_threads,
                vec![fwd_thread_hdl, metrics_thread_hdl],
            ]
            .into_iter()
            .flatten()
            .collect(),
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}
