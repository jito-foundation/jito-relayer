//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use crossbeam_channel::unbounded;
use solana_perf::packet::{PacketBatchRecycler, PACKETS_PER_BATCH};
use solana_perf::recycler::Recycler;
use solana_streamer::streamer;
use solana_streamer::streamer::StreamerReceiveStats;
use crate::find_packet_sender_stake_stage::FindPacketSenderStakeStage;

pub const DEFAULT_TPU_COALESCE_MS: u64 = 5;

/// Timeout interval when joining threads during TPU close
const TPU_THREADS_JOIN_TIMEOUT_SECONDS: u64 = 10;

// allow multiple connections for NAT and any open/close overlap
pub const MAX_QUIC_CONNECTIONS_PER_IP: usize = 8;

pub struct TpuSockets {
    pub transactions_sockets: Vec<UdpSocket>,
    pub transactions_forward_sockets: Vec<UdpSocket>,
    pub transactions_quic_sockets: UdpSocket,
}

pub struct Tpu {}

impl Tpu {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sockets: TpuSockets,
        exit: &Arc<AtomicBool>,
        coalesce_ms: u64,
    ) -> Self {
        let TpuSockets {
            transactions_sockets,
            transactions_forward_sockets,
            transactions_quic_sockets,
        } = sockets;

        // Fetch Stage
        let recycler: PacketBatchRecycler = Recycler::warmed(10_000, PACKETS_PER_BATCH);

        let tpu_sockets: Vec<_> = transactions_sockets.into_iter().map(Arc::new).collect();
        let tpu_forwards_sockets: Vec<_> = transactions_forward_sockets.into_iter().map(Arc::new).collect();

        let (tx_sender, tx_receiver) = unbounded();
        let (tx_fwd_sender, tx_fwd_receiver) = unbounded();

        let tpu_stats = Arc::new(StreamerReceiveStats::new("tpu_receiver"));
        let tpu_threads: Vec<_> = tpu_sockets
            .into_iter()
            .map(|socket| {
                streamer::receiver(
                    socket,
                    exit.clone(),
                    tx_sender.clone(),
                    recycler.clone(),
                    tpu_stats.clone(),
                    coalesce_ms,
                    true,
                    None,
                )
            })
            .collect();

        let tpu_fwd_stats = Arc::new(StreamerReceiveStats::new("tpu_fwd_receiver"));
        let tpu_fwd_threads: Vec<_> = tpu_forwards_sockets
            .into_iter()
            .map(|socket| {
                streamer::receiver(
                    socket,
                    exit.clone(),
                    tx_fwd_sender.clone(),
                    recycler.clone(),
                    tpu_stats.clone(),
                    coalesce_ms,
                    true,
                    None,
                )
            })
            .collect();

        let find_tpu_stake_stage = FindPacketSenderStakeStage::new();

        Tpu {}
    }

    pub fn join(&mut self) {

    }
}
