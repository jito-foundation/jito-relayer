//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

use std::{
    net::{IpAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::{sleep, Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::{unbounded, Receiver};
use jito_rpc::load_balancer::LoadBalancer;
use solana_core::{
    banking_stage::BankingPacketBatch, find_packet_sender_stake_stage::FindPacketSenderStakeStage,
    sigverify::TransactionSigVerifier, sigverify_stage::SigVerifyStage,
};
use solana_metrics::datapoint_info;
use solana_perf::packet::PacketBatch;
use solana_sdk::signature::Keypair;
use solana_streamer::{
    quic::{spawn_server, StreamStats, MAX_STAKED_CONNECTIONS, MAX_UNSTAKED_CONNECTIONS},
    streamer::StakedNodes,
};

use crate::{fetch_stage::FetchStage, staked_nodes_updater_service::StakedNodesUpdaterService};

pub const DEFAULT_TPU_COALESCE_MS: u64 = 5;

// allow multiple connections for NAT and any open/close overlap
pub const MAX_QUIC_CONNECTIONS_PER_IP: usize = 8;

#[derive(Debug)]
pub struct TpuSockets {
    pub transactions_quic_sockets: UdpSocket,
    pub transactions_forwards_quic_sockets: UdpSocket,
}

pub struct Tpu {
    fetch_stage: FetchStage,
    staked_nodes_updater_service: StakedNodesUpdaterService,
    find_packet_sender_stake_stage: FindPacketSenderStakeStage,
    sigverify_stage: SigVerifyStage,
    thread_handles: Vec<JoinHandle<()>>,
}

impl Tpu {
    pub fn new(
        sockets: TpuSockets,
        exit: &Arc<AtomicBool>,
        keypair: &Keypair,
        tpu_ip: &IpAddr,
        tpu_fwd_ip: &IpAddr,
        rpc_load_balancer: &Arc<LoadBalancer>,
    ) -> (Self, Receiver<BankingPacketBatch>) {
        let TpuSockets {
            transactions_quic_sockets,
            transactions_forwards_quic_sockets,
        } = sockets;

        let staked_nodes = Arc::new(RwLock::new(StakedNodes::default()));
        let staked_nodes_updater_service = StakedNodesUpdaterService::new(
            exit.clone(),
            rpc_load_balancer.clone(),
            staked_nodes.clone(),
        );

        // sender tracked as fetch_stage-channel_stats.tpu_sender-len
        let (tpu_sender, tpu_receiver) = unbounded();

        // receiver tracked as fetch_stage-channel_stats.tpu_forwards_receiver-len
        let (tpu_forwards_sender, tpu_forwards_receiver) = unbounded();
        let stats = Arc::new(StreamStats::default());

        let tpu_quic_t = spawn_server(
            transactions_quic_sockets,
            keypair,
            *tpu_ip,
            tpu_sender.clone(),
            exit.clone(),
            MAX_QUIC_CONNECTIONS_PER_IP,
            staked_nodes.clone(),
            MAX_STAKED_CONNECTIONS,
            MAX_UNSTAKED_CONNECTIONS,
            stats.clone(),
        )
        .unwrap();

        let tpu_forwards_quic_t = spawn_server(
            transactions_forwards_quic_sockets,
            keypair,
            *tpu_fwd_ip,
            tpu_forwards_sender,
            exit.clone(),
            MAX_QUIC_CONNECTIONS_PER_IP,
            staked_nodes.clone(),
            MAX_STAKED_CONNECTIONS.saturating_add(MAX_UNSTAKED_CONNECTIONS),
            0, // Prevent unstaked nodes from forwarding transactions
            stats,
        )
        .unwrap();

        let fetch_stage = FetchStage::new(tpu_forwards_receiver, tpu_sender, exit.clone());

        // receiver tracked in tpu-channel_stats.find_packet_sender_stake_receiver-len
        let (find_packet_sender_stake_sender, find_packet_sender_stake_receiver) = unbounded();
        let find_packet_sender_stake_stage = FindPacketSenderStakeStage::new(
            tpu_receiver,
            find_packet_sender_stake_sender,
            staked_nodes,
            "tpu_find_packet_sender_stake-stats",
        );

        let metrics_t =
            Self::start_metrics_thread(exit.clone(), find_packet_sender_stake_receiver.clone());

        // receiver tracked as forwarder_metrics.verified_receiver-len
        let (verified_sender, verified_receiver) = unbounded();
        let sigverify_stage = {
            let verifier = TransactionSigVerifier::new(verified_sender);
            SigVerifyStage::new(find_packet_sender_stake_receiver, verifier, "tpu-verifier")
        };

        (
            Tpu {
                fetch_stage,
                staked_nodes_updater_service,
                find_packet_sender_stake_stage,
                sigverify_stage,
                thread_handles: vec![tpu_quic_t, tpu_forwards_quic_t, metrics_t],
            },
            verified_receiver,
        )
    }

    // channel consumed by solana code, receiver tracked in tpu-channel_stats.find_packet_sender_stake_receiver-len
    fn start_metrics_thread(
        exit: Arc<AtomicBool>,
        find_packet_sender_stake_receiver: Receiver<Vec<PacketBatch>>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("tpu_metrics_thread".to_string())
            .spawn(move || {
                let metrics_interval = Duration::from_secs(1);
                while !exit.load(Ordering::Relaxed) {
                    datapoint_info!(
                        "tpu-channel_stats",
                        (
                            "find_packet_sender_stake_receiver-len",
                            find_packet_sender_stake_receiver.len(),
                            i64
                        ),
                    );
                    sleep(metrics_interval);
                }
            })
            .unwrap()
    }

    pub fn join(self) -> thread::Result<()> {
        self.fetch_stage.join()?;
        self.staked_nodes_updater_service.join()?;
        self.find_packet_sender_stake_stage.join()?;
        self.sigverify_stage.join()?;
        for t in self.thread_handles {
            t.join()?
        }
        Ok(())
    }
}
