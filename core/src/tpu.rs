//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

use crate::fetch_stage::FetchStage;
use crate::staked_nodes_updater_service::StakedNodesUpdaterService;
use crossbeam_channel::{unbounded, Receiver};
use solana_core::banking_stage::BankingPacketBatch;
use solana_core::find_packet_sender_stake_stage::FindPacketSenderStakeStage;
use solana_core::sigverify::TransactionSigVerifier;
use solana_core::sigverify_stage::SigVerifyStage;
use solana_sdk::signature::Keypair;
use solana_streamer::quic::{
    spawn_server, StreamStats, MAX_STAKED_CONNECTIONS, MAX_UNSTAKED_CONNECTIONS,
};
use std::collections::HashMap;
use std::net::{IpAddr, UdpSocket};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::thread;

pub const DEFAULT_TPU_COALESCE_MS: u64 = 5;

/// Timeout interval when joining threads during TPU close
const TPU_THREADS_JOIN_TIMEOUT_SECONDS: u64 = 10;

// allow multiple connections for NAT and any open/close overlap
pub const MAX_QUIC_CONNECTIONS_PER_IP: usize = 8;

pub struct TpuSockets {
    pub transactions_sockets: Vec<UdpSocket>,
    pub transactions_forward_sockets: Vec<UdpSocket>,
    pub transactions_quic_sockets: UdpSocket,
    pub transactions_forwards_quic_sockets: UdpSocket,
}

pub struct Tpu {
    fetch_stage: FetchStage,
    staked_nodes_updater_service: StakedNodesUpdaterService,
    find_packet_sender_stake_stage: FindPacketSenderStakeStage,
    sigverify_stage: SigVerifyStage,
}

impl Tpu {
    pub fn new(
        sockets: TpuSockets,
        exit: &Arc<AtomicBool>,
        tpu_coalesce_ms: u64,
        keypair: &Keypair,
        tpu_ip: &IpAddr,
        tpu_fwd_ip: &IpAddr,
    ) -> (Self, Receiver<BankingPacketBatch>) {
        let TpuSockets {
            transactions_sockets,
            transactions_forward_sockets,
            transactions_quic_sockets,
            transactions_forwards_quic_sockets,
        } = sockets;

        let (packet_sender, packet_receiver) = unbounded();
        let (vote_sender, _) = unbounded();
        let (forwarded_packet_sender, forwarded_packet_receiver) = unbounded();

        let fetch_stage = FetchStage::new_with_sender(
            transactions_sockets,
            transactions_forward_sockets,
            vec![],
            exit,
            &packet_sender,
            &vote_sender,
            &forwarded_packet_sender,
            forwarded_packet_receiver,
            tpu_coalesce_ms,
            None,
        );

        let staked_nodes = Arc::new(RwLock::new(HashMap::new()));
        let staked_nodes_updater_service =
            StakedNodesUpdaterService::new(exit.clone(), staked_nodes.clone());

        let (find_packet_sender_stake_sender, find_packet_sender_stake_receiver) = unbounded();

        let find_packet_sender_stake_stage = FindPacketSenderStakeStage::new(
            packet_receiver,
            find_packet_sender_stake_sender,
            staked_nodes.clone(),
            "tpu-find-packet-sender-stake",
        );

        let stats = Arc::new(StreamStats::default());
        let tpu_quic_t = spawn_server(
            transactions_quic_sockets,
            keypair,
            *tpu_ip,
            packet_sender,
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
            forwarded_packet_sender,
            exit.clone(),
            MAX_QUIC_CONNECTIONS_PER_IP,
            staked_nodes,
            MAX_STAKED_CONNECTIONS.saturating_add(MAX_UNSTAKED_CONNECTIONS),
            0, // Prevent unstaked nodes from forwarding transactions
            stats,
        )
        .unwrap();

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
            },
            verified_receiver,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.fetch_stage.join()?;
        self.staked_nodes_updater_service.join()?;
        self.find_packet_sender_stake_stage.join()?;
        self.sigverify_stage.join()?;
        Ok(())
    }
}
