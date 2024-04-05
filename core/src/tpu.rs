//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.
use std::{
    collections::HashMap,
    net::{IpAddr, UdpSocket},
    sync::{atomic::AtomicBool, Arc, RwLock},
    thread,
    thread::JoinHandle,
    time::Duration,
};

use crossbeam_channel::Receiver;
use jito_rpc::load_balancer::LoadBalancer;
use solana_core::{
    banking_trace::{BankingPacketBatch, BankingTracer},
    sigverify::TransactionSigVerifier,
    sigverify_stage::SigVerifyStage,
    tpu::MAX_QUIC_CONNECTIONS_PER_PEER,
};
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use solana_streamer::{
    nonblocking::quic::DEFAULT_WAIT_FOR_CHUNK_TIMEOUT,
    quic::{spawn_server, MAX_STAKED_CONNECTIONS},
    streamer::StakedNodes,
};

use crate::{fetch_stage::FetchStage, staked_nodes_updater_service::StakedNodesUpdaterService};

pub const DEFAULT_TPU_COALESCE_MS: u64 = 5;

// allow multiple connections for NAT and any open/close overlap
pub const MAX_QUIC_CONNECTIONS_PER_IP: usize = 8;

#[derive(Debug)]
pub struct TpuSockets {
    pub transactions_quic_sockets: Vec<UdpSocket>,
    pub transactions_forwards_quic_sockets: Vec<UdpSocket>,
}

pub struct Tpu {
    fetch_stage: FetchStage,
    staked_nodes_updater_service: StakedNodesUpdaterService,
    sigverify_stage: SigVerifyStage,
    thread_handles: Vec<JoinHandle<()>>,
}

impl Tpu {
    pub const TPU_QUEUE_CAPACITY: usize = 10_000;

    pub fn new(
        sockets: TpuSockets,
        exit: &Arc<AtomicBool>,
        keypair: &Keypair,
        tpu_ip: &IpAddr,
        tpu_fwd_ip: &IpAddr,
        rpc_load_balancer: &Arc<LoadBalancer>,
        max_unstaked_quic_connections: usize,
        staked_nodes_overrides: HashMap<Pubkey, u64>,
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
            staked_nodes_overrides,
        );

        // sender tracked as fetch_stage-channel_stats.tpu_sender_len
        let (tpu_sender, tpu_receiver) = crossbeam_channel::bounded(Tpu::TPU_QUEUE_CAPACITY);

        // receiver tracked as fetch_stage-channel_stats.tpu_forwards_receiver_len
        let (tpu_forwards_sender, tpu_forwards_receiver) =
            crossbeam_channel::bounded(Tpu::TPU_QUEUE_CAPACITY);

        let mut quic_tasks = transactions_quic_sockets
            .into_iter()
            .map(|sock| {
                spawn_server(
                    "quic_streamer_tpu",
                    sock,
                    keypair,
                    *tpu_ip,
                    tpu_sender.clone(),
                    exit.clone(),
                    MAX_QUIC_CONNECTIONS_PER_PEER,
                    staked_nodes.clone(),
                    MAX_STAKED_CONNECTIONS,
                    max_unstaked_quic_connections,
                    DEFAULT_WAIT_FOR_CHUNK_TIMEOUT,
                    Duration::from_millis(DEFAULT_TPU_COALESCE_MS),
                )
                .unwrap()
                .1
            })
            .collect::<Vec<_>>();

        quic_tasks.extend(
            transactions_forwards_quic_sockets
                .into_iter()
                .map(|sock| {
                    spawn_server(
                        "quic_streamer_tpu_forwards",
                        sock,
                        keypair,
                        *tpu_fwd_ip,
                        tpu_forwards_sender.clone(),
                        exit.clone(),
                        MAX_QUIC_CONNECTIONS_PER_PEER,
                        staked_nodes.clone(),
                        MAX_STAKED_CONNECTIONS.saturating_add(max_unstaked_quic_connections),
                        0, // Prevent unstaked nodes from forwarding transactions
                        DEFAULT_WAIT_FOR_CHUNK_TIMEOUT,
                        Duration::from_millis(DEFAULT_TPU_COALESCE_MS),
                    )
                    .unwrap()
                    .1
                })
                .collect::<Vec<_>>(),
        );

        let fetch_stage = FetchStage::new(tpu_forwards_receiver, tpu_sender, exit.clone());

        let (banking_packet_sender, banking_packet_receiver) =
            BankingTracer::new_disabled().create_channel_non_vote();
        let sigverify_stage = SigVerifyStage::new(
            tpu_receiver,
            TransactionSigVerifier::new(banking_packet_sender),
            "tpu-verifier",
        );

        (
            Tpu {
                fetch_stage,
                staked_nodes_updater_service,
                sigverify_stage,
                thread_handles: quic_tasks,
            },
            banking_packet_receiver,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.fetch_stage.join()?;
        self.staked_nodes_updater_service.join()?;
        self.sigverify_stage.join()?;
        for t in self.thread_handles {
            t.join()?
        }
        Ok(())
    }
}
