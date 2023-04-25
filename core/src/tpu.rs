//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

use std::{
    collections::HashSet,
    net::{IpAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::{sleep, Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::Receiver;
use dashmap::DashMap;
use jito_rpc::load_balancer::LoadBalancer;
use solana_core::{
    banking_stage::BankingPacketBatch, find_packet_sender_stake_stage::FindPacketSenderStakeStage,
    sigverify::TransactionSigVerifier, sigverify_stage::SigVerifyStage,
};
use solana_metrics::datapoint_info;
use solana_perf::packet::PacketBatch;
use solana_sdk::{
    address_lookup_table_account::AddressLookupTableAccount, pubkey::Pubkey, signature::Keypair,
};
use solana_streamer::{
    quic::{spawn_server, StreamStats, MAX_STAKED_CONNECTIONS, MAX_UNSTAKED_CONNECTIONS},
    streamer::StakedNodes,
};

use crate::{
    fetch_stage::FetchStage, ofac_stage::OfacStage,
    staked_nodes_updater_service::StakedNodesUpdaterService,
};

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
    ofac_stage: OfacStage,
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
        ofac_addresses: &HashSet<Pubkey>,
        address_lookup_table_cache: &Arc<DashMap<Pubkey, AddressLookupTableAccount>>,
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

        // sender tracked as fetch_stage-channel_stats.tpu_sender_len
        let (tpu_sender, tpu_receiver) = crossbeam_channel::bounded(Tpu::TPU_QUEUE_CAPACITY);

        // receiver tracked as fetch_stage-channel_stats.tpu_forwards_receiver_len
        let (tpu_forwards_sender, tpu_forwards_receiver) =
            crossbeam_channel::bounded(Tpu::TPU_QUEUE_CAPACITY);
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

        // receiver tracked in tpu-channel_stats.find_packet_sender_stake_receiver_len
        let (find_packet_sender_stake_sender, find_packet_sender_stake_receiver) =
            crossbeam_channel::bounded(Self::TPU_QUEUE_CAPACITY);
        let find_packet_sender_stake_stage = FindPacketSenderStakeStage::new(
            tpu_receiver,
            find_packet_sender_stake_sender,
            staked_nodes,
            "tpu_find_packet_sender_stake-stats",
        );

        let metrics_t =
            Self::start_metrics_thread(exit.clone(), find_packet_sender_stake_receiver.clone());

        // receiver tracked as forwarder_metrics.verified_receiver_len
        let (verified_sender, verified_receiver) =
            crossbeam_channel::bounded(Self::TPU_QUEUE_CAPACITY);
        let sigverify_stage = SigVerifyStage::new(
            find_packet_sender_stake_receiver,
            TransactionSigVerifier::new(verified_sender),
            "tpu-verifier",
        );

        let (ofac_sender, ofac_receiver) = crossbeam_channel::bounded(Self::TPU_QUEUE_CAPACITY);
        let ofac_stage = OfacStage::new(
            verified_receiver,
            ofac_sender,
            ofac_addresses,
            address_lookup_table_cache,
        );

        (
            Tpu {
                fetch_stage,
                staked_nodes_updater_service,
                find_packet_sender_stake_stage,
                sigverify_stage,
                thread_handles: vec![tpu_quic_t, tpu_forwards_quic_t, metrics_t],
                ofac_stage,
            },
            ofac_receiver,
        )
    }

    // channel consumed by solana code, receiver tracked in tpu-channel_stats.find_packet_sender_stake_receiver_len
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
                            "find_packet_sender_stake_receiver_len",
                            find_packet_sender_stake_receiver.len(),
                            i64
                        ),
                        (
                            "find_packet_sender_stake_receiver_capacity",
                            find_packet_sender_stake_receiver.capacity().unwrap(),
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
        self.ofac_stage.join()?;
        Ok(())
    }
}
