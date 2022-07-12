use std::{
    collections::VecDeque,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    thread::{sleep, spawn, JoinHandle},
    time::{Duration, Instant},
};

use clap::Parser;
use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use jito_block_engine::block_engine::BlockEngine;
use jito_core::tpu::{Tpu, TpuSockets};
use jito_protos::validator_interface_service::validator_interface_server::ValidatorInterfaceServer;
use jito_relayer::{
    auth::AuthenticationInterceptor, relayer::Relayer, schedule_cache::LeaderScheduleCache,
};
use jito_rpc::load_balancer::LoadBalancer;
use log::{error, info, warn};
use solana_core::banking_stage::BankingPacketBatch;
use solana_net_utils::multi_bind_in_range;
use solana_sdk::signature::{Keypair, Signer};
use tokio::{
    runtime::Builder,
    sync::mpsc::{channel, error::TrySendError},
};
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// IP address to bind to for transaction packets
    #[clap(long, env, value_parser, default_value_t = IpAddr::from_str("127.0.0.1").unwrap())]
    tpu_bind_ip: IpAddr,

    /// Port to bind to for tpu packets
    #[clap(long, env, value_parser, default_value_t = 10_500)]
    tpu_port: u16,

    /// Port to bind to for tpu fwd packets
    #[clap(long, env, value_parser, default_value_t = 10_501)]
    tpu_fwd_port: u16,

    /// Port to bind to for tpu packets
    #[clap(long, env, value_parser, default_value_t = 10_502)]
    tpu_quic_port: u16,

    /// Port to bind to for tpu fwd packets
    #[clap(long, env, value_parser, default_value_t = 10_503)]
    tpu_quic_fwd_port: u16,

    /// Bind IP address for GRPC server
    #[clap(long, env, value_parser, default_value_t = IpAddr::from_str("127.0.0.1").unwrap())]
    grpc_bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[clap(long, env, value_parser, default_value_t = 10102)]
    grpc_bind_port: u16,

    /// Number of TPU threads
    #[clap(long, env, value_parser, default_value_t = 32)]
    num_tpu_binds: usize,

    /// Number of TPU forward threads
    #[clap(long, env, value_parser, default_value_t = 16)]
    num_tpu_fwd_binds: usize,

    /// RPC server list
    #[clap(long, env, value_parser, default_value = "http://127.0.0.1:8899")]
    rpc_servers: Vec<String>,

    /// Websocket server list
    #[clap(long, env, value_parser, default_value = "ws://127.0.0.1:8900")]
    websocket_servers: Vec<String>,

    /// The public-facing IP address of this server
    #[clap(long, env, value_parser, default_value_t = IpAddr::from_str("127.0.0.1").unwrap())]
    public_ip: IpAddr,

    /// Skip authentication
    #[clap(long, env, value_parser)]
    no_auth: bool,

    /// Packet delay in milliseconds
    #[clap(long, env, value_parser, default_value_t = 200)]
    packet_delay_ms: u64,

    /// Block engine address
    #[clap(long, env, value_parser, default_value = "http://127.0.0.1:10101")]
    block_engine_url: String,
}

struct Sockets {
    tpu_sockets: TpuSockets,
    tpu_ip: IpAddr,
    tpu_fwd_ip: IpAddr,
}

fn get_sockets(args: &Args) -> Sockets {
    let (tpu_bind_port, transactions_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_port, args.tpu_port + 1),
        args.num_tpu_binds,
    )
    .expect("to bind tpu sockets");

    let (tpu_bind_fwd_port, transactions_forward_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_fwd_port, args.tpu_fwd_port + 1),
        args.num_tpu_fwd_binds,
    )
    .expect("to bind tpu_forward sockets");

    let (tpu_quic_bind_port, mut tpu_quic_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_quic_port, args.tpu_quic_port + 1),
        1,
    )
    .expect("to bind tpu_quic sockets");

    let (tpu_fwd_quic_bind_port, mut tpu_fwd_quic_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_quic_fwd_port, args.tpu_quic_fwd_port + 1),
        1,
    )
    .expect("to bind tpu_quic sockets");

    assert_eq!(tpu_bind_port, args.tpu_port);
    assert_eq!(tpu_bind_fwd_port, args.tpu_fwd_port);
    assert_eq!(tpu_quic_bind_port, args.tpu_quic_port);
    assert_eq!(tpu_fwd_quic_bind_port, args.tpu_quic_fwd_port);

    Sockets {
        tpu_sockets: TpuSockets {
            transactions_sockets,
            transactions_forward_sockets,
            transactions_quic_sockets: tpu_quic_sockets.pop().unwrap(),
            transactions_forwards_quic_sockets: tpu_fwd_quic_sockets.pop().unwrap(),
        },
        tpu_ip: IpAddr::from_str("0.0.0.0").unwrap(),
        tpu_fwd_ip: IpAddr::from_str("0.0.0.0").unwrap(),
    }
}

fn start_forward_and_delay_thread(
    packet_receiver: Receiver<BankingPacketBatch>,
    delay_sender: Sender<BankingPacketBatch>,
    packet_delay_ms: u64,
    block_engine_sender: tokio::sync::mpsc::Sender<BankingPacketBatch>,
) -> JoinHandle<()> {
    const SLEEP_DURATION: Duration = Duration::from_millis(5);
    let expiration_delay = Duration::from_millis(packet_delay_ms);

    thread::Builder::new()
        .name("jito-forward_packets_to_block_engine".into())
        .spawn(move || {
            let mut buffered_packet_batches = VecDeque::with_capacity(100_000);

            loop {
                match packet_receiver.recv_timeout(SLEEP_DURATION) {
                    Ok(packet_batch) => {
                        match block_engine_sender.try_send(packet_batch.clone()) {
                            Ok(_) => {}
                            Err(TrySendError::Closed(_)) => {
                                error!("error sending packet batch to block engine handler");
                                break;
                            }
                            Err(TrySendError::Full(_)) => {
                                warn!("full!");
                            }
                        }
                        buffered_packet_batches.push_back((Instant::now(), packet_batch));
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // do nothing
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        break;
                    }
                }

                // Instant is monotonically increasing
                while let Some((timestamp, _)) = buffered_packet_batches.front() {
                    if Instant::now().duration_since(*timestamp) >= expiration_delay {
                        if let Err(e) =
                            delay_sender.send(buffered_packet_batches.pop_front().unwrap().1)
                        {
                            error!("exiting forwarding delayed packets: {:?}", e);
                            break;
                        }
                    }
                }
            }
        })
        .unwrap()
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let sockets = get_sockets(&args);

    let keypair = Keypair::new();
    solana_metrics::set_host_id(keypair.pubkey().to_string());
    info!("Relayer Started with pubkey: {}", keypair.pubkey());

    let exit = Arc::new(AtomicBool::new(false));

    assert_eq!(
        args.rpc_servers.len(),
        args.websocket_servers.len(),
        "num rpc servers = num websocket servers"
    );
    assert!(!args.rpc_servers.is_empty(), "num rpc servers >= 1");

    let servers: Vec<(String, String)> = args
        .rpc_servers
        .into_iter()
        .zip(args.websocket_servers.into_iter())
        .collect();

    let (rpc_load_balancer, slot_receiver) = LoadBalancer::new(&servers, &exit);
    let rpc_load_balancer = Arc::new(Mutex::new(rpc_load_balancer));

    let (tpu, packet_receiver) = Tpu::new(
        sockets.tpu_sockets,
        &exit,
        5,
        &keypair,
        &sockets.tpu_ip,
        &sockets.tpu_fwd_ip,
        &rpc_load_balancer,
    );

    let leader_cache = Arc::new(LeaderScheduleCache::new(&rpc_load_balancer));
    let lc = leader_cache.clone();
    // ToDo:  Put this somewhere more reasonable and align with epoch updates
    let exit_l = exit.clone();
    spawn(move || {
        while !exit_l.load(Ordering::Relaxed) {
            lc.update_leader_cache();
            sleep(Duration::from_secs(10));
        }
    });

    let (delay_sender, delay_receiver) = unbounded();
    let (block_engine_sender, block_engine_receiver) = channel(1_000);

    let block_engine_forwarder = BlockEngine::new(args.block_engine_url, block_engine_receiver);

    let forward_and_delay_thread = start_forward_and_delay_thread(
        packet_receiver,
        delay_sender,
        args.packet_delay_ms,
        block_engine_sender,
    );

    let rt = Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let addr = SocketAddr::new(args.grpc_bind_ip, args.grpc_bind_port);
        info!("Relayer listening on: {}", addr);

        let relayer = Relayer::new(
            slot_receiver,
            delay_receiver,
            leader_cache.clone(),
            exit.clone(),
            args.public_ip,
            args.tpu_port,
            args.tpu_fwd_port,
        );

        let cache = leader_cache.clone();
        let auth_interceptor = AuthenticationInterceptor { cache };
        let svc = ValidatorInterfaceServer::with_interceptor(relayer, auth_interceptor);

        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("serve server");
    });

    exit.store(true, Ordering::Relaxed);

    tpu.join().unwrap();
    forward_and_delay_thread.join().unwrap();
}
