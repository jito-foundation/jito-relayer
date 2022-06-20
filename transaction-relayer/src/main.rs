use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use clap::Parser;
use jito_core::tpu::{Tpu, TpuSockets};
use jito_protos::validator_interface_service::validator_interface_server::ValidatorInterfaceServer;
use jito_relayer::auth::AuthenticationInterceptor;
use jito_relayer::relayer::Relayer;
use jito_relayer::schedule_cache::LeaderScheduleCache;
use jito_rpc::load_balancer::LoadBalancer;
use solana_net_utils::multi_bind_in_range;
use solana_sdk::signature::{Keypair, Signer};
use tokio::runtime::Builder;
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// IP address to bind to for transaction packets
    #[clap(long, env, default_value_t = IpAddr::from_str("127.0.0.1").unwrap())]
    tpu_bind_ip: IpAddr,

    /// Port to bind to for tpu packets
    #[clap(long, env, default_value_t = 8005)]
    tpu_port: u16,

    /// Port to bind to for tpu fwd packets
    #[clap(long, env, default_value_t = 8006)]
    tpu_fwd_port: u16,

    /// Port to bind to for tpu packets
    #[clap(long, env, default_value_t = 8007)]
    tpu_quic_port: u16,

    /// Port to bind to for tpu fwd packets
    #[clap(long, env, default_value_t = 8008)]
    tpu_quic_fwd_port: u16,

    /// Bind IP address for GRPC server
    #[clap(long, env, default_value_t = IpAddr::from_str("0.0.0.0").unwrap())]
    grpc_bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[clap(long, env, default_value_t = 42069)]
    grpc_bind_port: u16,

    /// Number of TPU threads
    #[clap(long, env, default_value_t = 32)]
    num_tpu_binds: usize,

    /// Number of TPU forward threads
    #[clap(long, env, default_value_t = 16)]
    num_tpu_fwd_binds: usize,

    /// RPC server list
    #[clap(long, env, default_value = "https://api.mainnet-beta.solana.com/")]
    rpc_servers: Vec<String>,

    /// Websocket server list
    #[clap(long, env, default_value = "wss://api.mainnet-beta.solana.com")]
    websocket_servers: Vec<String>,
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

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let sockets = get_sockets(&args);

    let keypair = Keypair::new();
    solana_metrics::set_host_id(keypair.pubkey().to_string());
    println!("Pub Key: {}", keypair.pubkey().to_string());

    let exit = Arc::new(AtomicBool::new(false));

    assert_eq!(
        args.rpc_servers.len(),
        args.websocket_servers.len(),
        "num rpc servers = num websocket servers"
    );
    assert!(args.rpc_servers.len() >= 1, "num rpc servers >= 1");

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

    let leader_cache = Arc::new(LeaderScheduleCache::new(
        &rpc_load_balancer,
        &keypair.pubkey().to_string(),
    ));

    // ToDo: Start a leader update loop here
    leader_cache.update_leader_cache();

    let rt = Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let addr = SocketAddr::new(args.grpc_bind_ip, args.grpc_bind_port);
        println!("Relayer listening on: {}", addr);

        let relayer = Relayer::new(slot_receiver, packet_receiver);

        // let cache = leader_cache.clone();
        // let auth_interceptor = AuthenticationInterceptor { cache };
        // let svc = ValidatorInterfaceServer::with_interceptor(relayer, auth_interceptor);

        let svc = ValidatorInterfaceServer::new(relayer);
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("serve server");
    });

    exit.store(true, Ordering::Relaxed);
    tpu.join().unwrap();
}
