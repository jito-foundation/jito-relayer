use clap::Parser;
use jito_core::tpu::{Tpu, TpuSockets};
use jito_rpc::load_balancer::LoadBalancer;
use solana_net_utils::multi_bind_in_range;
use solana_sdk::signature::Keypair;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// IP address to bind to for transaction packets
    #[clap(long, env, default_value_t = IpAddr::from_str("0.0.0.0").unwrap())]
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
    server_bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[clap(long, env, default_value_t = 42069)]
    server_bind_port: u16,

    /// Number of TPU threads
    #[clap(long, env, default_value_t = 32)]
    num_tpu_binds: usize,

    /// Number of TPU forward threads
    #[clap(long, env, default_value_t = 16)]
    num_tpu_fwd_binds: usize,

    /// RPC server list
    #[clap(long, env)]
    rpc_servers: Vec<String>,

    /// Websocket server list
    #[clap(long, env)]
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

    let exit = Arc::new(AtomicBool::new(false));

    let (tpu, packet_receiver) = Tpu::new(
        sockets.tpu_sockets,
        &exit,
        5,
        &keypair,
        &sockets.tpu_ip,
        &sockets.tpu_fwd_ip,
    );

    let servers: Vec<(String, String)> = args
        .rpc_servers
        .into_iter()
        .zip(args.websocket_servers.into_iter())
        .collect();

    let rpc_load_balancer = LoadBalancer::new(&servers, &exit);

    sleep(Duration::from_secs(10));

    exit.store(true, Ordering::Relaxed);
    tpu.join().unwrap();
    rpc_load_balancer.join().unwrap();
}
