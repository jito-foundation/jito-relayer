use clap::Parser;
use std::{
    net::{IpAddr},
};
use solana_net_utils::multi_bind_in_range;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// IP address to bind to for transaction packets
    #[clap(long, env)]
    tpu_bind_ip: IpAddr,

    /// Port to bind to for tpu packets
    #[clap(long, env)]
    tpu_port: u16,

    /// Port to bind to for tpu fwd packets
    #[clap(long, env)]
    tpu_fwd_port: u16,

    /// Bind IP address for GRPC server
    #[clap(long, env)]
    server_bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[clap(long, env)]
    server_bind_port: IpAddr,

    /// Number of TPU threads
    #[clap(long, env, default_value_t = 32)]
    num_tpu_binds: usize,

    /// Number of TPU forward threads
    #[clap(long, env, default_value_t = 16)]
    num_tpu_fwd_binds: usize,

    /// RPC server list
    #[clap(long, env)]
    rpc_servers: Vec<String>,
}

fn main() {
    let args: Args = Args::parse();

    let (tpu_bind_port, tpu_sockets) =
        multi_bind_in_range(args.tpu_bind_ip, (args.tpu_port, args.tpu_port + 1), args.num_tpu_binds)
            .expect("to bind tpu sockets");

    let (tpu_bind_fwd_port, tpu_fwd_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_fwd_port, args.tpu_fwd_port + 1),
        args.num_tpu_fwd_binds,
    )
        .expect("to bind tpu_forward sockets");

    assert_eq!(tpu_bind_port, args.tpu_port);
    assert_eq!(tpu_bind_fwd_port, args.tpu_fwd_port);
}
