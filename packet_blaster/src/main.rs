use std::{
    io,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    thread::Builder,
    time::{Duration, Instant},
};

use bincode::serialize;
use clap::Parser;
use log::*;
use solana_client::{
    connection_cache::ConnectionCacheStats, nonblocking::quic_client::QuicLazyInitializedEndpoint,
    quic_client::QuicTpuConnection, rpc_client::RpcClient, tpu_connection::TpuConnection,
};
use solana_sdk::{
    signature::{Keypair, Signature, Signer},
    system_transaction::transfer,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// RPC address
    #[clap(long, env, default_value = "http://127.0.0.1:8899")]
    rpc_addr: String,

    /// Number of keypairs
    #[clap(long, env, default_value_t = 10)]
    num_keypairs: u64,

    /// Socket address for relayer TPU
    #[clap(long, env, default_value = "127.0.0.1:11222")]
    tpu_addr: SocketAddr,

    /// Flag to use quic for relayer TPU
    #[clap(long, env, default_value_t = false)]
    use_quic: bool,
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let keypairs: Vec<_> = (0..args.num_keypairs)
        .map(|_| Arc::new(Keypair::new()))
        .collect();

    let pubkeys: Vec<_> = keypairs.iter().map(|kp| kp.pubkey()).collect();
    let pubkeys_str = pubkeys
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<String>>()
        .join(",");
    info!("using keypairs: {:?}", pubkeys_str);

    let client = Arc::new(RpcClient::new(&args.rpc_addr));
    assert!(request_and_confirm_airdrop(&client, &pubkeys));

    let threads: Vec<_> = keypairs
        .into_iter()
        .map(|keypair| {
            let client = Arc::new(RpcClient::new(&args.rpc_addr));
            Builder::new()
                .spawn(move || {
                    let tpu_sender = TpuSender::new(args.tpu_addr, &args.use_quic);
                    let mut last_blockhash_refresh = Instant::now();
                    let mut latest_blockhash = client.get_latest_blockhash().unwrap();
                    let mut last_count = 0;
                    info!("sending packets...");
                    let mut count: u64 = 0;
                    loop {
                        if last_blockhash_refresh.elapsed() > Duration::from_secs(5) {
                            let packets_per_second = (count - last_count) as f64
                                / last_blockhash_refresh.elapsed().as_secs_f64();
                            info!(
                                "packets sent/s: {:.2} ({} total)",
                                packets_per_second, count
                            );

                            last_blockhash_refresh = Instant::now();
                            latest_blockhash = client.get_latest_blockhash().unwrap();
                            last_count = count;
                        }

                        let serialized_txs: Vec<Vec<u8>> = (0..10)
                            .map(|_| {
                                count += 1;
                                serialize(&transfer(
                                    &keypair,
                                    &keypair.pubkey(),
                                    count,
                                    latest_blockhash,
                                ))
                                .unwrap()
                            })
                            .collect();

                        tpu_sender.send(serialized_txs);
                    }
                })
                .unwrap()
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }
}

fn request_and_confirm_airdrop(client: &RpcClient, pubkeys: &[solana_sdk::pubkey::Pubkey]) -> bool {
    let sigs: Vec<_> = pubkeys
        .iter()
        .map(|pubkey| client.request_airdrop(pubkey, 100_000_000_000))
        .collect();

    if sigs.iter().any(|s| s.is_err()) {
        return false;
    }
    let sigs: Vec<Signature> = sigs.into_iter().map(|s| s.unwrap()).collect();

    let now = Instant::now();
    while now.elapsed() < Duration::from_secs(20) {
        let r = client.get_signature_statuses(&sigs).expect("got statuses");
        if r.value.iter().all(|s| s.is_some()) {
            return true;
        }
    }
    false
}

enum TpuSender {
    UdpSender {
        address: SocketAddr,
        sock: UdpSocket,
    },
    QuicSender {
        client: QuicTpuConnection,
    },
}

impl TpuSender {
    fn new(addr: SocketAddr, use_quic: &bool) -> TpuSender {
        if *use_quic {
            TpuSender::QuicSender {
                client: QuicTpuConnection::new(
                    Arc::new(QuicLazyInitializedEndpoint::default()),
                    addr,
                    Arc::new(ConnectionCacheStats::default()),
                ),
            }
        } else {
            TpuSender::UdpSender {
                address: addr,
                sock: UdpSocket::bind("0.0.0.0:0").unwrap(),
            }
        }
    }

    fn send(&self, serialized_txs: Vec<Vec<u8>>) {
        match self {
            TpuSender::UdpSender { address, sock } => {
                let _: Vec<io::Result<usize>> = serialized_txs
                    .iter()
                    .map(|tx| sock.send_to(tx, address))
                    .collect();
            }
            TpuSender::QuicSender { client } => client
                .send_wire_transaction_batch_async(serialized_txs)
                .expect("quic send panic"),
        }
    }
}
