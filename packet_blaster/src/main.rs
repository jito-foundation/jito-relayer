use std::{
    fs, io,
    io::ErrorKind,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    ops::Sub,
    path::PathBuf,
    sync::Arc,
    thread::Builder,
    time::{Duration, Instant},
};

use bincode::serialize;
use clap::Parser;
use log::*;
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    connection_cache::ConnectionCacheStats,
    nonblocking::quic_client::QuicLazyInitializedEndpoint,
    quic_client::QuicTpuConnection,
    rpc_client::RpcClient,
    tpu_connection::TpuConnection,
};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_transaction::transfer,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// RPC address
    #[clap(long, env, default_value = "http://127.0.0.1:8899")]
    rpc_addr: String,

    /// Path to keypairs
    #[clap(long, env)]
    keypair_path: PathBuf,

    /// Socket address for relayer TPU
    #[clap(long, env, default_value = "127.0.0.1:8009")]
    tpu_addr: SocketAddr,

    /// Flag to use quic for relayer TPU
    #[clap(long, env, default_value_t = true)]
    use_quic: bool,
}

fn read_keypairs(path: PathBuf) -> io::Result<Vec<Keypair>> {
    if path.is_dir() {
        let result = fs::read_dir(path)?
            .filter_map(|entry| solana_sdk::signature::read_keypair_file(entry.ok()?.path()).ok())
            .collect::<Vec<_>>();
        Ok(result)
    } else {
        Ok(vec![Keypair::from_bytes(&fs::read(path)?).map_err(
            |e| io::Error::new(ErrorKind::NotFound, e.to_string()),
        )?])
    }
}

const TXN_BATCH_SIZE: u64 = 10;
fn main() {
    env_logger::init();

    let args: Args = Args::parse();
    dbg!(&args);

    let keypairs = read_keypairs(args.keypair_path).expect("Failed to prepare keypairs");
    let pubkeys: Vec<_> = keypairs.iter().map(|kp| kp.pubkey()).collect();
    info!(
        "Packet blaster going to send with {} pubkeys: {pubkeys:?}",
        pubkeys.len()
    );

    let threads: Vec<_> = keypairs
        .into_iter()
        .enumerate()
        .map(|(thread_id, keypair)| {
            let client = Arc::new(RpcClient::new(&args.rpc_addr));
            Builder::new()
                .name(format!("packet-blaster-thread_{thread_id}"))
                .spawn(move || {
                    let tpu_sender = TpuSender::new(args.tpu_addr, &args.use_quic);
                    let metrics_interval = Duration::from_secs(5);
                    let mut last_blockhash_refresh = Instant::now();
                    let mut latest_blockhash = client.get_latest_blockhash().unwrap();
                    let mut curr_txn_count = 0usize;
                    let mut prev_txn_count = 0usize;
                    info!("sending packets on thread {thread_id}");
                    loop {
                        let now = Instant::now();
                        let elapsed = now.sub(last_blockhash_refresh);
                        if elapsed > metrics_interval {
                            info!(
                                "packets sent/s: {:.2}, {curr_txn_count} total",
                                (curr_txn_count - prev_txn_count) as f64 / elapsed.as_secs_f64()
                            );

                            last_blockhash_refresh = now;
                            latest_blockhash = client.get_latest_blockhash().unwrap();
                            prev_txn_count = curr_txn_count;
                            curr_txn_count = 0;
                        }

                        let serialized_txs: Vec<Vec<u8>> = (0..TXN_BATCH_SIZE)
                            .filter_map(|i| {
                                serialize(&transfer(
                                    &keypair,
                                    &keypair.pubkey(),
                                    curr_txn_count as u64 + i,
                                    latest_blockhash,
                                ))
                                .ok()
                            })
                            .collect();
                        curr_txn_count += serialized_txs.len();
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
                sock: UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0u16))
                    .unwrap(),
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

#[allow(unused)]
fn request_and_confirm_airdrop(
    client: &RpcClient,
    pubkeys: &[Pubkey],
) -> solana_client::client_error::Result<()> {
    let sigs = pubkeys
        .iter()
        .map(|pubkey| client.request_airdrop(pubkey, 100_000_000_000))
        .collect::<solana_client::client_error::Result<Vec<Signature>>>()?;

    let now = Instant::now();
    while now.elapsed() < Duration::from_secs(20) {
        let r = client.get_signature_statuses(&sigs)?;
        if r.value.iter().all(|s| s.is_some()) {
            return Err(ClientError::from(ClientErrorKind::Custom(
                "signature error".to_string(),
            )));
        }
    }
    Ok(())
}
