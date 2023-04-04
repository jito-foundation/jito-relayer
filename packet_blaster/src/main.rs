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
use clap::{Parser, ValueEnum};
use log::*;
use once_cell::sync::Lazy;
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    connection_cache::ConnectionCacheStats,
    nonblocking::quic_client::QuicLazyInitializedEndpoint,
    quic_client::QuicTpuConnection,
    rpc_client::RpcClient,
    tpu_connection::TpuConnection,
};
use solana_sdk::{
    native_token::LAMPORTS_PER_SOL,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_transaction::transfer,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// RPC address
    #[arg(long, env, default_value = "http://127.0.0.1:8899")]
    rpc_addr: String,

    /// Path to keypairs
    #[arg(long, env)]
    keypair_path: PathBuf,

    /// Socket address for relayer TPU
    #[arg(long, env, default_value = "127.0.0.1:8009")]
    tpu_addr: SocketAddr,

    /// Flag to use quic for relayer TPU
    #[command(subcommand)]
    connection_mode: Mode,
}

#[derive(clap::Subcommand, Debug, Clone)]
enum Mode {
    /// Solana Quic
    Quic,
    /// Custom Quinn client
    Custom { send_socket: SocketAddr },
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
            let connection_mode = args.connection_mode.clone();
            Builder::new()
                .name(format!("packet-blaster-thread_{thread_id}"))
                .spawn(move || {
                    let tpu_sender = TpuSender::new(args.tpu_addr, &connection_mode);
                    let metrics_interval = Duration::from_secs(5);
                    let mut last_blockhash_refresh = Instant::now();
                    let mut latest_blockhash = client.get_latest_blockhash().unwrap();
                    let mut curr_txn_count = 0u64;
                    info!("sending packets on thread {thread_id}");
                    loop {
                        let now = Instant::now();
                        let elapsed = now.sub(last_blockhash_refresh);
                        if elapsed > metrics_interval {
                            info!(
                                "packets sent/s: {:.2}, {curr_txn_count} total",
                                curr_txn_count as f64 / elapsed.as_secs_f64()
                            );
                            last_blockhash_refresh = now;
                            latest_blockhash = client.get_latest_blockhash().unwrap();
                        }

                        let serialized_txs: Vec<Vec<u8>> = (0..TXN_BATCH_SIZE)
                            .filter_map(|i| {
                                let txn = transfer(
                                    &keypair,
                                    &keypair.pubkey(),
                                    curr_txn_count + i,
                                    latest_blockhash,
                                );
                                println!(
                                    "pubkey: {}, lamports: {}, signature: {:?}",
                                    &keypair.pubkey(),
                                    curr_txn_count + i,
                                    &txn.signatures
                                );
                                serialize(&txn).ok()
                            })
                            .collect();
                        curr_txn_count += serialized_txs.len() as u64;
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
    CustomSender { connection: quinn::Connection },
    QuicSender { client: QuicTpuConnection },
}

impl TpuSender {
    async fn new(
        dest_addr: SocketAddr,
        connection_mode: &Mode,
    ) -> Result<TpuSender, anyhow::Error> {
        match connection_mode {
            Mode::Quic => Ok(TpuSender::QuicSender {
                client: QuicTpuConnection::new(
                    Arc::new(QuicLazyInitializedEndpoint::default()),
                    dest_addr,
                    Arc::new(ConnectionCacheStats::default()),
                ),
            }),
            Mode::Custom { send_socket } => {
                let mut endpoint = quinn::Endpoint::client(send_socket)?;

                // Connect to the server passing in the server name which is supposed to be in the server certificate.
                let connection = endpoint.connect(dest_addr, SERVER_NAME)?.await?;
                TpuSender::CustomSender { connection }
            }
        }
    }

    async fn send(&self, serialized_txs: Vec<Vec<u8>>) {
        match self {
            TpuSender::CustomSender {
                address,
                send_socket,
            } => {
                let _: Vec<io::Result<usize>> = serialized_txs
                    .iter()
                    .map(|tx| send_socket.send_to(tx, address))
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
        .map(|pubkey| client.request_airdrop(pubkey, 100 * LAMPORTS_PER_SOL))
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
