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
use once_cell::sync::Lazy;
use quinn::WriteError;
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
    Custom,
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

// binds many localhost sockets
pub fn multi_bind_local(num: u32, port: u16) -> io::Result<Vec<UdpSocket>> {
    const NUM_TRIES: usize = 100;
    for _ in 0..NUM_TRIES {
        let sockets = (1..=num)
            .filter_map(|i| {
                let ip: [u8; 4] = i.to_be_bytes();
                let socket_addr = (IpAddr::V4(Ipv4Addr::new(127, ip[1], ip[2], ip[3])), port);
                UdpSocket::bind(socket_addr).ok()
            })
            .collect::<Vec<UdpSocket>>();
        if sockets.len() as u32 == num {
            return Ok(sockets);
        }
    }
    Err(io::Error::from(io::ErrorKind::AddrNotAvailable))
}

// binds many localhost sockets
pub fn multi_local_socket_addr(num: u32, port: u16) -> Vec<SocketAddr> {
    (1..=num)
        .map(|i| {
            let ip: [u8; 4] = i.to_be_bytes();
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, ip[1], ip[2], ip[3])), port)
        })
        .collect::<Vec<SocketAddr>>()
}

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

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
    let send_socket_addrs = multi_local_socket_addr(keypairs.len() as u32, 60000);
    let threads: Vec<_> = keypairs
        .into_iter()
        .zip(send_socket_addrs)
        .enumerate()
        .map(|(thread_id, (keypair, socket_addr))| {
            let client = Arc::new(RpcClient::new(&args.rpc_addr));
            let connection_mode = args.connection_mode.clone();
            Builder::new()
                .name(format!("packet-blaster-thread_{thread_id}"))
                .spawn(move || {
                    let tpu_sender = RUNTIME
                        .block_on(TpuSender::new(socket_addr, args.tpu_addr, &connection_mode))
                        .unwrap();
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
                        RUNTIME.block_on(tpu_sender.send(serialized_txs)).unwrap();
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
        send_addr: SocketAddr,
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
            Mode::Custom => {
                let endpoint = quinn::Endpoint::client(send_addr)?;

                // Connect to the server passing in the server name which is supposed to be in the server certificate.
                let connection = endpoint.connect(dest_addr, "connect")?.await?;
                Ok(TpuSender::CustomSender { connection })
            }
        }
    }

    async fn send(&self, serialized_txs: Vec<Vec<u8>>) -> Result<(), anyhow::Error> {
        match self {
            TpuSender::CustomSender { connection } => {
                let futures = serialized_txs
                    .into_iter()
                    .map(|buf| async move {
                        let mut send_stream = connection.open_uni().await?;
                        send_stream.write_all(&buf).await?;
                        send_stream.finish().await?;
                        Ok::<(), WriteError>(())
                    })
                    .collect::<Vec<_>>();

                let results: Vec<Result<(), WriteError>> =
                    futures_util::future::join_all(futures).await;
                for result in results {
                    if let Err(e) = result {
                        return Err(e.into());
                    }
                }
                Ok(())
            }
            TpuSender::QuicSender { client } => {
                client.send_wire_transaction_batch_async(serialized_txs)?;
                Ok(())
            }
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
