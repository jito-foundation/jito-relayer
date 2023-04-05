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

    /// Path to signer+payer keypairs
    #[arg(long, env)]
    keypair_path: PathBuf,

    /// Socket address for relayer TPU
    #[arg(long, env, default_value = "127.0.0.1:8009")]
    tpu_addr: SocketAddr,

    /// Method of connecting to solana TPU
    #[command(subcommand)]
    connection_mode: Mode,
}

#[derive(clap::Subcommand, Debug, Clone)]
enum Mode {
    /// Solana Quic
    Quic,

    /// Custom Quinn client
    Custom {
        /// Only works from localhost relative to relayer.
        /// Creates many 127.x.x.x addresses to overwhelm relayer.
        #[arg(long, env)]
        spam_from_localhost: bool,
    },
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

/// Generates many localhost sockets
pub fn local_socket_addr(thread_id: usize, port: u16, spam_from_localhost: bool) -> SocketAddr {
    let ip: [u8; 4] = (thread_id as u32).to_be_bytes();
    match spam_from_localhost {
        true => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, ip[1], ip[2], ip[3])), port),
        false => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port + thread_id as u16), /* for sending from remote machine */
    }
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
    let threads: Vec<_> = keypairs
        .into_iter()
        .enumerate()
        .map(|(thread_id, keypair)| {
            let client = Arc::new(RpcClient::new(&args.rpc_addr));
            let connection_mode = args.connection_mode.clone();
            Builder::new()
                .name(format!("packet-blaster-thread_{thread_id}"))
                .spawn(move || {
                    let tpu_sender = RUNTIME
                        .block_on(TpuSender::new(args.tpu_addr, &connection_mode, thread_id))
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

// taken from https://github.com/solana-labs/solana/blob/527e2d4f59c6429a4a959d279738c872b97e56b5/client/src/nonblocking/quic_client.rs#L42
struct SkipServerVerification;
impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}
impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

impl TpuSender {
    // source taken from https://github.com/solana-labs/solana/blob/527e2d4f59c6429a4a959d279738c872b97e56b5/client/src/nonblocking/quic_client.rs#L93
    // original code doesn't allow specifying source socket
    fn create_endpoint(send_addr: SocketAddr) -> quinn::Endpoint {
        let (certs, priv_key) =
            solana_streamer::tls_certificates::new_self_signed_tls_certificate_chain(
                &Keypair::new(),
                send_addr.ip(),
            )
            .expect("Failed to create QUIC client certificate");
        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_single_cert(certs, priv_key)
            .expect("Failed to set QUIC client certificates");
        crypto.enable_early_data = true;
        crypto.alpn_protocols =
            vec![solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID.to_vec()];

        let mut endpoint = quinn::Endpoint::client(send_addr).unwrap();
        let mut config = quinn::ClientConfig::new(Arc::new(crypto));

        let mut transport_config = quinn::TransportConfig::default();
        let timeout = quinn::IdleTimeout::from(quinn::VarInt::from_u32(
            solana_sdk::quic::QUIC_MAX_TIMEOUT_MS,
        ));
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(
            solana_sdk::quic::QUIC_KEEP_ALIVE_MS,
        )));
        config.transport_config(Arc::new(transport_config));

        endpoint.set_default_client_config(config);
        endpoint
    }

    async fn new(
        dest_addr: SocketAddr,
        connection_mode: &Mode,
        thread_id: usize,
    ) -> Result<TpuSender, anyhow::Error> {
        match connection_mode {
            Mode::Quic => Ok(TpuSender::QuicSender {
                client: QuicTpuConnection::new(
                    Arc::new(QuicLazyInitializedEndpoint::default()),
                    dest_addr,
                    Arc::new(ConnectionCacheStats::default()),
                ),
            }),
            Mode::Custom {
                spam_from_localhost,
            } => {
                let send_socket_addr = local_socket_addr(thread_id, 60000, *spam_from_localhost);
                let endpoint = Self::create_endpoint(send_socket_addr);
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
                        Ok::<(), quinn::WriteError>(())
                    })
                    .collect::<Vec<_>>();

                let results: Vec<Result<(), quinn::WriteError>> =
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
