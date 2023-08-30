use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use auth::{generate_auth_tokens, AuthInterceptor};
use clap::Parser;
use jito_protos::{
    auth::auth_service_client::AuthServiceClient,
    relayer::{self, relayer_client::RelayerClient},
};
use jito_relayer_client::{auth, ProxyError, Result};
use log::{debug, error, info};
use solana_sdk::{signature::Keypair, signer::keypair::read_keypair_file};
use tokio::time::{interval, sleep, timeout};
use tonic::transport::Endpoint;
use tonic::Streaming;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// GRPC address of Relayer
    #[arg(long, env, default_value = "http://127.0.0.1:8899")]
    grpc_addr: String,

    /// Path to keypair file used to authenticate with the backend.
    #[arg(long, env)]
    keypair_path: PathBuf,
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();
    dbg!(&args);

    let keypair = read_keypair_file(args.keypair_path).expect("keypair file does not exist");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(start(args.grpc_addr, keypair));
}

async fn start(relayer_addr: String, keypair: Keypair) {
    const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
    const CONNECTION_BACKOFF: Duration = Duration::from_secs(5);
    let mut error_count: u64 = 0;

    loop {
        if let Err(_e) = connect_auth_and_stream(&relayer_addr, &keypair, &CONNECTION_TIMEOUT).await
        {
            error_count += 1;
            error!("Failed to connect to relayer.  Attempt: {}", error_count)
        }
        sleep(CONNECTION_BACKOFF).await;
    }
}

async fn connect_auth_and_stream(
    relayer_addr: &String,
    keypair: &Keypair,
    connection_timeout: &Duration,
) -> Result<()> {
    let mut relayer_endpoint = Endpoint::from_shared(relayer_addr.clone()).map_err(|_| {
        ProxyError::AuthenticationConnectionError(format!(
            "invalid relayer url value: {}",
            relayer_addr
        ))
    })?;
    if relayer_addr.starts_with("https") {
        relayer_endpoint = relayer_endpoint
            .tls_config(tonic::transport::ClientTlsConfig::new())
            .map_err(|_| {
                ProxyError::AuthenticationConnectionError(
                    "failed to set tls_config for relayer auth service".to_string(),
                )
            })?;
    }

    debug!("connecting to auth: {}", relayer_addr);
    let auth_channel = timeout(*connection_timeout, relayer_endpoint.connect())
        .await
        .map_err(|_| ProxyError::AuthenticationConnectionTimeout)?
        .map_err(|e| ProxyError::AuthenticationConnectionError(e.to_string()))?;

    let mut auth_client = AuthServiceClient::new(auth_channel);

    debug!("generating authentication token");
    let (access_token, _refresh_token) = timeout(
        *connection_timeout,
        generate_auth_tokens(&mut auth_client, keypair),
    )
    .await
    .map_err(|_| ProxyError::AuthenticationTimeout)??;

    debug!("connecting to relayer: {}", relayer_addr);
    let relayer_channel = timeout(*connection_timeout, relayer_endpoint.connect())
        .await
        .map_err(|_| ProxyError::RelayerConnectionTimeout)?
        .map_err(|e| ProxyError::RelayerConnectionError(e.to_string()))?;

    let access_token = Arc::new(Mutex::new(access_token));
    let mut relayer_client = RelayerClient::with_interceptor(
        relayer_channel,
        AuthInterceptor::new(access_token.clone()),
    );

    let mut packet_stream: Streaming<relayer::SubscribePacketsResponse> = timeout(
        *connection_timeout,
        relayer_client.subscribe_packets(relayer::SubscribePacketsRequest {}),
    )
    .await
    .map_err(|_| ProxyError::MethodTimeout("relayer_subscribe_packets".to_string()))?
    .map_err(|e| ProxyError::MethodError(e.to_string()))?
    .into_inner();

    let mut auth_tick = interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = auth_tick.tick() => {
                info!("ToDo: Implement Auth Check");
            }
            maybe_msg = packet_stream.message() => {
                let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                let timestamp = resp.header.unwrap().ts.unwrap();
                info!("Got packet. timestamp: {:?}", timestamp);
            }
        }
    }
}
