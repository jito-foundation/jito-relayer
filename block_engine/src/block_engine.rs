use std::{
    collections::HashSet,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant, SystemTime},
};

use cached::{Cached, TimedCache};
use jito_protos::{
    auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, GenerateAuthTokensResponse, RefreshAccessTokenRequest, Role,
        Token,
    },
    block_engine::{
        block_engine_relayer_client::BlockEngineRelayerClient, packet_batch_update::Msg,
        AccountsOfInterestRequest, AccountsOfInterestUpdate, ExpiringPacketBatch,
        PacketBatchUpdate,
    },
    convert::{packet_to_proto_packet, versioned_tx_from_packet},
    packet::{Packet as ProtoPacket, PacketBatch as ProtoPacketBatch},
    shared::{Header, Heartbeat},
};
use log::{error, *};
use prost_types::Timestamp;
use solana_metrics::{datapoint_error, datapoint_info};
use solana_perf::packet::PacketBatch;
use solana_sdk::{pubkey::Pubkey, signature::Signer, signer::keypair::Keypair};
use thiserror::Error;
use tokio::{
    runtime::Runtime,
    select,
    sync::mpsc::{channel, Receiver, Sender},
    time::{interval, sleep},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::InterceptedService,
    service::Interceptor,
    transport::{Channel, Endpoint},
    Response, Status, Streaming,
};

#[derive(Clone)]
struct AuthInterceptor {
    access_token: Arc<Mutex<Token>>,
}

impl AuthInterceptor {
    pub fn new(access_token: Arc<Mutex<Token>>) -> Self {
        AuthInterceptor { access_token }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.access_token.lock().unwrap().value)
                .parse()
                .unwrap(),
        );
        Ok(request)
    }
}

pub struct BlockEnginePackets {
    pub packet_batches: Vec<PacketBatch>,
    pub stamp: SystemTime,
    pub expiration: u32,
}

#[derive(Error, Debug)]
pub enum BlockEngineError {
    #[error("auth service failed: {0}")]
    AuthServiceFailure(String),

    #[error("block engine failed: {0}")]
    BlockEngineFailure(String),
}

pub type BlockEngineResult<T> = Result<T, BlockEngineError>;

/// Attempts to maintain a connection to a Block Engine and forward packets to it
pub struct BlockEngineRelayerHandler {
    block_engine_forwarder: JoinHandle<()>,
}

impl BlockEngineRelayerHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_engine_url: String,
        auth_service_url: String,
        block_engine_receiver: Receiver<BlockEnginePackets>,
        keypair: Arc<Keypair>,
        exit: &Arc<AtomicBool>,
        cluster: String,
        region: String,
        aoi_cache_ttl_s: u64,
    ) -> BlockEngineRelayerHandler {
        let block_engine_forwarder = Self::start_block_engine_relayer_stream(
            block_engine_url,
            auth_service_url,
            block_engine_receiver,
            keypair,
            exit,
            cluster,
            region,
            aoi_cache_ttl_s,
        );
        BlockEngineRelayerHandler {
            block_engine_forwarder,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.block_engine_forwarder.join()
    }

    #[allow(clippy::too_many_arguments)]
    fn start_block_engine_relayer_stream(
        block_engine_url: String,
        auth_service_url: String,
        mut block_engine_receiver: Receiver<BlockEnginePackets>,
        keypair: Arc<Keypair>,
        exit: &Arc<AtomicBool>,
        cluster: String,
        region: String,
        aoi_cache_ttl_s: u64,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        Builder::new()
            .name("jito_block_engine_relayer_stream".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    while !exit.load(Ordering::Relaxed) {
                        match Self::auth_and_connect(
                            &block_engine_url,
                            &auth_service_url,
                            &mut block_engine_receiver,
                            &keypair,
                            &exit,
                            &cluster,
                            &region,
                            aoi_cache_ttl_s,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("error authenticating and connecting: {:?}", e);
                                datapoint_error!("block_engine_relayer-error",
                                    "block_engine_url" => block_engine_url,
                                    "auth_service_url" => auth_service_url,
                                    "cluster" => &cluster,
                                    "region" => &region,
                                    ("error", e.to_string(), String)
                                );
                                sleep(Duration::from_secs(2)).await;
                            }
                        }
                    }
                });
            })
            .unwrap()
    }

    /// Relayers are whitelisted in the block engine. In order to auth, a challenge-response handshake
    /// is performed. After that, the relayer can fetch an access and refresh JWT token that's provided
    /// in request headers to the block engine.
    async fn auth(
        auth_client: &mut AuthServiceClient<Channel>,
        keypair: &Arc<Keypair>,
    ) -> BlockEngineResult<(Token, Token)> {
        let auth_response = auth_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: Role::Relayer.into(),
                pubkey: keypair.pubkey().to_bytes().to_vec(),
            })
            .await
            .map_err(|e| BlockEngineError::AuthServiceFailure(e.to_string()))?;

        let challenge = format!(
            "{}-{}",
            keypair.pubkey(),
            auth_response.into_inner().challenge
        );
        let signed_challenge = keypair.sign_message(challenge.as_bytes()).as_ref().to_vec();

        let GenerateAuthTokensResponse {
            access_token: maybe_access_token,
            refresh_token: maybe_refresh_token,
        } = auth_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge,
                client_pubkey: keypair.pubkey().as_ref().to_vec(),
                signed_challenge,
            })
            .await
            .map_err(|e| BlockEngineError::AuthServiceFailure(e.to_string()))?
            .into_inner();

        if maybe_access_token.is_none() || maybe_refresh_token.is_none() {
            return Err(BlockEngineError::AuthServiceFailure(
                "failed to get valid auth tokens".to_string(),
            ));
        }
        let access_token = maybe_access_token.unwrap();
        let refresh_token = maybe_refresh_token.unwrap();

        if access_token.expires_at_utc.is_none() || refresh_token.expires_at_utc.is_none() {
            return Err(BlockEngineError::AuthServiceFailure(
                "auth tokens don't have valid expiration time".to_string(),
            ));
        }

        Ok((access_token, refresh_token))
    }

    /// Authenticates the relayer with the block engine and connects to the forwarding service
    #[allow(clippy::too_many_arguments)]
    async fn auth_and_connect(
        block_engine_url: &str,
        auth_service_url: &str,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
        keypair: &Arc<Keypair>,
        exit: &Arc<AtomicBool>,
        cluster: &str,
        region: &str,
        aoi_cache_ttl_s: u64,
    ) -> BlockEngineResult<()> {
        let mut auth_endpoint = Endpoint::from_str(auth_service_url).expect("valid auth url");
        if auth_service_url.contains("https") {
            auth_endpoint = auth_endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .expect("invalid tls config");
        }
        let channel = auth_endpoint
            .connect()
            .await
            .map_err(|e| BlockEngineError::AuthServiceFailure(e.to_string()))?;
        let mut auth_client = AuthServiceClient::new(channel);

        let (access_token, mut refresh_token) = Self::auth(&mut auth_client, keypair).await?;

        let access_token_expiration =
            SystemTime::try_from(access_token.expires_at_utc.as_ref().unwrap().clone()).unwrap();
        let refresh_token_expiration =
            SystemTime::try_from(refresh_token.expires_at_utc.as_ref().unwrap().clone()).unwrap();

        info!(
            "access_token_expiration: {:?}, refresh_token_expiration: {:?}",
            access_token_expiration
                .duration_since(SystemTime::now())
                .unwrap(),
            refresh_token_expiration
                .duration_since(SystemTime::now())
                .unwrap()
        );

        let shared_access_token = Arc::new(Mutex::new(access_token));
        let auth_interceptor = AuthInterceptor::new(shared_access_token.clone());

        let mut block_engine_endpoint =
            Endpoint::from_str(block_engine_url).expect("valid block engine url");
        if block_engine_url.contains("https") {
            block_engine_endpoint = block_engine_endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .expect("invalid tls config");
        }
        let block_engine_channel = block_engine_endpoint
            .connect()
            .await
            .map_err(|e| BlockEngineError::BlockEngineFailure(e.to_string()))?;

        datapoint_info!("block_engine-connection_stats",
            "block_engine_url" => block_engine_url,
            "auth_service_url" => auth_service_url,
            "cluster" => cluster,
            "region" => region,
            ("connected", 1, i64)
        );

        let block_engine_client =
            BlockEngineRelayerClient::with_interceptor(block_engine_channel, auth_interceptor);
        Self::start_event_loop(
            block_engine_client,
            block_engine_receiver,
            auth_client,
            keypair,
            &mut refresh_token,
            shared_access_token,
            exit,
            String::from(cluster),
            String::from(region),
            aoi_cache_ttl_s,
        )
        .await
    }

    /// Starts the bi-directional packet stream.
    /// The relayer will send heartbeats and packets to the block engine.
    /// The block engine will send heartbeats back to the relayer.
    /// If there's a missed heartbeat or any issues responding to each other, they'll disconnect and
    /// try to re-establish connection
    #[allow(clippy::too_many_arguments)]
    async fn start_event_loop(
        mut client: BlockEngineRelayerClient<InterceptedService<Channel, AuthInterceptor>>,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
        auth_client: AuthServiceClient<Channel>,
        keypair: &Arc<Keypair>,
        refresh_token: &mut Token,
        shared_access_token: Arc<Mutex<Token>>,
        exit: &Arc<AtomicBool>,
        cluster: String,
        region: String,
        aoi_cache_ttl_s: u64,
    ) -> BlockEngineResult<()> {
        let subscribe_aoi_stream = client
            .subscribe_accounts_of_interest(AccountsOfInterestRequest {})
            .await
            .map_err(|e| BlockEngineError::BlockEngineFailure(e.to_string()))?;
        let (packet_msg_sender, packet_msg_receiver) = channel(100);
        let _response = client
            .start_expiring_packet_stream(ReceiverStream::new(packet_msg_receiver))
            .await
            .map_err(|e| BlockEngineError::BlockEngineFailure(e.to_string()))?;

        Self::handle_packet_stream(
            packet_msg_sender,
            block_engine_receiver,
            subscribe_aoi_stream,
            auth_client,
            keypair,
            refresh_token,
            shared_access_token,
            exit,
            cluster,
            region,
            aoi_cache_ttl_s,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_packet_stream(
        block_engine_packet_sender: Sender<PacketBatchUpdate>,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
        subscribe_aoi_stream: Response<Streaming<AccountsOfInterestUpdate>>,
        mut auth_client: AuthServiceClient<Channel>,
        keypair: &Arc<Keypair>,
        refresh_token: &mut Token,
        shared_access_token: Arc<Mutex<Token>>,
        exit: &Arc<AtomicBool>,
        cluster: String,
        region: String,
        aoi_cache_ttl_s: u64,
    ) -> BlockEngineResult<()> {
        let mut aoi_stream = subscribe_aoi_stream.into_inner();

        // drain old buffered packets before streaming packets to the block engine
        while block_engine_receiver.try_recv().is_ok() {}

        let mut accounts_of_interest: TimedCache<Pubkey, u8> =
            TimedCache::with_lifespan_and_capacity(aoi_cache_ttl_s, 1_000_000);

        let mut heartbeat_count = 0;
        let mut aoi_update_count = 0;
        let mut auth_refresh_count = 0;
        let mut packet_forward_count = 0;

        let mut heartbeat = interval(Duration::from_millis(500));
        let mut refresh_interval = interval(Duration::from_secs(60));
        let mut metrics_interval = interval(Duration::from_secs(1));

        while !exit.load(Ordering::Relaxed) {
            select! {
                _ = heartbeat.tick() => {
                    trace!("sending heartbeat");
                    Self::check_and_send_heartbeat(&block_engine_packet_sender, &heartbeat_count).await?;
                    heartbeat_count += 1;
                }
                maybe_aoi = aoi_stream.message() => {
                    trace!("received aoi message");
                    Self::handle_aoi(maybe_aoi, &mut accounts_of_interest)?;
                    aoi_update_count += 1;
                }
                block_engine_batches = block_engine_receiver.recv() => {
                    trace!("received block engine batches");
                    let block_engine_batches = block_engine_batches
                        .ok_or_else(|| BlockEngineError::BlockEngineFailure("disconnected".to_string()))?;
                    if let Some(filtered_packets) = Self::filter_aoi_packets(block_engine_batches, &mut accounts_of_interest).await {
                        packet_forward_count += Self::forward_packets(&block_engine_packet_sender, filtered_packets).await?;
                    }
                }
                _ = refresh_interval.tick() => {
                    trace!("refreshing auth interval");
                    Self::maybe_refresh_auth(&mut auth_client, keypair, refresh_token, &shared_access_token, &mut auth_refresh_count).await?;
                }
                _ = metrics_interval.tick() => {
                    trace!("flushing metrics");

                    // removes expired items from aoi cache
                    let flush_start = Instant::now();
                    accounts_of_interest.flush();
                    let flush_elapsed_us = flush_start.elapsed().as_micros();

                    datapoint_info!("block_engine_relayer-loop_stats",
                        "cluster" => &cluster,
                        "region" => &region,
                        ("heartbeat_count", heartbeat_count, i64),
                        ("accounts_of_interest_len", accounts_of_interest.cache_size(), i64),
                        ("aoi_update_count", aoi_update_count, i64),
                        ("auth_refresh_count", auth_refresh_count, i64),
                        ("packet_forward_count", packet_forward_count, i64),
                        ("flush_elapsed_us", flush_elapsed_us, i64)
                    );
                }
            }
        }
        Ok(())
    }

    /// Refresh authentication tokens if they're about to expire
    async fn maybe_refresh_auth(
        auth_client: &mut AuthServiceClient<Channel>,
        keypair: &Arc<Keypair>,
        refresh_token: &mut Token,
        shared_access_token: &Arc<Mutex<Token>>,
        auth_refresh_count: &mut u64,
    ) -> BlockEngineResult<()> {
        // expires_at_utc is checked for None when establishing connection
        let access_token_expiration_time = shared_access_token
            .lock()
            .unwrap()
            .expires_at_utc
            .as_ref()
            .unwrap()
            .clone();

        let access_token_expiration_time =
            SystemTime::try_from(access_token_expiration_time).unwrap();
        let access_token_duration_left =
            access_token_expiration_time.duration_since(SystemTime::now());

        let refresh_token_expiration_time =
            SystemTime::try_from(refresh_token.expires_at_utc.as_ref().unwrap().clone()).unwrap();
        let refresh_token_duration_left =
            refresh_token_expiration_time.duration_since(SystemTime::now());

        let is_access_token_expiring_soon = match access_token_duration_left {
            Ok(dur) => dur < Duration::from_secs(5 * 60),
            Err(_) => true,
        };
        let is_refresh_token_expiring_soon = match refresh_token_duration_left {
            Ok(dur) => dur < Duration::from_secs(5 * 60),
            Err(_) => true,
        };

        match (
            is_refresh_token_expiring_soon,
            is_access_token_expiring_soon,
        ) {
            (true, _) => {
                // re-run the authentication process from the beginning
                let (access_token, new_refresh_token) = Self::auth(auth_client, keypair).await?;

                *refresh_token = new_refresh_token;
                *shared_access_token.lock().unwrap() = access_token;
                info!("access and refresh token were refreshed");

                *auth_refresh_count += 1;

                Ok(())
            }
            (false, true) => {
                // fetch a new access token
                let response = auth_client
                    .refresh_access_token(RefreshAccessTokenRequest {
                        refresh_token: refresh_token.value.clone(),
                    })
                    .await
                    .map_err(|e| BlockEngineError::AuthServiceFailure(e.to_string()))?;

                let maybe_access_token = response.into_inner().access_token;
                if maybe_access_token.is_none() {
                    return Err(BlockEngineError::AuthServiceFailure(
                        "missing access token".to_string(),
                    ));
                }

                *shared_access_token.lock().unwrap() = maybe_access_token.unwrap();
                info!("access token was refreshed");

                *auth_refresh_count += 1;

                Ok(())
            }
            (false, false) => Ok(()),
        }
    }

    fn handle_aoi(
        maybe_msg: Result<Option<AccountsOfInterestUpdate>, Status>,
        accounts_of_interest: &mut TimedCache<Pubkey, u8>,
    ) -> BlockEngineResult<()> {
        match maybe_msg {
            Ok(Some(aoi_update)) => {
                let pubkeys: Vec<Pubkey> = aoi_update
                    .accounts
                    .iter()
                    .filter_map(|a| Pubkey::from_str(a).ok())
                    .collect();
                pubkeys.into_iter().for_each(|pubkey| {
                    accounts_of_interest.cache_set(pubkey, 0);
                });

                Ok(())
            }
            Ok(None) => Err(BlockEngineError::BlockEngineFailure(
                "disconnected".to_string(),
            )),
            Err(e) => Err(BlockEngineError::BlockEngineFailure(e.to_string())),
        }
    }

    /// Forwards packets to the Block Engine
    async fn forward_packets(
        block_engine_packet_sender: &Sender<PacketBatchUpdate>,
        batch: ExpiringPacketBatch,
    ) -> BlockEngineResult<usize> {
        let num_packets = batch.clone().batch.unwrap().packets.len();

        if let Err(e) = block_engine_packet_sender
            .send(PacketBatchUpdate {
                msg: Some(Msg::Batches(batch)),
            })
            .await
        {
            error!("error forwarding packets {}", e);
            Err(BlockEngineError::BlockEngineFailure(
                "disconnected".to_string(),
            ))
        } else {
            Ok(num_packets)
        }
    }

    /// Filters out packets that aren't on list of interest
    async fn filter_aoi_packets(
        block_engine_batches: BlockEnginePackets,
        accounts_of_interest: &mut TimedCache<Pubkey, u8>,
    ) -> Option<ExpiringPacketBatch> {
        let filtered_packets: Vec<ProtoPacket> = block_engine_batches
            .packet_batches
            .into_iter()
            .flat_map(|b| {
                b.iter()
                    .filter_map(|p| {
                        let pb = packet_to_proto_packet(p)?;
                        let tx = versioned_tx_from_packet(&pb)?;
                        let writable_iter = tx
                            .message
                            .static_account_keys()
                            .iter()
                            .enumerate()
                            .filter_map(|(idx, acc)| {
                                if tx.message.is_maybe_writable(idx) {
                                    Some(acc)
                                } else {
                                    None
                                }
                            });
                        let writable_accounts: HashSet<&Pubkey> = HashSet::from_iter(writable_iter);
                        if writable_accounts
                            .iter()
                            .any(|a| accounts_of_interest.cache_get(a).is_some())
                        {
                            Some(pb)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<ProtoPacket>>()
            })
            .collect::<Vec<ProtoPacket>>();

        if !filtered_packets.is_empty() {
            Some(ExpiringPacketBatch {
                header: Some(Header {
                    ts: Some(Timestamp::from(block_engine_batches.stamp)),
                }),
                batch: Some(ProtoPacketBatch {
                    packets: filtered_packets,
                }),
                expiry_ms: block_engine_batches.expiration,
            })
        } else {
            None
        }
    }

    /// Checks the heartbeat timeout and errors out if the heartbeat didn't come in time.
    /// Assuming that's okay, sends a heartbeat back and if that fails, disconnect.
    async fn check_and_send_heartbeat(
        block_engine_packet_sender: &Sender<PacketBatchUpdate>,
        heartbeat_count: &u64,
    ) -> BlockEngineResult<()> {
        if let Err(e) = block_engine_packet_sender
            .send(PacketBatchUpdate {
                msg: Some(Msg::Heartbeat(Heartbeat {
                    count: *heartbeat_count,
                })),
            })
            .await
        {
            error!("error sending heartbeat {}", e);
            return Err(BlockEngineError::BlockEngineFailure(
                "disconnected".to_string(),
            ));
        }

        Ok(())
    }
}
