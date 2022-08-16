use std::{
    collections::{hash_map::RandomState, HashSet},
    str::FromStr,
    sync::{Arc, Mutex},
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use jito_protos::{
    auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, GenerateAuthTokensResponse, RefreshAccessTokenRequest, Role,
        Token,
    },
    block_engine::{
        accounts_of_interest_update, block_engine_relayer_client::BlockEngineRelayerClient,
        packet_batch_update::Msg, AccountsOfInterestRequest, AccountsOfInterestUpdate,
        ExpiringPacketBatch, PacketBatchUpdate,
    },
    convert::{packet_to_proto_packet, versioned_tx_from_packet},
    packet::{Packet as ProtoPacket, PacketBatch as ProtoPacketBatch},
    shared::{Header, Heartbeat},
};
use log::{error, *};
use prost_types::Timestamp;
use solana_metrics::{datapoint_error, datapoint_info};
use solana_perf::packet::PacketBatch;
use solana_sdk::transaction::VersionedTransaction;
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
    pub fn new(
        block_engine_url: String,
        auth_service_url: String,
        block_engine_receiver: Receiver<BlockEnginePackets>,
        keypair: Arc<Keypair>,
    ) -> BlockEngineRelayerHandler {
        let block_engine_forwarder = Self::start_block_engine_relayer_stream(
            block_engine_url,
            auth_service_url,
            block_engine_receiver,
            keypair,
        );
        BlockEngineRelayerHandler {
            block_engine_forwarder,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.block_engine_forwarder.join()
    }

    fn start_block_engine_relayer_stream(
        block_engine_url: String,
        auth_service_url: String,
        mut block_engine_receiver: Receiver<BlockEnginePackets>,
        keypair: Arc<Keypair>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("jito_block_engine_relayer_stream".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    loop {
                        match Self::auth_and_connect(
                            &block_engine_url,
                            &auth_service_url,
                            &mut block_engine_receiver,
                            &keypair,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("error authenticating and connecting: {:?}", e);
                                datapoint_error!("block_engine_relayer-error",
                                    "block_engine_url" => block_engine_url,
                                    "auth_service_url" => auth_service_url,
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
    async fn auth_and_connect(
        block_engine_url: &str,
        auth_service_url: &str,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
        keypair: &Arc<Keypair>,
    ) -> BlockEngineResult<()> {
        let auth_endpoint = Endpoint::from_str(auth_service_url).expect("valid auth url");
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

        let block_engine_endpoint =
            Endpoint::from_str(block_engine_url).expect("valid block engine url");
        let block_engine_channel = block_engine_endpoint
            .connect()
            .await
            .map_err(|e| BlockEngineError::BlockEngineFailure(e.to_string()))?;

        datapoint_info!("block_engine-connection_stats",
            "block_engine_url" => block_engine_url,
            "auth_service_url" => auth_service_url,
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
        )
        .await
    }

    /// Starts the bi-directional packet stream.
    /// The relayer will send heartbeats and packets to the block engine.
    /// The block engine will send heartbeats back to the relayer.
    /// If there's a missed heartbeat or any issues responding to each other, they'll disconnect and
    /// try to re-establish connection
    async fn start_event_loop(
        mut client: BlockEngineRelayerClient<InterceptedService<Channel, AuthInterceptor>>,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
        auth_client: AuthServiceClient<Channel>,
        keypair: &Arc<Keypair>,
        refresh_token: &mut Token,
        shared_access_token: Arc<Mutex<Token>>,
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
        )
        .await
    }

    async fn handle_packet_stream(
        block_engine_packet_sender: Sender<PacketBatchUpdate>,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
        subscribe_aoi_stream: Response<Streaming<AccountsOfInterestUpdate>>,
        mut auth_client: AuthServiceClient<Channel>,
        keypair: &Arc<Keypair>,
        refresh_token: &mut Token,
        shared_access_token: Arc<Mutex<Token>>,
    ) -> BlockEngineResult<()> {
        let mut aoi_stream = subscribe_aoi_stream.into_inner();

        // drain old buffered packets before streaming packets to the block engine
        while block_engine_receiver.try_recv().is_ok() {}

        let mut accounts_of_interest: HashSet<Pubkey, RandomState> = HashSet::new();

        let mut heartbeat_count = 0;
        let mut aoi_update_count = 0;
        let mut auth_refresh_count = 0;
        let mut packet_forward_count = 0;

        let mut heartbeat = interval(Duration::from_millis(500));
        let mut refresh_interval = interval(Duration::from_secs(60));
        let mut metrics_interval = interval(Duration::from_secs(1));

        loop {
            select! {
                _ = heartbeat.tick() => {
                    Self::check_and_send_heartbeat(&block_engine_packet_sender, &heartbeat_count).await?;
                    heartbeat_count += 1;
                }
                maybe_aoi = aoi_stream.message() => {
                    Self::handle_aoi(maybe_aoi, &mut accounts_of_interest).await?;
                    aoi_update_count += 1;
                }
                block_engine_batches = block_engine_receiver.recv() => {
                    let filtered_packets = Self::filter_aoi_packets(block_engine_batches, &accounts_of_interest).await;
                    packet_forward_count += Self::forward_packets(&block_engine_packet_sender, filtered_packets).await?;
                }
                _ = refresh_interval.tick() => {
                    Self::maybe_refresh_auth(&mut auth_client, keypair, refresh_token, &shared_access_token, &mut auth_refresh_count).await?;
                }
                _ = metrics_interval.tick() => {
                    datapoint_info!("block_engine_relayer-loop_stats",
                        ("heartbeat_count", heartbeat_count, i64),
                        ("accounts_of_interest_len", accounts_of_interest.len(), i64),
                        ("aoi_update_count", aoi_update_count, i64),
                        ("auth_refresh_count", auth_refresh_count, i64),
                        ("packet_forward_count", packet_forward_count, i64),
                    );
                }
            }
        }
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

    async fn handle_aoi(
        maybe_msg: Result<Option<AccountsOfInterestUpdate>, Status>,
        accounts_of_interest: &mut HashSet<Pubkey, RandomState>,
    ) -> BlockEngineResult<()> {
        match maybe_msg {
            Ok(Some(aoi_update)) => match aoi_update.msg {
                None => Err(BlockEngineError::BlockEngineFailure(
                    "AOI message malformed".to_string(),
                )),
                Some(accounts_of_interest_update::Msg::Add(accounts)) => {
                    let accounts: HashSet<Pubkey> = accounts
                        .accounts
                        .iter()
                        .filter_map(|a| Pubkey::from_str(a).ok())
                        .collect();

                    for a in accounts {
                        accounts_of_interest.insert(a);
                    }

                    Ok(())
                }
                Some(accounts_of_interest_update::Msg::Remove(accounts)) => {
                    let accounts: HashSet<Pubkey> = accounts
                        .accounts
                        .iter()
                        .filter_map(|a| Pubkey::from_str(a).ok())
                        .collect();

                    for a in accounts {
                        accounts_of_interest.remove(&a);
                    }
                    Ok(())
                }
                Some(accounts_of_interest_update::Msg::Overwrite(accounts)) => {
                    *accounts_of_interest = accounts
                        .accounts
                        .iter()
                        .filter_map(|a| Pubkey::from_str(a).ok())
                        .collect();
                    Ok(())
                }
            },
            Ok(None) => Err(BlockEngineError::BlockEngineFailure(
                "disconnected".to_string(),
            )),
            Err(e) => Err(BlockEngineError::BlockEngineFailure(e.to_string())),
        }
    }

    /// Forwards packets to the Block Engine
    async fn forward_packets(
        block_engine_packet_sender: &Sender<PacketBatchUpdate>,
        maybe_batch: BlockEngineResult<ExpiringPacketBatch>,
    ) -> BlockEngineResult<usize> {
        let batch = maybe_batch
            .map_err(|_e| BlockEngineError::BlockEngineFailure("disconnected".to_string()))?;

        let num_packets = batch.clone().batch.unwrap().packets.len();

        if block_engine_packet_sender
            .send(PacketBatchUpdate {
                msg: Some(Msg::Batches(batch)),
            })
            .await
            .is_err()
        {
            Err(BlockEngineError::BlockEngineFailure(
                "disconnected".to_string(),
            ))
        } else {
            Ok(num_packets)
        }
    }

    ///
    async fn filter_aoi_packets(
        block_engine_batches: Option<BlockEnginePackets>,
        accounts_of_interest: &HashSet<Pubkey, RandomState>,
    ) -> BlockEngineResult<ExpiringPacketBatch> {
        let block_engine_batches = block_engine_batches
            .ok_or_else(|| BlockEngineError::BlockEngineFailure("disconnected".to_string()))?;

        let packets_txs: Vec<(ProtoPacket, VersionedTransaction)> = block_engine_batches
            .packet_batches
            .into_iter()
            .flat_map(|b| {
                b.iter()
                    .filter_map(|p| {
                        let pb = packet_to_proto_packet(p)?;
                        Some((pb.clone(), versioned_tx_from_packet(&pb)?))
                    })
                    .collect::<Vec<(ProtoPacket, VersionedTransaction)>>()
            })
            .collect();

        let mut filtered_packets = Vec::new();
        packets_txs.into_iter().for_each(|(packet, tx)| {
            let tx_accounts: HashSet<Pubkey> =
                HashSet::from_iter(tx.message.static_account_keys().to_vec());
            if tx_accounts.iter().any(|a| accounts_of_interest.contains(a)) {
                filtered_packets.push(packet);
            }
        });

        let filtered_batch = ExpiringPacketBatch {
            header: Some(Header {
                ts: Some(Timestamp::from(block_engine_batches.stamp)),
            }),
            batch: Some(ProtoPacketBatch {
                packets: filtered_packets,
            }),
            expiry_ms: block_engine_batches.expiration,
        };

        Ok(filtered_batch)
    }

    /// Checks the heartbeat timeout and errors out if the heartbeat didn't come in time.
    /// Assuming that's okay, sends a heartbeat back and if that fails, disconnect.
    async fn check_and_send_heartbeat(
        block_engine_packet_sender: &Sender<PacketBatchUpdate>,
        heartbeat_count: &u64,
    ) -> BlockEngineResult<()> {
        if block_engine_packet_sender
            .send(PacketBatchUpdate {
                msg: Some(Msg::Heartbeat(Heartbeat {
                    count: *heartbeat_count,
                })),
            })
            .await
            .is_err()
        {
            return Err(BlockEngineError::BlockEngineFailure(
                "disconnected".to_string(),
            ));
        }

        Ok(())
    }
}
