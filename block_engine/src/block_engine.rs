use std::{
    cell::RefCell,
    collections::{hash_map::RandomState, HashSet},
    rc::Rc,
    str::FromStr,
    sync::Arc,
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use jito_protos::{
    auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, GenerateAuthTokensResponse, Role,
    },
    block_engine::{
        accounts_of_interest_update, block_engine_relayer_client::BlockEngineRelayerClient,
        packet_batch_update::Msg, AccountsOfInterestRequest, AccountsOfInterestUpdate,
        ExpiringPacketBatch, PacketBatchUpdate,
    },
    convert::packet_to_proto_packet,
    packet::{Packet as ProtoPacket, PacketBatch as ProtoPacketBatch},
    shared::{Header, Heartbeat},
};
use log::{error, *};
use prost_types::Timestamp;
use solana_perf::packet::PacketBatch;
use solana_sdk::{pubkey::Pubkey, signature::Signer, signer::keypair::Keypair};
use thiserror::Error;
use tokio::{
    runtime::Runtime,
    select,
    sync::mpsc::{Receiver, Sender},
    time::{interval, sleep},
};
use tonic::{transport::Endpoint, Response, Status, Streaming};

use crate::auth::{AuthClient, AuthInterceptor};

pub struct BlockEnginePackets {
    pub packet_batches: Vec<PacketBatch>,
    pub stamp: SystemTime,
    pub expiration: u32,
}

#[derive(Error, Debug)]
pub enum BlockEngineError {
    #[error("auth service failed connection: {0}")]
    AuthServiceFailure(String),

    #[error("connection closed")]
    ConnectionClosedError,

    #[error("malformed message")]
    MalformedMessage,

    #[error("heartbeat timeout")]
    HeartbeatTimeout,

    #[error("GRPC error: {0}")]
    GrpcError(#[from] Status),
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
                            &block_engine_receiver,
                            &keypair,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("error authenticating and connecting: {:?}", e);
                            }
                        }
                    }
                });
            })
            .unwrap()
    }

    /// Authenticates the relayer with the block engine and connects to the forwarding service
    async fn auth_and_connect(
        block_engine_url: &str,
        auth_service_url: &str,
        block_engine_receiver: &Receiver<BlockEnginePackets>,
        keypair: &Arc<Keypair>,
    ) -> BlockEngineResult<()> {
        let auth_endpoint = Endpoint::from_str(auth_service_url).expect("valid auth url");
        let channel = auth_endpoint
            .connect()
            .await
            .map_err(|e| BlockEngineError::AuthServiceFailure(e.to_string()))?;
        let mut auth_client = AuthServiceClient::new(channel);

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

        info!(
            "access_token: {:?}, refresh_token:{:?}",
            access_token, refresh_token
        );

        //
        // let auth_interceptor =
        //     AuthInterceptor::new(keypair, Rc::new(RefCell::new(String::default())));
        // loop {
        //     sleep(Duration::from_secs(1)).await;
        //
        //     info!("connecting to block engine at url: {:?}", block_engine_url);
        //     match endpoint.connect().await {
        //         Ok(channel) => {
        //             let client = BlockEngineRelayerClient::with_interceptor(
        //                 channel,
        //                 auth_interceptor.clone(),
        //             );
        //             let mut client = AuthClient::new(client, 3);
        //
        //             match Self::start_event_loop(
        //                 &mut client,
        //                 &mut block_engine_receiver,
        //             )
        //             .await
        //             {
        //                 Ok(_) => {}
        //                 Err(e) => {
        //                     error!("error with packet stream: {:?}", e);
        //                 }
        //             }
        //         }
        //         Err(e) => {
        //             error!("error connecting: {:?}", e);
        //         }
        //     }
        // }
        Ok(())
    }

    /// Starts the bi-directional packet stream.
    /// The relayer will send heartbeats and packets to the block engine.
    /// The block engine will send heartbeats back to the relayer.
    /// If there's a missed heartbeat or any issues responding to each other, they'll disconnect and
    /// try to re-establish connection
    async fn start_event_loop(
        client: &mut AuthClient,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
    ) -> BlockEngineResult<()> {
        let subscribe_aoi_stream = client
            .subscribe_accounts_of_interest(AccountsOfInterestRequest {})
            .await?;
        let (packet_msg_sender, _response) = client.start_expiring_packet_stream(100).await?;

        Self::handle_packet_stream(
            packet_msg_sender,
            block_engine_receiver,
            subscribe_aoi_stream,
        )
        .await
    }

    async fn handle_packet_stream(
        block_engine_packet_sender: Sender<PacketBatchUpdate>,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
        subscribe_aoi_stream: Response<Streaming<AccountsOfInterestUpdate>>,
    ) -> BlockEngineResult<()> {
        let mut aoi_stream = subscribe_aoi_stream.into_inner();

        // drain old buffered packets before streaming packets to the block engine
        while block_engine_receiver.try_recv().is_ok() {}

        let mut accounts_of_interest: HashSet<Pubkey, RandomState> = HashSet::new();
        let mut heartbeat_count = 0;
        let heartbeat = interval(Duration::from_millis(500));
        tokio::pin!(heartbeat);
        loop {
            select! {
                _ = heartbeat.tick() => {
                    Self::check_and_send_heartbeat(&block_engine_packet_sender, &heartbeat_count).await?;
                    heartbeat_count += 1;
                }
                maybe_aoi = aoi_stream.message() => {
                    Self::handle_aoi(maybe_aoi, &mut accounts_of_interest).await?;
                }
                block_engine_batches = block_engine_receiver.recv() => {
                    Self::forward_packets(&block_engine_packet_sender, block_engine_batches).await?;
                }
            }
        }
    }

    async fn handle_aoi(
        maybe_msg: Result<Option<AccountsOfInterestUpdate>, Status>,
        accounts_of_interest: &mut HashSet<Pubkey, RandomState>,
    ) -> BlockEngineResult<()> {
        match maybe_msg {
            Ok(Some(aoi_update)) => match aoi_update.msg {
                None => Err(BlockEngineError::MalformedMessage),
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
            Ok(None) => Err(BlockEngineError::ConnectionClosedError),
            Err(e) => Err(e.into()),
        }
    }

    /// Forwards packets to the Block Engine
    async fn forward_packets(
        block_engine_packet_sender: &Sender<PacketBatchUpdate>,
        block_engine_batches: Option<BlockEnginePackets>,
    ) -> BlockEngineResult<()> {
        let block_engine_batches =
            block_engine_batches.ok_or(BlockEngineError::ConnectionClosedError)?;
        if block_engine_packet_sender
            .send(PacketBatchUpdate {
                msg: Some(Msg::Batches(ExpiringPacketBatch {
                    header: Some(Header {
                        ts: Some(Timestamp::from(block_engine_batches.stamp)),
                    }),
                    batch: Some(ProtoPacketBatch {
                        packets: block_engine_batches
                            .packet_batches
                            .into_iter()
                            .flat_map(|b| {
                                b.iter()
                                    .filter_map(packet_to_proto_packet)
                                    .collect::<Vec<ProtoPacket>>()
                            })
                            .collect(),
                    }),
                    expiry_ms: block_engine_batches.expiration,
                })),
            })
            .await
            .is_err()
        {
            Err(BlockEngineError::ConnectionClosedError)
        } else {
            Ok(())
        }
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
            return Err(BlockEngineError::ConnectionClosedError);
        }

        Ok(())
    }
}
