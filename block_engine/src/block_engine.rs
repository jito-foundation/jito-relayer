use std::{
    collections::{hash_map::RandomState, HashSet},
    str::FromStr,
    sync::Arc,
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use jito_protos::{
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
    sync::mpsc::{channel, Receiver, Sender},
    time::{interval, sleep},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::InterceptedService,
    metadata::AsciiMetadataValue,
    service::Interceptor,
    transport::{Channel, Endpoint},
    IntoStreamingRequest, Request, Response, Status, Streaming,
};

#[derive(Clone)]
pub struct AuthenticationInterceptor {
    pub keypair: Arc<Keypair>,
}

impl Interceptor for AuthenticationInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let pubkey = AsciiMetadataValue::try_from(self.keypair.pubkey().to_string()).unwrap();
        // TODO (LB): add better authentication here by signing a message with keypair
        // Block Engine expects pubkey to be set
        request.metadata_mut().insert("pubkey", pubkey);
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
        block_engine_receiver: Receiver<BlockEnginePackets>,
        keypair: Arc<Keypair>,
    ) -> BlockEngineRelayerHandler {
        let block_engine_forwarder = Self::start_block_engine_relayer_stream(
            block_engine_url,
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
        mut block_engine_receiver: Receiver<BlockEnginePackets>,
        keypair: Arc<Keypair>,
    ) -> JoinHandle<()> {
        let endpoint = Endpoint::from_shared(block_engine_url.to_string())
            .expect("valid block engine endpoint");
        Builder::new()
            .name("jito_block_engine_relayer_stream".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    let auth_interceptor = AuthenticationInterceptor { keypair };
                    loop {
                        sleep(Duration::from_secs(1)).await;

                        info!("connecting to block engine at url: {:?}", block_engine_url);
                        match endpoint.connect().await {
                            Ok(channel) => {
                                let mut client = BlockEngineRelayerClient::with_interceptor(
                                    channel,
                                    auth_interceptor.clone(),
                                );
                                match Self::start_event_loop(
                                    &mut client,
                                    &mut block_engine_receiver,
                                )
                                .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("error with packet stream: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("error connecting: {:?}", e);
                            }
                        }
                    }
                });
            })
            .unwrap()
    }

    /// Starts the bi-directional packet stream.
    /// The relayer will send heartbeats and packets to the block engine.
    /// The block engine will send heartbeats back to the relayer.
    /// If there's a missed heartbeat or any issues responding to each other, they'll disconnect and
    /// try to re-establish connection
    async fn start_event_loop(
        client: &mut BlockEngineRelayerClient<
            InterceptedService<Channel, AuthenticationInterceptor>,
        >,
        block_engine_receiver: &mut Receiver<BlockEnginePackets>,
    ) -> BlockEngineResult<()> {
        let (packet_msg_sender, packet_msg_receiver) = channel::<PacketBatchUpdate>(100);
        let receiver_stream = ReceiverStream::new(packet_msg_receiver);

        let subscribe_aoi_stream = client
            .subscribe_accounts_of_interest(AccountsOfInterestRequest {})
            .await?;
        let _response = client
            .start_expiring_packet_stream(receiver_stream.into_streaming_request())
            .await?;

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
