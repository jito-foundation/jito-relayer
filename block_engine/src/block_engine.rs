use std::{
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use jito_protos::validator_interface_service::{
    packet_stream_msg::Msg, validator_interface_client::ValidatorInterfaceClient,
    ExpiringPacketBatches, Heartbeat, PacketStreamMsg,
};
use log::{error, *};
use prost_types::Timestamp;
use thiserror::Error;
use tokio::{
    runtime::Runtime,
    select,
    sync::mpsc::{channel, Receiver, Sender},
    time::{interval, sleep},
};
use tonic::{IntoStreamingRequest, Response, Status, Streaming};

#[derive(Error, Debug)]
pub enum BlockEngineError {
    #[error("connection closed")]
    ConnectionClosedError,

    #[error("heartbeat timeout")]
    HeartbeatTimeout,
}

pub type BlockEngineResult<T> = Result<T, BlockEngineError>;

/// Attempts to maintain a connection to a Block Engine and forward packets to it
pub struct BlockEngine {
    block_engine_forwarder: JoinHandle<()>,
}

impl BlockEngine {
    pub fn new(
        block_engine_url: String,
        block_engine_receiver: Receiver<ExpiringPacketBatches>,
    ) -> BlockEngine {
        let block_engine_forwarder =
            Self::start_block_engine_forwarder(block_engine_url, block_engine_receiver);
        BlockEngine {
            block_engine_forwarder,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.block_engine_forwarder.join()
    }

    fn start_block_engine_forwarder(
        block_engine_url: String,
        mut block_engine_receiver: Receiver<ExpiringPacketBatches>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("start_block_engine_forwarder".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    loop {
                        sleep(Duration::from_secs(1)).await;

                        info!("connecting to block engine at url: {:?}", block_engine_url);
                        match ValidatorInterfaceClient::connect(block_engine_url.to_string()).await
                        {
                            Ok(mut client) => {
                                let (packet_msg_sender, packet_msg_receiver) =
                                    channel::<PacketStreamMsg>(1_000);
                                let receiver_stream = tokio_stream::wrappers::ReceiverStream::new(
                                    packet_msg_receiver,
                                );
                                match client
                                    .start_bi_directional_packet_stream(
                                        receiver_stream.into_streaming_request(),
                                    )
                                    .await
                                {
                                    Ok(stream) => {
                                        match Self::handle_packet_stream(
                                            packet_msg_sender,
                                            stream,
                                            &mut block_engine_receiver,
                                        )
                                        .await
                                        {
                                            Ok(_) => {}
                                            Err(e) => {
                                                error!("error handling packet stream: {:?}", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("error starting packet stream: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "can't connect to block engine: {:?}, error: {:?}",
                                    block_engine_url, e
                                );
                            }
                        }
                    }
                });
            })
            .unwrap()
    }

    async fn handle_packet_stream(
        packet_msg_sender: Sender<PacketStreamMsg>,
        stream: Response<Streaming<PacketStreamMsg>>,
        block_engine_receiver: &mut Receiver<ExpiringPacketBatches>,
    ) -> BlockEngineResult<()> {
        let mut stream = stream.into_inner();
        let heartbeat = interval(Duration::from_millis(500));

        let mut last_heartbeat_time = Instant::now();

        // drain anything buffered before sending new packets
        while let Ok(_) = block_engine_receiver.try_recv() {}

        tokio::pin!(heartbeat);
        loop {
            select! {
                _ = heartbeat.tick() => {
                    Self::check_and_send_heartbeat(&last_heartbeat_time, &packet_msg_sender).await?;
                }
                maybe_msg = stream.message() => {
                    Self::handle_receive_message(maybe_msg, &mut last_heartbeat_time).await?;
                }
                block_engine_packets = block_engine_receiver.recv() => {
                    Self::forward_packets(&packet_msg_sender, block_engine_packets).await?;
                }
            }
        }
    }

    async fn handle_receive_message(
        maybe_msg: Result<Option<PacketStreamMsg>, Status>,
        last_heartbeat_time: &mut Instant,
    ) -> BlockEngineResult<()> {
        match maybe_msg {
            Ok(Some(PacketStreamMsg {
                msg: Some(Msg::Heartbeat(heartbeat)),
            })) => match heartbeat.ts {
                None => {
                    error!("expected timestamp from block engine, disconnecting");
                    Err(BlockEngineError::ConnectionClosedError)
                }
                Some(ts) => {
                    *last_heartbeat_time = Instant::now();

                    let time_now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    let heartbeat_time = Duration::new(ts.seconds as u64, ts.nanos as u32);
                    let time_diff = time_now.as_secs_f64() - heartbeat_time.as_secs_f64();
                    info!("time_diff: {:?}", time_diff);
                    Ok(())
                }
            },
            Ok(Some(PacketStreamMsg {
                msg: Some(Msg::Batches(_)),
            })) => {
                error!("received packets but not expecting any, disconnecting");
                Err(BlockEngineError::ConnectionClosedError)
            }
            Ok(Some(PacketStreamMsg { msg: None })) => {
                error!("received packets but not expecting any, disconnecting");
                Err(BlockEngineError::ConnectionClosedError)
            }
            Ok(None) => {
                error!("received none, closing connection");
                Err(BlockEngineError::ConnectionClosedError)
            }
            Err(e) => {
                error!("error receiving message: {:?}", e);
                Err(BlockEngineError::ConnectionClosedError)
            }
        }
    }

    async fn forward_packets(
        packet_msg_sender: &Sender<PacketStreamMsg>,
        block_engine_packets: Option<ExpiringPacketBatches>,
    ) -> BlockEngineResult<()> {
        match block_engine_packets {
            None => Err(BlockEngineError::ConnectionClosedError),
            Some(block_engine_packets) => {
                if packet_msg_sender
                    .send(PacketStreamMsg {
                        msg: Some(Msg::Batches(block_engine_packets)),
                    })
                    .await
                    .is_err()
                {
                    Err(BlockEngineError::ConnectionClosedError)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Checks the heartbeat timeout and errors out if the heartbeat didn't come in time.
    /// Assuming that's okay, sends a heartbeat back and if that fails, disconnect.
    async fn check_and_send_heartbeat(
        last_heartbeat_time: &Instant,
        packet_msg_sender: &Sender<PacketStreamMsg>,
    ) -> BlockEngineResult<()> {
        if Instant::now().duration_since(*last_heartbeat_time) > Duration::from_secs_f32(1.5) {
            return Err(BlockEngineError::HeartbeatTimeout);
        }

        if packet_msg_sender
            .send(PacketStreamMsg {
                msg: Some(Msg::Heartbeat(Heartbeat {
                    ts: Some(Timestamp::from(SystemTime::now())),
                    count: 0,
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
