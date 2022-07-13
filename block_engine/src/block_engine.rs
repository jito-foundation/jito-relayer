use std::{
    thread::{Builder, JoinHandle},
    time::Duration,
};

use jito_protos::validator_interface_service::{
    packet_stream_msg::Msg::BatchList, validator_interface_client::ValidatorInterfaceClient,
};
use log::{error, warn};
use solana_core::banking_stage::BankingPacketBatch;
use tokio::{runtime::Runtime, sync::mpsc::Receiver, time::sleep};
use tokio_stream::iter;
use tonic::Request;

/// Attempts to maintain a connection to a Block Engine and forward packets to it
pub struct BlockEngine {
    block_engine_forwarder: JoinHandle<()>,
}

impl BlockEngine {
    pub fn new(
        block_engine_url: String,
        block_engine_receiver: Receiver<BankingPacketBatch>,
    ) -> BlockEngine {
        let block_engine_forwarder =
            Self::start_block_engine_forwarder(block_engine_url, block_engine_receiver);
        BlockEngine {
            block_engine_forwarder,
        }
    }

    fn start_block_engine_forwarder(
        block_engine_url: String,
        block_engine_receiver: Receiver<BankingPacketBatch>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("start_block_engine_forwarder".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    loop {
                        warn!("connecting to block engine...");
                        match ValidatorInterfaceClient::connect(block_engine_url.to_string()).await
                        {
                            Ok(mut client) => {
                                let mut request = Request::new(iter(vec![]));
                                match client.start_bi_directional_packet_stream(request).await {
                                    Ok(stream) => {
                                        let mut stream = stream.into_inner();
                                    }
                                    Err(e) => {
                                        error!("error connecting to server: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "can't connect to block engine: {:?}, error: {:?}",
                                    block_engine_url, e
                                );
                                sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                });
            })
            .unwrap()
    }
}
