use std::thread::{Builder, JoinHandle};

use solana_core::banking_stage::BankingPacketBatch;
use tokio::sync::mpsc::Receiver;

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
                tokio::spawn(async move {});
            })
            .unwrap()
    }
}
