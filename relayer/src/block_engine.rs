// use bitvec::prelude::BitVec;
use std::{collections::HashSet, net::SocketAddr, sync::RwLock};

use bitvec::vec::BitVec;
use jito_protos::packet::PacketBatchList as PbPacketBatchList;
// use crossbeam_channel::Receiver;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{
    signature::{Keypair, Signature},
    signer::Signer,
};

use crate::blocking_client::{AuthenticationInjector, BlockingClient};

pub(crate) struct BlockEngine {
    pub delta: RwLock<u32>,
    pub aoi: RwLock<HashSet<Pubkey>>,
    client: BlockingClient,
}

impl BlockEngine {
    pub fn new(block_eng_addr: String, keypair: Keypair) -> BlockEngine {
        let msg = b"Let's get this money!".to_vec();
        let sig: Signature = keypair.sign_message(msg.as_slice());
        let pubkey = keypair.pubkey();
        let interceptor = AuthenticationInjector::new(msg, sig, pubkey);

        let mut client = BlockingClient::new(block_eng_addr.clone(), &interceptor)
            .expect("connect to block engine!");

        BlockEngine {
            delta: RwLock::new(0u32),
            aoi: RwLock::new(HashSet::new()),
            client,
        }
    }

    pub fn stream_batch_list(&self, _batchlist: &PbPacketBatchList) {}
}
