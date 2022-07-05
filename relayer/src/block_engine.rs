// use bitvec::prelude::BitVec;
use std::{collections::HashSet, net::SocketAddr, sync::RwLock};

use bitvec::vec::BitVec;
use jito_protos::packet::PacketBatchList as PbPacketBatchList;
// use crossbeam_channel::Receiver;
use solana_sdk::pubkey::Pubkey;

pub(crate) struct BlockEngine {
    pub delta: RwLock<u32>,
    pub aoi: RwLock<HashSet<Pubkey>>,
}

impl BlockEngine {
    pub fn new(_block_eng_addr: SocketAddr) -> BlockEngine {
        BlockEngine {
            delta: RwLock::new(0u32),
            aoi: RwLock::new(HashSet::new()),
        }
    }

    pub fn stream_aoi_batch_list(&self, _batchlist_with_mask: &(PbPacketBatchList, BitVec)) {}
}
