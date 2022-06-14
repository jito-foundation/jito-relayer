use crossbeam_channel::Receiver;
use solana_core::banking_stage::BankingPacketBatch;
use solana_sdk::clock::Slot;

pub struct Router {
    slot_receiver: Receiver<Slot>,
    packet_receiver: Receiver<BankingPacketBatch>,
}

impl Router {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
        rpc_list: Vec<String>,
    ) -> Router {

        // Spawn task here that keeps up to date leader schedule


        Router {
            slot_receiver,
            packet_receiver,
        }
    }
}
