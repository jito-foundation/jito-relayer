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
    ) -> Router {
        Router {
            slot_receiver,
            packet_receiver,
        }
    }
}
