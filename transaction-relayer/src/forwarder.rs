use std::{
    collections::VecDeque,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use log::{error, warn};
use solana_core::banking_stage::BankingPacketBatch;
use solana_perf::packet::PacketBatch;
use tokio::sync::mpsc::error::TrySendError;

/// Forwards packets to the Block Engine handler thread then delays transactions for packet_delay_ms
/// before forwarding them to the validator.
pub fn start_forward_and_delay_thread(
    packet_receiver: Receiver<BankingPacketBatch>,
    delay_sender: Sender<Vec<PacketBatch>>,
    packet_delay_ms: u32,
    block_engine_sender: tokio::sync::mpsc::Sender<Vec<PacketBatch>>,
) -> JoinHandle<()> {
    const SLEEP_DURATION: Duration = Duration::from_millis(5);
    let packet_delay = Duration::from_millis(packet_delay_ms as u64);

    Builder::new()
        .name("jito-forward_packets_to_block_engine".into())
        .spawn(move || {
            let mut buffered_packet_batches = VecDeque::with_capacity(100_000);

            loop {
                match packet_receiver.recv_timeout(SLEEP_DURATION) {
                    Ok(packet_batch) => {
                        // try_send because the block engine receiver only drains when it's connected
                        // and we don't want to OOM on packet_receiver
                        match block_engine_sender.try_send(packet_batch.0.clone()) {
                            Ok(_) => {}
                            Err(TrySendError::Closed(_)) => {
                                error!("error sending packet batch to block engine handler");
                                break;
                            }
                            Err(TrySendError::Full(_)) => {
                                warn!("buffer is full!");
                            }
                        }
                        buffered_packet_batches.push_back((Instant::now(), packet_batch.0));
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        break;
                    }
                }

                while let Some((pushed_time, _)) = buffered_packet_batches.front() {
                    if pushed_time.elapsed() >= packet_delay {
                        if let Err(e) =
                            delay_sender.send(buffered_packet_batches.pop_front().unwrap().1)
                        {
                            error!("exiting forwarding delayed packets: {:?}", e);
                            break;
                        }
                    }
                }
            }
        })
        .unwrap()
}
