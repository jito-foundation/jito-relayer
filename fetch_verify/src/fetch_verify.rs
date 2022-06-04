use std::{
    net::UdpSocket,
    sync::{atomic::AtomicBool, Arc},
    thread,
    thread::{Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use log::*;
use solana_core::{sigverify::TransactionSigVerifier, sigverify_stage::SigVerifyStage};
use solana_perf::{
    packet::{PacketBatch, PacketBatchRecycler},
    recycler::Recycler,
};
use solana_streamer::streamer;
use tokio::sync::mpsc::{
    unbounded_channel as unbounded_tokio_channel, UnboundedReceiver as UnboundedTokioReceiver,
    UnboundedSender as UnboundedTokioSender,
};

pub struct FetchVerify {
    thread_hdls: Vec<JoinHandle<()>>,
    sigverify_stage: SigVerifyStage,
}

impl FetchVerify {
    pub fn new(
        tpu_sockets: Vec<UdpSocket>,
        tpu_fwd_sockets: Vec<UdpSocket>,
        exit: &Arc<AtomicBool>,
        coalesce_ms: u64,
    ) -> (FetchVerify, UnboundedTokioReceiver<Vec<PacketBatch>>) {
        let (packet_sender, packet_receiver) = unbounded();

        let mut thread_hdls = Self::new_multi_fetch(
            tpu_sockets,
            tpu_fwd_sockets,
            exit,
            coalesce_ms,
            packet_sender,
        );

        let (adapter_sender, adapter_receiver) = unbounded();
        thread::spawn(move || loop {
            match packet_receiver.recv_timeout(Duration::from_secs(1)) {
                Ok(batch) => {
                    let mut batches = vec![batch];
                    while let Ok(batch) = packet_receiver.try_recv() {
                        batches.push(batch);
                    }
                    if let Err(e) = adapter_sender.send(batches) {
                        error!("error adapting packet batches {}", e);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    // do nothing
                }
            }
        });

        let (verified_sender, verified_receiver) = unbounded();
        let sigverify_stage = {
            let verifier = TransactionSigVerifier::default();
            SigVerifyStage::new(adapter_receiver, verified_sender, verifier)
        };

        let (verified_bridge_sender, verified_bridge_receiver) = unbounded_tokio_channel();
        thread_hdls.push(Self::start_crossbeam_tokio_bridge(
            verified_receiver,
            verified_bridge_sender,
        ));

        (
            FetchVerify {
                thread_hdls,
                sigverify_stage,
            },
            verified_bridge_receiver,
        )
    }

    /// Creates a thread for each socket for TPU
    fn new_multi_fetch(
        tpu_sockets: Vec<UdpSocket>,
        tpu_fwd_sockets: Vec<UdpSocket>,
        exit: &Arc<AtomicBool>,
        coalesce_ms: u64,
        sender: Sender<PacketBatch>,
    ) -> Vec<JoinHandle<()>> {
        let recycler: PacketBatchRecycler = Recycler::warmed(1000, 1024);

        let tpu_sockets = tpu_sockets.into_iter().map(Arc::new);
        let tpu_fwd_sockets = tpu_fwd_sockets.into_iter().map(Arc::new);

        let tpu_handles = tpu_sockets.map(|socket| {
            streamer::receiver(
                socket,
                exit,
                sender.clone(),
                recycler.clone(),
                "fetch_stage",
                coalesce_ms,
                true,
            )
        });

        let tpu_fwd_handles = tpu_fwd_sockets.map(|socket| {
            streamer::receiver(
                socket,
                exit,
                sender.clone(),
                recycler.clone(),
                "fetch_forward_stage",
                coalesce_ms,
                true,
            )
        });

        tpu_handles.chain(tpu_fwd_handles).collect()
    }

    // bridge the sync <> async gap
    fn start_crossbeam_tokio_bridge(
        verified_receiver: Receiver<Vec<PacketBatch>>,
        verified_sender: UnboundedTokioSender<Vec<PacketBatch>>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("thread-verified-receiver-bridge".into())
            .spawn(move || loop {
                match verified_receiver.recv() {
                    Ok(batches) => {
                        if let Err(e) = verified_sender.send(batches) {
                            error!(
                                "error sending batches over bridge_sender channel [error={}]",
                                e
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        error!("verified_receiver channel disconnected [error={}]", e);
                        break;
                    }
                }
            })
            .unwrap()
    }

    /// TODO : return Result with ?
    pub fn join(self) {
        for t in self.thread_hdls {
            let _ = t.join().unwrap();
        }
        self.sigverify_stage.join().unwrap();
    }
}
