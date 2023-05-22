use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use dashmap::DashMap;
use log::{error, info};
use solana_client::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use solana_metrics::{datapoint_error, datapoint_info};
use solana_sdk::{clock::Slot, commitment_config::CommitmentConfig};

pub struct LoadBalancer {
    /// (ws_url, slot)
    server_to_slot: Arc<DashMap<String, Slot>>,
    /// (rpc_url, client)
    servers: DashMap<String, String>,
    subscription_threads: Vec<JoinHandle<()>>,
}

impl LoadBalancer {
    const DISCONNECT_WEBSOCKET_TIMEOUT: Duration = Duration::from_secs(30);
    pub const SLOT_QUEUE_CAPACITY: usize = 100;
    pub fn new(
        servers: &[(String, String)], /* http rpc url, ws url */
        exit: &Arc<AtomicBool>,
    ) -> (LoadBalancer, Receiver<Slot>) {
        let server_to_slot = Arc::new(DashMap::from_iter(
            servers.iter().map(|(_, ws)| (ws.clone(), 0)),
        ));

        // sender tracked as health_manager-channel_stats.slot_sender_len
        let (slot_sender, slot_receiver) = crossbeam_channel::bounded(Self::SLOT_QUEUE_CAPACITY);
        let subscription_threads =
            Self::start_subscription_threads(servers, server_to_slot.clone(), slot_sender, exit);
        (
            LoadBalancer {
                server_to_slot,
                servers: servers
                    .iter()
                    .map(|(http, ws)| (http.to_string(), ws.to_string()))
                    .collect(),
                subscription_threads,
            },
            slot_receiver,
        )
    }

    fn start_subscription_threads(
        servers: &[(String, String)],
        server_to_slot: Arc<DashMap<String, Slot>>,
        slot_sender: Sender<Slot>,
        exit: &Arc<AtomicBool>,
    ) -> Vec<JoinHandle<()>> {
        let highest_slot = Arc::new(AtomicU64::default());

        servers
            .iter()
            .map(|(_, websocket_url)| {
                let ws_url_no_token = websocket_url
                    .split('?')
                    .next()
                    .unwrap_or_default()
                    .to_string();
                let exit = exit.clone();
                let websocket_url = websocket_url.clone();
                let server_to_slot = server_to_slot.clone();
                let slot_sender = slot_sender.clone();
                let highest_slot = highest_slot.clone();

                Builder::new()
                    .name(format!("load_balancer_subscription_thread-{ws_url_no_token}"))
                    .spawn(move || {
                        while !exit.load(Ordering::Relaxed) {
                            info!("running slot_subscribe() with url: {websocket_url}");
                            let mut last_slot_update = Instant::now();

                            match PubsubClient::slot_subscribe(&websocket_url) {
                                Ok((_subscription, receiver)) => {
                                    while !exit.load(Ordering::Relaxed) {
                                        match receiver.recv_timeout(Duration::from_millis(100))
                                        {
                                            Ok(slot) => {
                                                last_slot_update = Instant::now();

                                                server_to_slot
                                                    .insert(websocket_url.clone(), slot.slot);
                                                datapoint_info!(
                                                        "rpc_load_balancer-slot_count",
                                                        "url" => ws_url_no_token,
                                                        ("slot", slot.slot, i64)
                                                );

                                                {
                                                    let old_slot = highest_slot.fetch_max(slot.slot, Ordering::Relaxed);
                                                    if slot.slot > old_slot {
                                                        if let Err(e) = slot_sender.send(slot.slot)
                                                        {
                                                            error!("error sending slot: {e}");
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            Err(RecvTimeoutError::Timeout) => {
                                                // RPC servers occasionally stop sending slot updates and never recover.
                                                // If enough time has passed, attempt to recover by forcing a new connection
                                                if last_slot_update.elapsed() >= Self::DISCONNECT_WEBSOCKET_TIMEOUT
                                                {
                                                    datapoint_error!(
                                                        "rpc_load_balancer-force_disconnect",
                                                        "url" => ws_url_no_token,
                                                        ("event", 1, i64)
                                                    );
                                                    break;
                                                }
                                            }
                                            Err(RecvTimeoutError::Disconnected) => {
                                                info!("slot subscribe disconnected. url: {ws_url_no_token}");
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "slot subscription error client: {ws_url_no_token}, error: {e:?}"
                                    );
                                }
                            }

                            sleep(Duration::from_secs(1));
                        }
                    })
                    .unwrap()
            })
            .collect()
    }

    /// Returns a new RPC client with the highest slot
    /// NOTE: if you're using a load balancer this might not do what you expect. For instance,
    /// the websocket could be attached to a different RPC server behind the scenes.
    pub fn rpc_client(&self) -> RpcClient {
        let (highest_server, _) = self.get_highest_slot();

        let rpc_url = self
            .servers
            .get(&highest_server)
            .unwrap()
            .value()
            .to_string();
        RpcClient::new_with_commitment(rpc_url, CommitmentConfig::processed())
    }

    /// Returns a new non-blocking RPC client with the highest slot
    /// NOTE: if you're using a load balancer this might not do what you expect. For instance,
    /// the websocket could be attached to a different RPC server behind the scenes.
    pub fn non_blocking_rpc_client(&self) -> solana_client::nonblocking::rpc_client::RpcClient {
        let (highest_server, _) = self.get_highest_slot();

        let rpc_url = self
            .servers
            .get(&highest_server)
            .unwrap()
            .value()
            .to_string();
        solana_client::nonblocking::rpc_client::RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig::processed(),
        )
    }

    pub fn get_highest_slot(&self) -> (String, Slot) {
        let multi = self
            .server_to_slot
            .iter()
            .max_by(|lhs, rhs| lhs.value().cmp(rhs.value()))
            .unwrap();
        let (server, slot) = multi.pair();
        (server.to_string(), *slot)
    }

    pub fn join(self) -> thread::Result<()> {
        for s in self.subscription_threads {
            s.join()?;
        }
        Ok(())
    }
}
