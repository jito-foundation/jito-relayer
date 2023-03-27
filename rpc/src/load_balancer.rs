use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use dashmap::DashMap;
use log::{error, info};
use solana_client::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use solana_metrics::{datapoint_error, datapoint_info};
use solana_sdk::{
    clock::Slot,
    commitment_config::{CommitmentConfig, CommitmentLevel},
};

const DISCONNECT_WEBSOCKET_TIMEOUT_S: u64 = 30;

pub struct LoadBalancer {
    /// (ws_url, slot)
    server_to_slot: DashMap<String, Slot>,
    /// (rpc_url, client)
    server_to_rpc_client: DashMap<String, Arc<RpcClient>>,
    subscription_threads: Vec<JoinHandle<()>>,
}

impl LoadBalancer {
    const RPC_TIMEOUT: Duration = Duration::from_secs(15);

    pub fn new(
        servers: &[(String, String)], /* http rpc url, ws url */
        exit: &Arc<AtomicBool>,
        cluster: String,
        region: String,
    ) -> (LoadBalancer, Receiver<Slot>) {
        let server_to_slot = DashMap::from_iter(servers.iter().map(|(_, ws)| (ws.clone(), 0)));

        let server_to_rpc_client = DashMap::from_iter(servers.iter().map(|(rpc_url, ws)| {
            // warm up the connection
            let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
                rpc_url,
                LoadBalancer::RPC_TIMEOUT,
                CommitmentConfig {
                    commitment: CommitmentLevel::Processed,
                },
            ));
            if let Err(e) = rpc_client.get_slot() {
                error!("error warming up rpc: {rpc_url}. error: {e}");
            }
            // store as ws instead of http so we can lookup by furthest ahead ws subscription
            (ws.clone(), rpc_client)
        }));

        // sender tracked as health_manager-channel_stats.slot_sender-len
        let (slot_sender, slot_receiver) = unbounded();
        let subscription_threads = Self::start_subscription_threads(
            servers,
            &server_to_slot,
            &server_to_rpc_client,
            exit,
            slot_sender,
            cluster,
            region,
        );
        (
            LoadBalancer {
                server_to_slot,
                server_to_rpc_client,
                subscription_threads,
            },
            slot_receiver,
        )
    }

    fn start_subscription_threads(
        servers: &[(String, String)],
        server_to_slot: &DashMap<String, Slot>,
        _server_to_rpc_client: &DashMap<String, Arc<RpcClient>>,
        exit: &Arc<AtomicBool>,
        slot_sender: Sender<Slot>,
        cluster: String,
        region: String,
    ) -> Vec<JoinHandle<()>> {
        let highest_slot = Arc::new(Mutex::new(Slot::default()));

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
                let cluster = cluster.clone();
                let region = region.clone();

                Builder::new()
                    .name(format!("load_balance_subscription_thread-{ws_url_no_token}"))
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
                                                        "cluster" => &cluster,
                                                        "region" => &region,
                                                        "url" => ws_url_no_token,
                                                        ("slot", slot.slot, i64)
                                                );

                                                {
                                                    let mut highest_slot_l =
                                                        highest_slot.lock().unwrap();

                                                    if slot.slot > *highest_slot_l {
                                                        *highest_slot_l = slot.slot;
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
                                                if last_slot_update.elapsed().as_secs()
                                                    >= DISCONNECT_WEBSOCKET_TIMEOUT_S
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

    /// Returns the server with the highest slot URL
    pub fn rpc_client(&self) -> Arc<RpcClient> {
        let (highest_server, _) = self.get_highest_slot();

        self.server_to_rpc_client
            .get(&highest_server)
            .unwrap()
            .value()
            .to_owned()
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
