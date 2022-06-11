use crossbeam_channel::RecvTimeoutError;
use log::{error, info};
use solana_client::pubsub_client::PubsubClient;
use solana_client::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::{sleep, Builder, JoinHandle};
use std::time::Duration;

pub struct LoadBalancer {
    // (http, websocket)
    server_to_slot: Arc<Mutex<HashMap<String, Slot>>>,
    server_to_rpc_client: Arc<Mutex<HashMap<String, Arc<RpcClient>>>>,
    subscription_threads: Vec<JoinHandle<()>>,
}

impl LoadBalancer {
    pub fn new(servers: &[(String, String)], exit: &Arc<AtomicBool>) -> LoadBalancer {
        let server_to_slot = HashMap::from_iter(servers.iter().map(|(_, ws)| (ws.clone(), 0)));
        let server_to_slot = Arc::new(Mutex::new(server_to_slot));

        let server_to_rpc_client = HashMap::from_iter(servers.iter().map(|(http, ws)| {
            // warm up the connection
            let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
                http,
                Duration::from_secs(5),
                CommitmentConfig {
                    commitment: CommitmentLevel::Processed,
                },
            ));
            if let Err(e) = rpc_client.get_slot() {
                error!("error warming up http: {} error: {}", http, e);
            }
            // store as ws instead of http so we can lookup by furthest ahead ws subscription
            (ws.clone(), rpc_client)
        }));
        let server_to_rpc_client = Arc::new(Mutex::new(server_to_rpc_client));

        let subscription_threads = Self::start_subscription_threads(
            servers.to_vec(),
            &server_to_slot,
            &server_to_rpc_client,
            exit,
        );
        LoadBalancer {
            server_to_slot,
            server_to_rpc_client,
            subscription_threads,
        }
    }

    fn start_subscription_threads(
        servers: Vec<(String, String)>,
        server_to_slot: &Arc<Mutex<HashMap<String, Slot>>>,
        _server_to_rpc_client: &Arc<Mutex<HashMap<String, Arc<RpcClient>>>>,
        exit: &Arc<AtomicBool>,
    ) -> Vec<JoinHandle<()>> {
        servers
            .iter()
            .map(|(_, websocket_url)| {
                let exit = exit.clone();
                let websocket_url = websocket_url.clone();
                let server_to_slot = server_to_slot.clone();

                Builder::new()
                    .name(format_args!("rpc-thread({})", websocket_url).to_string())
                    .spawn(move || {
                        while !exit.load(Ordering::Relaxed) {
                            info!("slot subscribing to url: {}", websocket_url);

                            match PubsubClient::slot_subscribe(&websocket_url) {
                                Ok(subscription) => {
                                    while !exit.load(Ordering::Relaxed) {
                                        match subscription
                                            .1
                                            .recv_timeout(Duration::from_millis(100))
                                        {
                                            Ok(slot) => {
                                                info!("url: {} slot: {:?}", websocket_url, slot);
                                                server_to_slot
                                                    .lock()
                                                    .unwrap()
                                                    .insert(websocket_url.clone(), slot.slot);
                                            }
                                            Err(RecvTimeoutError::Timeout) => {}
                                            Err(RecvTimeoutError::Disconnected) => {
                                                info!(
                                                    "slot subscribe disconnected url: {}",
                                                    websocket_url
                                                );
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "slot subscription error client: {}, error: {}",
                                        websocket_url, e
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

    /// Returns the highest URL
    pub fn rpc_client(&self) -> Arc<RpcClient> {
        let server_to_slot_l = self.server_to_slot.lock().unwrap();
        let server_to_rpc_client_l = self.server_to_rpc_client.lock().unwrap();
        let (highest_server, _) = server_to_slot_l
            .iter()
            .max_by(|(_, slot_a), (_, slot_b)| slot_a.cmp(slot_b))
            .unwrap();
        return server_to_rpc_client_l.get(highest_server).cloned().unwrap();
    }

    pub fn join(self) -> thread::Result<()> {
        for s in self.subscription_threads {
            s.join()?;
        }
        Ok(())
    }
}
