use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    thread::{sleep, Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use log::{error, info};
use solana_client::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use solana_metrics::datapoint_info;
use solana_sdk::{
    clock::Slot,
    commitment_config::{CommitmentConfig, CommitmentLevel},
};

pub struct LoadBalancer {
    // (http, websocket)
    server_to_slot: Arc<Mutex<HashMap<String, Slot>>>,
    server_to_rpc_client: Arc<Mutex<HashMap<String, Arc<RpcClient>>>>,
    subscription_threads: Vec<JoinHandle<()>>,
}

impl LoadBalancer {
    pub fn new(
        servers: &[(String, String)],
        exit: &Arc<AtomicBool>,
    ) -> (LoadBalancer, Receiver<Slot>) {
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

        let (slot_sender, slot_receiver) = unbounded();
        let subscription_threads = Self::start_subscription_threads(
            servers.to_vec(),
            &server_to_slot,
            &server_to_rpc_client,
            exit,
            slot_sender,
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
        servers: Vec<(String, String)>,
        server_to_slot: &Arc<Mutex<HashMap<String, Slot>>>,
        _server_to_rpc_client: &Arc<Mutex<HashMap<String, Arc<RpcClient>>>>,
        exit: &Arc<AtomicBool>,
        slot_sender: Sender<Slot>,
    ) -> Vec<JoinHandle<()>> {
        let highest_slot = Arc::new(Mutex::new(Slot::default()));

        servers
            .iter()
            .map(|(_, websocket_url)| {
                let exit = exit.clone();
                let websocket_url = websocket_url.clone();
                let server_to_slot = server_to_slot.clone();
                let slot_sender = slot_sender.clone();
                let highest_slot = highest_slot.clone();

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
                                                {
                                                    datapoint_info!(
                                                        "rpc_load_balancer-slot_count",
                                                        "url" => websocket_url,
                                                        ("slot", slot.slot, i64)
                                                    );

                                                    let mut highest_slot_l =
                                                        highest_slot.lock().unwrap();

                                                    if slot.slot > *highest_slot_l {
                                                        *highest_slot_l = slot.slot;
                                                        if let Err(e) = slot_sender.send(slot.slot)
                                                        {
                                                            error!("error sending slot: {}", e);
                                                            break;
                                                        }
                                                    }
                                                }
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
                                        "slot subscription error client: {}, error: {:?}",
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

    pub fn get_highest_slot(&self) -> Slot {
        let server_to_slot_l = self.server_to_slot.lock().unwrap();
        let (_, highest_slot) = server_to_slot_l
            .iter()
            .max_by(|(_, slot_a), (_, slot_b)| slot_a.cmp(slot_b))
            .unwrap();
        *highest_slot
    }

    pub fn join(self) -> thread::Result<()> {
        for s in self.subscription_threads {
            s.join()?;
        }
        Ok(())
    }
}
