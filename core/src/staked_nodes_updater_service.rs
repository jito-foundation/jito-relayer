use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    thread::{self, sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use jito_rpc::load_balancer::LoadBalancer;
use log::error;
use solana_client::{client_error, rpc_response::RpcContactInfo};

const IP_TO_STAKE_REFRESH_DURATION: Duration = Duration::from_secs(5);

pub struct StakedNodesUpdaterService {
    thread_hdl: JoinHandle<()>,
}

impl StakedNodesUpdaterService {
    pub fn new(
        exit: Arc<AtomicBool>,
        shared_staked_nodes: Arc<RwLock<HashMap<IpAddr, u64>>>,
        rpc_load_balancer: &Arc<Mutex<LoadBalancer>>,
    ) -> Self {
        let rpc_load_balancer = rpc_load_balancer.clone();
        let thread_hdl = Builder::new()
            .name("sol-sn-updater".to_string())
            .spawn(move || {
                let mut last_stakes = Instant::now();
                while !exit.load(Ordering::Relaxed) {
                    let mut new_ip_to_stake = HashMap::new();
                    match Self::try_refresh_ip_to_stake(
                        &mut last_stakes,
                        &mut new_ip_to_stake,
                        &rpc_load_balancer,
                    ) {
                        Ok(true) => {
                            let mut shared = shared_staked_nodes.write().unwrap();
                            *shared = new_ip_to_stake;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            error!("error updating ip to stake mapping: {}", e);
                        }
                    }
                }
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn try_refresh_ip_to_stake(
        last_stakes: &mut Instant,
        ip_to_stake: &mut HashMap<IpAddr, u64>,
        rpc_load_balancer: &Arc<Mutex<LoadBalancer>>,
    ) -> client_error::Result<bool> {
        if last_stakes.elapsed() > IP_TO_STAKE_REFRESH_DURATION {
            let client = rpc_load_balancer.lock().unwrap().rpc_client();
            let vote_accounts = client.get_vote_accounts()?;
            // maps node pubkey to RpcContactInfo
            let cluster_nodes: HashMap<String, RpcContactInfo> = client
                .get_cluster_nodes()?
                .into_iter()
                .filter_map(|contact_info| Some((contact_info.pubkey.to_string(), contact_info)))
                .collect();

            *ip_to_stake = vote_accounts
                .current
                .iter()
                .filter_map(|a| {
                    if let Some(acc) = cluster_nodes.get(&a.node_pubkey) {
                        Some((acc.gossip?.ip(), a.activated_stake))
                    } else {
                        error!("error getting ip for account {}", a.node_pubkey);
                        None
                    }
                })
                .collect();
            *last_stakes = Instant::now();
            Ok(true)
        } else {
            sleep(Duration::from_millis(1));
            Ok(false)
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
