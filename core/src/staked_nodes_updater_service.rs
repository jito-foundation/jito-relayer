use std::{
    collections::HashMap,
    net::IpAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    thread::{self, sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use jito_rpc::load_balancer::LoadBalancer;
use solana_client::{client_error, rpc_response::RpcContactInfo};
use solana_sdk::pubkey::Pubkey;
use solana_streamer::streamer::StakedNodes;

const IP_TO_STAKE_REFRESH_DURATION: Duration = Duration::from_secs(5);

pub struct StakedNodesUpdaterService {
    thread_hdl: JoinHandle<()>,
}

impl StakedNodesUpdaterService {
    pub fn new(
        exit: Arc<AtomicBool>,
        rpc_load_balancer: Arc<Mutex<LoadBalancer>>,
        shared_staked_nodes: Arc<RwLock<StakedNodes>>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("sol-sn-updater".to_string())
            .spawn(move || {
                let mut last_stakes = Instant::now();
                while !exit.load(Ordering::Relaxed) {
                    let mut new_ip_to_stake = HashMap::new();
                    let mut total_stake = 0;
                    let mut new_pubkey_stake_map: HashMap<Pubkey, u64> = HashMap::new();
                    if let Ok(true) = Self::try_refresh_ip_to_stake(
                        &mut last_stakes,
                        &mut new_ip_to_stake,
                        &mut new_pubkey_stake_map,
                        &mut total_stake,
                        &rpc_load_balancer,
                    ) {
                        let mut shared = shared_staked_nodes.write().unwrap();
                        shared.total_stake = total_stake;
                        shared.ip_stake_map = new_ip_to_stake;
                        shared.pubkey_stake_map = new_pubkey_stake_map;
                    }
                }
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn try_refresh_ip_to_stake(
        last_stakes: &mut Instant,
        ip_to_stake: &mut HashMap<IpAddr, u64>,
        pubkey_stake_map: &mut HashMap<Pubkey, u64>,
        total_stake: &mut u64,
        rpc_load_balancer: &Arc<Mutex<LoadBalancer>>,
    ) -> client_error::Result<bool> {
        if last_stakes.elapsed() > IP_TO_STAKE_REFRESH_DURATION {
            let client = rpc_load_balancer.lock().unwrap().rpc_client();
            let vote_accounts = client.get_vote_accounts()?;
            let cluster_info: HashMap<String, RpcContactInfo> = client
                .get_cluster_nodes()?
                .into_iter()
                .map(|contact_info| (contact_info.pubkey.to_string(), contact_info))
                .collect();
            *total_stake = vote_accounts
                .current
                .iter()
                .chain(vote_accounts.delinquent.iter())
                .map(|acc| acc.activated_stake)
                .sum();
            *ip_to_stake = vote_accounts
                .current
                .iter()
                .chain(vote_accounts.delinquent.iter())
                .filter_map(
                    |vote_account| match cluster_info.get(&vote_account.node_pubkey) {
                        None => None,
                        Some(node_info) => {
                            Some((node_info.gossip?.ip(), vote_account.activated_stake))
                        }
                    },
                )
                .collect();
            *pubkey_stake_map = vote_accounts
                .current
                .iter()
                .chain(vote_accounts.delinquent.iter())
                .filter_map(|vote_account| {
                    Some((
                        Pubkey::from_str(&vote_account.node_pubkey).ok()?,
                        vote_account.activated_stake,
                    ))
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
