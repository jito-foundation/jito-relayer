use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{self, sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use jito_rpc::load_balancer::LoadBalancer;
use solana_client::client_error;
use solana_sdk::pubkey::Pubkey;
use solana_streamer::streamer::StakedNodes;

const IP_TO_STAKE_REFRESH_DURATION: Duration = Duration::from_secs(5);

pub struct StakedNodesUpdaterService {
    thread_hdl: JoinHandle<()>,
}

impl StakedNodesUpdaterService {
    pub fn new(
        exit: Arc<AtomicBool>,
        rpc_load_balancer: Arc<LoadBalancer>,
        shared_staked_nodes: Arc<RwLock<StakedNodes>>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("staked_nodes_updater_thread".to_string())
            .spawn(move || {
                let mut last_stakes = Instant::now();
                while !exit.load(Ordering::Relaxed) {
                    let mut stake_map = Arc::new(HashMap::new());
                    if let Ok(true) = Self::try_refresh_ip_to_stake(
                        &mut last_stakes,
                        &mut stake_map,
                        &rpc_load_balancer,
                    ) {
                        let shared = StakedNodes::new(stake_map, HashMap::default());
                        *shared_staked_nodes.write().unwrap() = shared;
                    }
                }
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn try_refresh_ip_to_stake(
        last_stakes: &mut Instant,
        pubkey_stake_map: &mut Arc<HashMap<Pubkey, u64>>,
        rpc_load_balancer: &Arc<LoadBalancer>,
    ) -> client_error::Result<bool> {
        if last_stakes.elapsed() > IP_TO_STAKE_REFRESH_DURATION {
            let client = rpc_load_balancer.rpc_client();
            let vote_accounts = client.get_vote_accounts()?;

            *pubkey_stake_map = Arc::new(
                vote_accounts
                    .current
                    .iter()
                    .chain(vote_accounts.delinquent.iter())
                    .filter_map(|vote_account| {
                        Some((
                            Pubkey::from_str(&vote_account.node_pubkey).ok()?,
                            vote_account.activated_stake,
                        ))
                    })
                    .collect(),
            );

            *last_stakes = Instant::now();
            Ok(true)
        } else {
            sleep(Duration::from_secs(1));
            Ok(false)
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
