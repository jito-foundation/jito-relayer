use std::{
    collections::HashSet,
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
};

use jito_rpc::load_balancer::LoadBalancer;
use log::{error, info};
use solana_sdk::{clock::Slot, pubkey::Pubkey};

pub struct LeaderScheduleCache {
    /// Maps slots to scheduled pubkey, used to index into the contact_infos map.
    schedule: Arc<RwLock<HashSet<Slot>>>,
    /// RPC Client
    rpc: Arc<Mutex<LoadBalancer>>,
    /// Validator Identity
    identity: Option<String>,
}

impl LeaderScheduleCache {
    pub fn new(rpc: &Arc<Mutex<LoadBalancer>>, identity: Option<String>) -> LeaderScheduleCache {
        LeaderScheduleCache {
            schedule: Arc::new(RwLock::new(HashSet::new())),
            rpc: rpc.clone(),
            identity,
        }
    }

    pub fn set_identity(&mut self, pk: &str) {
        self.identity = Some(String::from(pk));
        info!("Identity Set to {}", self.identity.as_ref().unwrap());
    }

    pub fn update_leader_cache(&self) -> () {
        if self.identity == None {
            return;
        }

        // let cfg = RpcLeaderScheduleConfig {
        //     identity: self.identity.clone(),
        //     commitment: None,
        // };

        let rpc_client = self.rpc.lock().unwrap().rpc_client();

        // ToDo: Should the rpc client lock be dropped manually?
        if let Ok(epoch_info) = rpc_client.get_epoch_info() {
            if let Ok(Some(leader_schedule)) = rpc_client.get_leader_schedule(None) {
                let epoch_offset = epoch_info.absolute_slot - epoch_info.slot_index;

                info!("Got Leader Schedule. Length = {}", leader_schedule.len());

                let mut schedule = self.schedule.write().unwrap();

                // Remove Old Slots
                schedule.retain(|s| *s >= epoch_info.absolute_slot);

                // Add New Slots
                if let Some(slots) = leader_schedule.get(&self.identity.as_ref().unwrap().clone()) {
                    for sl in (*slots).iter() {
                        let slot = *sl as Slot + epoch_offset;
                        if slot > epoch_info.absolute_slot {
                            schedule.insert(slot);
                        }
                    }
                };
            } else {
                error!("Couldn't Get Leader Schedule Update from RPC!!!")
            };
        } else {
            error!("Couldn't Get Leader Schedule Update from RPC!!!")
        };
    }

    pub fn fetch_scheduled_validator(&self, slot: &Slot) -> Option<Pubkey> {
        let schedule = self.schedule.read().unwrap();

        // ToDo: Write this better
        return if let Some(_) = schedule.get(slot) {
            if let Ok(pk) = Pubkey::from_str(&self.identity.as_ref()?) {
                Some(pk)
            } else {
                None
            }
        } else {
            None
        };
    }

    pub fn is_validator_scheduled(&self, _pk: Pubkey) -> bool {
        // Is the maximum scheduled slot bigger than the current slot
        if let Some(max_sched) = self.schedule.read().unwrap().iter().max() {
            *max_sched > self.rpc.lock().unwrap().get_highest_slot()
        } else {
            false
        }
    }
}
