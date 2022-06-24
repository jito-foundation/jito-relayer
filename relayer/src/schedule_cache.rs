use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
};

use jito_rpc::load_balancer::LoadBalancer;
use log::{debug, error};
use solana_sdk::{clock::Slot, pubkey::Pubkey};

pub struct LeaderScheduleCache {
    /// Maps slots to scheduled pubkey
    schedules: RwLock<HashMap<Slot, Pubkey>>,
    /// RPC Client
    load_balancer: Arc<Mutex<LoadBalancer>>,
}

impl LeaderScheduleCache {
    pub fn new(rpc: &Arc<Mutex<LoadBalancer>>) -> LeaderScheduleCache {
        LeaderScheduleCache {
            schedules: RwLock::new(HashMap::new()),
            load_balancer: rpc.clone(),
        }
    }

    pub fn update_leader_cache(&self) {
        debug!("Update Leader Cache !!!");

        let rpc_client = self.load_balancer.lock().unwrap().rpc_client();

        if let Ok(epoch_info) = rpc_client.get_epoch_info() {
            if let Ok(Some(leader_schedule)) = rpc_client.get_leader_schedule(None) {
                let epoch_offset = epoch_info.absolute_slot - epoch_info.slot_index;

                debug!("Got Leader Schedule. Length = {}", leader_schedule.len());

                let mut schedule = self.schedules.write().unwrap();

                // Remove Old Slots
                schedule.retain(|s, _| *s >= epoch_info.absolute_slot);

                // Add New Slots
                for (pk_str, slots) in leader_schedule.iter() {
                    for slot in slots.iter() {
                        if let Ok(pubkey) = Pubkey::from_str(pk_str) {
                            schedule.insert(*slot as u64 + epoch_offset, pubkey);
                        }
                    }
                }
                //Todo: Add Metrics Here
            } else {
                error!("Couldn't Get Leader Schedule Update from RPC!!!")
            };
        } else {
            error!("Couldn't Get Epoch Info from RPC!!!")
        };
    }

    pub fn fetch_scheduled_validator(&self, slot: &Slot) -> Option<Pubkey> {
        Some(*self.schedules.read().unwrap().get(slot)?)
    }

    pub fn is_validator_scheduled(&self, pubkey: Pubkey) -> bool {
        debug!("Is Validator Scheduled Called for {}", pubkey.to_string());
        debug!("Schedule {:?}", self.schedules.read().unwrap());

        for (_, pk) in self.schedules.read().unwrap().iter() {
            if *pk == pubkey {
                return true;
            }
        }
        false
    }
}
