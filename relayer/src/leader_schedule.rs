use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use ed25519_dalek::PublicKey;
use solana_client::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;

// ToDo Implement this
pub struct LeaderScheduleCache {
    /// Maps slots to scheduled pubkey, used to index into the contact_infos map.
    schedules: Arc<RwLock<HashMap<Slot, Pubkey>>>,
    /// RPC Client
    client: RpcClient,
}

impl LeaderScheduleCache {
    // ToDo: Feed in rpc server address
    pub fn new() -> LeaderScheduleCache {
        LeaderScheduleCache {
            schedules: Arc::new(RwLock::new(HashMap::new())),
            client: RpcClient::new("http://localhost:8899".to_string())
        }
    }

    pub fn update_leader_cache(&self) -> () {
        let leader_schedule = self.client.get_leader_schedule(None).unwrap().unwrap();
        let mut schedules = self.schedules.write().unwrap();

        for (key, slots) in leader_schedule.iter() {
            for slot in slots.iter() {
                schedules.insert(*slot as Slot, key.parse().unwrap());
            }
        }
    }

    pub fn fetch_scheduled_validator(&self, slot: &Slot) -> Option<Pubkey> {
        let schedules = self.schedules.read().unwrap();
        let pk = schedules.get(slot).clone()?;
        Some(*pk)
    }

    pub fn is_validator_scheduled(&self, pk: Pubkey) -> bool {
        // ToDo: Implement this
        true
    }
}