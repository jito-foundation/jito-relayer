use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
    thread::{sleep, Builder, JoinHandle},
    time::Duration,
};

use jito_rpc::load_balancer::LoadBalancer;
use log::{debug, error};
use solana_sdk::{clock::Slot, pubkey::Pubkey};

pub struct LeaderScheduleCacheUpdater {
    /// Maps slots to scheduled pubkey
    schedules: Arc<RwLock<HashMap<Slot, Pubkey>>>,

    /// Refreshes leader schedule
    refresh_thread: JoinHandle<()>,
}

#[derive(Clone)]
pub struct LeaderScheduleUpdatingHandle {
    schedule: Arc<RwLock<HashMap<Slot, Pubkey>>>,
}

/// Access handle to a constantly updating leader schedule
impl LeaderScheduleUpdatingHandle {
    pub fn new(schedule: Arc<RwLock<HashMap<Slot, Pubkey>>>) -> LeaderScheduleUpdatingHandle {
        LeaderScheduleUpdatingHandle { schedule }
    }

    pub fn leader_for_slot(&self, slot: &Slot) -> Option<Pubkey> {
        self.schedule.read().unwrap().get(slot).cloned()
    }

    pub fn leaders_for_slots(&self, slots: &[Slot]) -> Vec<Pubkey> {
        let schedule = self.schedule.read().unwrap();
        slots
            .iter()
            .filter_map(|s| schedule.get(s).cloned())
            .collect()
    }

    pub fn is_scheduled_validator(&self, pubkey: &Pubkey) -> bool {
        self.schedule
            .read()
            .unwrap()
            .iter()
            .any(|(_, scheduled_pubkey)| scheduled_pubkey == pubkey)
    }
}

impl LeaderScheduleCacheUpdater {
    pub fn new(
        load_balancer: &Arc<Mutex<LoadBalancer>>,
        exit: Arc<AtomicBool>,
    ) -> LeaderScheduleCacheUpdater {
        let schedules = Arc::new(RwLock::new(HashMap::new()));
        let refresh_thread = Self::refresh_thread(schedules.clone(), load_balancer.clone(), exit);
        LeaderScheduleCacheUpdater {
            schedules,
            refresh_thread,
        }
    }

    /// Gets a handle to a constantly updating leader schedule handler
    pub fn handle(&self) -> LeaderScheduleUpdatingHandle {
        LeaderScheduleUpdatingHandle::new(self.schedules.clone())
    }

    pub fn join(self) -> thread::Result<()> {
        self.refresh_thread.join()
    }

    fn refresh_thread(
        schedule: Arc<RwLock<HashMap<Slot, Pubkey>>>,
        load_balancer: Arc<Mutex<LoadBalancer>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("leader-schedule-refresh".into())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    Self::update_leader_cache(&load_balancer, &schedule);
                    sleep(Duration::from_secs(10));
                }
            })
            .unwrap()
    }

    pub fn update_leader_cache(
        load_balancer: &Arc<Mutex<LoadBalancer>>,
        schedule: &Arc<RwLock<HashMap<Slot, Pubkey>>>,
    ) {
        let rpc_client = load_balancer.lock().unwrap().rpc_client();

        if let Ok(epoch_info) = rpc_client.get_epoch_info() {
            if let Ok(Some(leader_schedule)) = rpc_client.get_leader_schedule(None) {
                let epoch_offset = epoch_info.absolute_slot - epoch_info.slot_index;

                debug!("Got Leader Schedule. Length = {}", leader_schedule.len());

                let mut schedule = schedule.write().unwrap();

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
            } else {
                error!("Couldn't Get Leader Schedule Update from RPC!!!")
            };
        } else {
            error!("Couldn't Get Epoch Info from RPC!!!")
        };
    }
}
