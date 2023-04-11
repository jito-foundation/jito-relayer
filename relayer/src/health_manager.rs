use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{select, tick, Receiver, Sender};
use solana_metrics::datapoint_info;
use solana_sdk::clock::Slot;

#[derive(PartialEq, Eq, Copy, Clone)]
pub enum HealthState {
    Unhealthy = 0,
    Healthy = 1,
}

pub struct HealthManager {
    state: Arc<RwLock<HealthState>>,
    manager_thread: JoinHandle<()>,
}

/// Manages health status of the relayer. Reports to metrics and other parts of system health
/// status so they can react accordingly.
impl HealthManager {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        slot_sender: Sender<Slot>,
        missing_slot_unhealthy_threshold: Duration,
        exit: Arc<AtomicBool>,
    ) -> HealthManager {
        let health_state = Arc::new(RwLock::new(HealthState::Unhealthy));
        HealthManager {
            state: health_state.clone(),
            manager_thread: Builder::new()
                .name("health_manager".to_string())
                .spawn(move || {
                    let mut last_update = Instant::now();
                    let mut slot_sender_max_len = 0usize;
                    let channel_len_tick = tick(Duration::from_secs(5));
                    let check_and_metrics_tick = tick(missing_slot_unhealthy_threshold / 2);

                    while !exit.load(Ordering::Relaxed) {
                        select! {
                            recv(check_and_metrics_tick) -> _ => {
                                let new_health_state =
                                    match last_update.elapsed() <= missing_slot_unhealthy_threshold {
                                        true => HealthState::Healthy,
                                        false => HealthState::Unhealthy,
                                    };
                                *health_state.write().unwrap() = new_health_state;
                                datapoint_info!(
                                    "relayer-health-state",
                                    ("health_state", new_health_state, i64)
                                );
                            }
                            recv(slot_receiver) -> maybe_slot => {
                                let slot = maybe_slot.expect("error receiving slot, exiting");
                                slot_sender.send(slot).expect("error forwarding slot, exiting");
                                last_update = Instant::now();
                            }
                            recv(channel_len_tick) -> _ => {
                                datapoint_info!(
                                    "health_manager-channel_stats",
                                    ("slot_sender_len", slot_sender_max_len, i64),
                                    ("slot_sender_capacity", slot_sender.capacity().unwrap(), i64),
                                );
                                slot_sender_max_len = 0;
                            }
                        }
                        slot_sender_max_len = std::cmp::max(slot_sender_max_len, slot_sender.len());
                    }
                })
                .unwrap(),
        }
    }

    /// Return a handle to the health manager state
    pub fn handle(&self) -> Arc<RwLock<HealthState>> {
        self.state.clone()
    }

    pub fn join(self) -> thread::Result<()> {
        self.manager_thread.join()
    }
}
