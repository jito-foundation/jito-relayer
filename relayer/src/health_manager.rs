use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{select, tick, Receiver};
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
                                    ("health_state", new_health_state, i64),
                                    ("slot_receiver_len", slot_receiver.len(), i64),
                                );
                            }
                            recv(slot_receiver) -> _maybe_slot => {
                                last_update = Instant::now();
                            }
                        }
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
