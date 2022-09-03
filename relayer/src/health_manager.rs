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
use log::error;
use solana_metrics::datapoint_info;
use solana_sdk::clock::Slot;

#[derive(PartialEq, Eq)]
pub enum HealthState {
    Unhealthy,
    Healthy,
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
        missing_slot_unhealthy_duration: Duration,
        exit: &Arc<AtomicBool>,
    ) -> HealthManager {
        let health_state = Arc::new(RwLock::new(HealthState::Unhealthy));
        HealthManager {
            state: health_state.clone(),
            manager_thread: Self::start_manager_thread(
                slot_receiver,
                slot_sender,
                missing_slot_unhealthy_duration,
                exit,
                health_state,
            ),
        }
    }

    pub fn start_manager_thread(
        slot_receiver: Receiver<Slot>,
        slot_sender: Sender<Slot>,
        missing_slot_unhealthy_duration: Duration,
        exit: &Arc<AtomicBool>,
        health_state: Arc<RwLock<HealthState>>,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        Builder::new()
            .name("health-manager".to_string())
            .spawn(move || {
                let mut last_update = Instant::now();
                let check_and_metrics_tick = tick(missing_slot_unhealthy_duration / 2);

                while !exit.load(Ordering::Relaxed) {
                    select! {
                        recv(check_and_metrics_tick) -> _ => {
                            let is_err = last_update.elapsed() > missing_slot_unhealthy_duration;
                            *health_state.write().unwrap() = if is_err { HealthState::Unhealthy } else { HealthState::Healthy };
                            datapoint_info!(
                                "relayer-health-state",
                                ("health", is_err, i64)
                            );
                        }
                        recv(slot_receiver) -> maybe_slot => {
                            if maybe_slot.is_err() {
                                error!("error receiving slot, exiting");
                                break;
                            }
                            let slot = maybe_slot.unwrap();
                            if slot_sender.send(slot).is_err() {
                                error!("error forwarding slot, exiting");
                                break;
                            }
                            last_update = Instant::now();
                        }
                    }
                }
            })
            .unwrap()
    }

    /// Return a handle to the health manager state
    pub fn handle(&self) -> Arc<RwLock<HealthState>> {
        self.state.clone()
    }

    pub fn join(self) -> thread::Result<()> {
        self.manager_thread.join()
    }
}
