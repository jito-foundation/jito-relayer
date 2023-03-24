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
        exit: Arc<AtomicBool>,
        cluster: String,
        region: String,
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
                cluster,
                region,
            ),
        }
    }

    pub fn start_manager_thread(
        slot_receiver: Receiver<Slot>,
        slot_sender: Sender<Slot>,
        missing_slot_unhealthy_duration: Duration,
        exit: Arc<AtomicBool>,
        health_state: Arc<RwLock<HealthState>>,
        cluster: String,
        region: String,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("health-manager".to_string())
            .spawn(move || {
                let mut last_update = Instant::now();
                let channel_len_tick = tick(Duration::from_secs(5));
                let check_and_metrics_tick = tick(missing_slot_unhealthy_duration / 2);

                while !exit.load(Ordering::Relaxed) {
                    select! {
                        recv(check_and_metrics_tick) -> _ => {
                            let is_err = last_update.elapsed() > missing_slot_unhealthy_duration;
                            let new_health_state = if is_err { HealthState::Unhealthy } else { HealthState::Healthy };
                            *health_state.write().unwrap() = new_health_state;
                            datapoint_info!(
                                "relayer-health-state",
                                "cluster" => cluster,
                                "region" => region,
                                ("health_state", new_health_state as i64, i64)
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
                                ("slot_sender-len", slot_sender.len(), i64),
                            );
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
