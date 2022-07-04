use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{sleep, spawn, JoinHandle},
    time::{Duration, SystemTime},
};

use crossbeam_channel::{unbounded, Receiver};
use jito_protos::{
    packet::PacketBatchList as PbPacketBatchList,
    shared::Header as PbHeader,
    validator_interface::{
        packet_stream_msg::Msg::{BatchList, Heartbeat},
        PacketStreamMsg,
    },
};
use log::{debug, error, info, warn};
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_sdk::{
    clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    pubkey::Pubkey,
};
use tokio::sync::mpsc::UnboundedSender;
use tonic::Status;

use crate::{schedule_cache::LeaderScheduleCache, server::ValidatorInterfaceServiceImpl};

type PacketsResultSender = UnboundedSender<Result<PacketStreamMsg, Status>>;

#[derive(Clone)]
pub struct PacketSubscription {
    tx: PacketsResultSender,
}

pub struct Router {
    pub(crate) packet_subs: Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub server: Option<ValidatorInterfaceServiceImpl>,
    current_slot: Arc<RwLock<Slot>>,
    look_ahead: u64,  // num leaders ahead to send packets to
    look_behind: u64, // num leaders behind to send packets to
    pub thread_handles: Option<Vec<JoinHandle<()>>>, // Option needed for join to take
}

impl Router {
    pub fn new(
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        slot_receiver: Receiver<Slot>,
        look_ahead: u64,
        look_behind: u64,
        public_ip: IpAddr,
        tpu_port: u16,
        tpu_fwd_port: u16,
        exit: Arc<AtomicBool>,
    ) -> Router {
        // **** Event Loops ****
        // Spawn Loops and save handles for joining
        let mut thread_handles = Vec::new();

        // Store Validator subscriptions
        let packet_subs = Arc::new(RwLock::new(HashMap::new()));

        // Send 2 Hz Heartbeat to all connected validators
        let heartbeat_hdl = Self::start_heartbeat_loop(packet_subs.clone(), exit.clone());
        thread_handles.push(heartbeat_hdl);

        // Receive slot updates and cache in Router
        let current_slot = Arc::new(RwLock::new(0_u64));
        let slot_loop_hdl =
            Self::start_slot_update_loop(current_slot.clone(), slot_receiver, exit.clone());
        thread_handles.push(slot_loop_hdl);

        // Receive disconnect messages (PubKey) and drop subscriptions
        let (client_disconnect_sender, client_disconnect_receiver) = unbounded();
        let disconnects_hdl =
            Self::handle_disconnects_loop(client_disconnect_receiver, packet_subs.clone(), exit);
        thread_handles.push(disconnects_hdl);

        // GRPC Server to handle incoming connection requests from validators
        let server = ValidatorInterfaceServiceImpl::new(
            packet_subs.clone(),
            client_disconnect_sender,
            public_ip,
            tpu_port,
            tpu_fwd_port,
        );

        let server = Some(server);
        let thread_handles = Some(thread_handles);

        Router {
            packet_subs,
            leader_schedule_cache,
            server,
            current_slot,
            look_ahead,
            look_behind,
            thread_handles,
        }
    }

    // pub fn join(&mut self) {
    //     let handles = self.thread_handles.take().unwrap();
    //     for hdl in handles {
    //         hdl.join().expect("task panicked");
    //     }
    // }

    fn start_heartbeat_loop(
        packet_subs: Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        info!("Started Heartbeat");
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                let failed_heartbeats = Self::send_heartbeat(&packet_subs);
                Self::disconnect(&packet_subs, &failed_heartbeats);
                sleep(Duration::from_millis(500));
            }
        })
    }

    fn start_slot_update_loop(
        current_slot: Arc<RwLock<Slot>>,
        slot_receiver: Receiver<Slot>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                match slot_receiver.recv() {
                    Ok(slot) => {
                        *current_slot.write().unwrap() = slot;
                    }
                    Err(_) => {
                        error!("error receiving slot");
                        break;
                    }
                }
            }
        })
    }

    // listen for client disconnects and remove from subscriptions map
    pub fn handle_disconnects_loop(
        rx: Receiver<Pubkey>,
        packet_subs: Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                match rx.recv() {
                    Ok(pk) => {
                        debug!("client [pk={}] disconnected", pk);
                        Self::disconnect(&packet_subs, &[pk]);
                    }
                    Err(_) => {
                        warn!("closed connection channel disconnected");
                        break;
                    }
                }
            }
        })
    }

    /// Sends  heartbeats to all active subscribers regardless of leader schedule.
    pub fn send_heartbeat(
        packet_subs: &Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
    ) -> Vec<Pubkey> {
        let active_subscriptions = packet_subs.read().unwrap().clone();
        let mut failed_subscriptions = Vec::new();
        let ts = prost_types::Timestamp::from(SystemTime::now());
        let header = PbHeader { ts: Some(ts) };
        for (pk, subscription) in active_subscriptions.iter() {
            if let Err(e) = subscription.tx.send(Ok(PacketStreamMsg {
                msg: Some(Heartbeat(header.clone())),
            })) {
                warn!("error sending heartbeat to subscriber [{}]", e);
                datapoint_info!(
                    "validator_subscription",
                    ("subscriber", pk.to_string(), String),
                    ("heartbeat_error", 1, i64)
                );
                failed_subscriptions.push(*pk);
            } else {
                debug!("sent heartbeat to subscriber [{}]", pk);
                datapoint_info!(
                    "validator_subscription",
                    ("subscriber", pk.to_string(), String),
                    ("heartbeat", 1, i64)
                );
            }
        }
        failed_subscriptions
    }

    /// returns true if subscription was added successfully, false if the given pubkey is already connected
    pub(crate) fn add_packet_subscription(
        packet_subs: Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
        pk: &Pubkey,
        tx: UnboundedSender<Result<PacketStreamMsg, Status>>,
    ) -> bool {
        let mut active_subs = packet_subs.write().unwrap();

        if active_subs.contains_key(pk) {
            datapoint_warn!(
                "validator_interface_add_packet_subscription",
                ("subscriber", pk.to_string(), String),
                ("add_packet_subscription_error", 1, i64)
            );
            false
        } else {
            active_subs.insert(Pubkey::new(&pk.to_bytes()), PacketSubscription { tx });
            info!(
                "validator_interface_add_packet_subscription - Subscriber: {}",
                pk.to_string()
            );

            datapoint_info!(
                "validator_interface_add_packet_subscription",
                ("subscriber", pk.to_string(), String),
                ("added", 1, i64),
                ("num_subs", active_subs.keys().len(), i64)
            );
            true
        }
    }

    pub(crate) fn disconnect(
        packet_subs: &Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
        keys: &[Pubkey],
    ) {
        if keys.is_empty() {
            return;
        }
        Self::remove_packet_subscription(packet_subs, keys);
    }

    // ToDo (JL): Why is this function needed separately from disconnect?
    pub(crate) fn remove_packet_subscription(
        packet_subs: &Arc<RwLock<HashMap<Pubkey, PacketSubscription>>>,
        keys: &[Pubkey],
    ) {
        if keys.is_empty() {
            return;
        }
        let mut active_subs = packet_subs.write().unwrap();
        for pk in keys {
            active_subs.remove(pk);
            datapoint_info!(
                "validator_interface_remove_packet_subscription",
                ("subscriber", pk.to_string(), String),
                ("removed", 1, i64),
                ("num_subs", active_subs.keys().len(), i64)
            );
        }
    }

    /// Sends packet streams to eligible connections over TCP.
    /// Eligible connections are ones where the `Leader` (connected node) has a slot scheduled
    /// between `start_slot` & `end_slot`.
    ///
    /// returns a tuple where:
    ///     tuple.0 = list of connection ids (pubkey) where an error was encountered on stream attempt
    ///     tuple.1 = a set of slots that were streamed for
    pub fn stream_batch_list(
        &self,
        batch_list: &PbPacketBatchList,
    ) -> (Vec<Pubkey>, HashSet<Slot>) {
        let mut failed_stream_pks: Vec<Pubkey> = Vec::new();
        let mut slots_sent: HashSet<Slot> = HashSet::new();

        let active_subscriptions = self.packet_subs.read().unwrap();
        if active_subscriptions.is_empty() {
            warn!("stream_batch_list: No Active Subscriptions");
            return (failed_stream_pks, slots_sent);
        }

        let current_slot = *self.current_slot.read().unwrap();
        let start_slot = current_slot - (NUM_CONSECUTIVE_LEADER_SLOTS * self.look_behind);
        let end_slot = current_slot + (NUM_CONSECUTIVE_LEADER_SLOTS * self.look_ahead);

        let validators_to_send =
            Self::validators_in_slot_range(start_slot, end_slot, &self.leader_schedule_cache);
        let iter = active_subscriptions.iter();
        for (pk, subscription) in iter {
            let slot_to_send = validators_to_send.get(pk);
            debug!("Slot to Send: {:?}", slot_to_send);
            if let Some(slot) = slot_to_send {
                if let Err(e) = subscription.tx.send(Ok(PacketStreamMsg {
                    msg: Some(BatchList(batch_list.clone())),
                })) {
                    datapoint_warn!(
                        "validator_interface_stream_batch_list",
                        ("subscriber", pk.to_string(), String),
                        ("batch_stream_error", 1, i64),
                        ("error", e.to_string(), String)
                    );
                    failed_stream_pks.push(*pk);
                } else {
                    debug!("slot sent {}", slot);
                    datapoint_info!(
                        "validator_interface_stream_batch_list",
                        ("subscriber", pk.to_string(), String),
                        ("batches_streamed", batch_list.batch_list.len(), i64)
                    );
                    slots_sent.insert(*slot);
                }
            }
        }

        (failed_stream_pks, slots_sent)
    }

    fn validators_in_slot_range(
        start_slot: Slot,
        end_slot: Slot,
        leader_schedule_cache: &LeaderScheduleCache,
    ) -> HashMap<Pubkey, Slot> {
        let n_leader_slots = NUM_CONSECUTIVE_LEADER_SLOTS as usize;
        let mut validators_to_send = HashMap::new();
        for slot in (start_slot..end_slot).step_by(n_leader_slots) {
            if let Some(pk) = leader_schedule_cache.fetch_scheduled_validator(&slot) {
                validators_to_send.insert(pk, slot);
            }
        }
        info!(
            "validators_in_slot_range: {}  -  {},   val: {:?}",
            start_slot, end_slot, validators_to_send
        );

        validators_to_send
    }
}
