use std::collections::{hash_map::Entry, HashMap};

use crate::server::ICID;

pub(crate) type ReasmID = (ICID, u64); // ICID, StreamID

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ReasmState {
    Free,
    Busy(ReasmID),
    Pub,
}

#[derive(Clone)]
pub(crate) struct ReasmSlot {
    state: ReasmState,
    pub(crate) sz: u16,
    pub(crate) data: [u8; crate::buf::TXN_MAX_SZ],
}

impl Default for ReasmSlot {
    fn default() -> Self {
        Self {
            state: ReasmState::Free,
            data: [0; crate::buf::TXN_MAX_SZ],
            sz: 0,
        }
    }
}

impl ReasmSlot {
    pub(crate) fn append(&mut self, data: &[u8]) -> bool {
        let new_sz = self.sz + data.len() as u16;
        if new_sz <= crate::buf::TXN_MAX_SZ as u16 {
            self.data[self.sz as usize..new_sz as usize].copy_from_slice(data);
            self.sz = new_sz;
            true
        } else {
            false
        }
    }
}

/// Reassembles transaction data arriving from stream fragments
pub(crate) struct Reasm {
    slots: Vec<ReasmSlot>,
    idx: usize,
    by_stream: HashMap<ReasmID, u16>,
}

impl Reasm {
    pub(crate) fn new(slot_cnt: usize) -> Reasm {
        Self {
            slots: vec![ReasmSlot::default(); slot_cnt],
            idx: 0usize,
            by_stream: HashMap::with_capacity(slot_cnt),
        }
    }

    pub(crate) fn acquire(&mut self, id: ReasmID) -> (&mut ReasmSlot, Option<ReasmID>) {
        let slot_cnt = self.slots.len();
        let (next_slot, evicted_id) = match self.by_stream.entry(id) {
            Entry::Occupied(entry) => {
                let slot: &mut ReasmSlot = &mut self.slots[*entry.get() as usize];
                assert!(slot.state == ReasmState::Busy(id));
                (slot, None)
            }
            Entry::Vacant(entry) => {
                let next_slot_idx = self.idx;
                let next_slot = &mut self.slots[next_slot_idx];
                let evicted_id = if let ReasmState::Busy(evicted_id) = next_slot.state {
                    Some(evicted_id)
                } else {
                    None
                };
                next_slot.state = ReasmState::Busy(id);
                next_slot.sz = 0;
                entry.insert(next_slot_idx as u16);
                self.idx = (next_slot_idx + 1) % slot_cnt;
                (next_slot, evicted_id)
            }
        };
        if let Some(id) = evicted_id {
            self.by_stream.remove(&id);
        }
        (next_slot, evicted_id)
    }

    pub(crate) fn finish(&mut self, id: ReasmID) {
        let slot_idx = self.by_stream.remove(&id).unwrap() as usize;
        let slot = &mut self.slots[slot_idx];
        if let ReasmState::Busy(reasm_id) = slot.state {
            slot.state = ReasmState::Pub;
            self.by_stream.remove(&reasm_id);
        }
        slot.state = ReasmState::Pub;
    }
}
