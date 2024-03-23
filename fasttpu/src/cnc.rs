use std::sync::atomic::AtomicU32;

#[derive(Default)]
pub struct Cnc {
    pub state: AtomicU32,
}

unsafe impl Sync for Cnc {}

impl Cnc {
    pub fn new() -> Self {
        Self::default()
    }
}
