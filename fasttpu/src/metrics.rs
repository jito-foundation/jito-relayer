use std::{
    ops::AddAssign,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};

#[derive(Default, Debug)]
pub struct ServerMetrics {
    pub rx_pkt_cnt: AtomicU64,
    pub rx_mmsg_cnt: AtomicU64,
    pub rx_pkt_drop_garbage_cnt: AtomicU64,
    pub rx_pkt_drop_martian_cnt: AtomicU64,
    pub rx_pkt_drop_unknown_cnt: AtomicU64,
    pub tx_pkt_cnt: AtomicU64,
    pub tx_drop_cnt: AtomicU64,
    pub quic_conn_cnt: AtomicU64,
    pub quic_accept_cnt: AtomicU64,
    pub tpu_txn_cnt: AtomicU64,
    pub tpu_txn_drop_cnt: AtomicU64,
}

impl AddAssign for ServerMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.rx_pkt_cnt
            .fetch_add(rhs.rx_pkt_cnt.load(Relaxed), Relaxed);
        self.rx_mmsg_cnt
            .fetch_add(rhs.rx_mmsg_cnt.load(Relaxed), Relaxed);
        self.rx_pkt_drop_garbage_cnt
            .fetch_add(rhs.rx_pkt_drop_garbage_cnt.load(Relaxed), Relaxed);
        self.rx_pkt_drop_martian_cnt
            .fetch_add(rhs.rx_pkt_drop_martian_cnt.load(Relaxed), Relaxed);
        self.rx_pkt_drop_unknown_cnt
            .fetch_add(rhs.rx_pkt_drop_unknown_cnt.load(Relaxed), Relaxed);
        self.tx_pkt_cnt
            .fetch_add(rhs.tx_pkt_cnt.load(Relaxed), Relaxed);
        self.tx_drop_cnt
            .fetch_add(rhs.tx_drop_cnt.load(Relaxed), Relaxed);
        self.quic_accept_cnt
            .fetch_add(rhs.quic_accept_cnt.load(Relaxed), Relaxed);
        self.quic_conn_cnt
            .fetch_add(rhs.quic_conn_cnt.load(Relaxed), Relaxed);
        self.tpu_txn_cnt
            .fetch_add(rhs.tpu_txn_cnt.load(Relaxed), Relaxed);
        self.tpu_txn_drop_cnt
            .fetch_add(rhs.tpu_txn_drop_cnt.load(Relaxed), Relaxed);
    }
}

impl Clone for ServerMetrics {
    fn clone(&self) -> Self {
        Self {
            rx_pkt_cnt: AtomicU64::new(self.rx_pkt_cnt.load(Relaxed)),
            rx_mmsg_cnt: AtomicU64::new(self.rx_mmsg_cnt.load(Relaxed)),
            rx_pkt_drop_garbage_cnt: AtomicU64::new(self.rx_pkt_drop_garbage_cnt.load(Relaxed)),
            rx_pkt_drop_martian_cnt: AtomicU64::new(self.rx_pkt_drop_martian_cnt.load(Relaxed)),
            rx_pkt_drop_unknown_cnt: AtomicU64::new(self.rx_pkt_drop_unknown_cnt.load(Relaxed)),
            tx_pkt_cnt: AtomicU64::new(self.tx_pkt_cnt.load(Relaxed)),
            tx_drop_cnt: AtomicU64::new(self.tx_drop_cnt.load(Relaxed)),
            quic_accept_cnt: AtomicU64::new(self.quic_accept_cnt.load(Relaxed)),
            quic_conn_cnt: AtomicU64::new(self.quic_conn_cnt.load(Relaxed)),
            tpu_txn_cnt: AtomicU64::new(self.tpu_txn_cnt.load(Relaxed)),
            tpu_txn_drop_cnt: AtomicU64::new(self.tpu_txn_drop_cnt.load(Relaxed)),
        }
    }
}

unsafe impl Sync for ServerMetrics {}
