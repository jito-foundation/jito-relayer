use solana_metrics::datapoint_info;

#[derive(Default)]
pub struct BlockEngineStats {
    heartbeat_count: u64,
    heartbeat_elapsed_us: u64,

    aoi_update_count: u64,
    aoi_update_elapsed_us: u64,
    aoi_accounts_received: u64,

    poi_update_count: u64,
    poi_update_elapsed_us: u64,
    poi_accounts_received: u64,

    num_packets_received: u64,

    packet_filter_elapsed_us: u64,
    packet_forward_elapsed_us: u64,

    // high water mark of queue length
    block_engine_packet_sender_len: u64,

    auth_refresh_count: u64,
    refresh_auth_elapsed_us: u64,

    packet_forward_count: u64,

    metrics_delay_us: u64,

    accounts_of_interest_len: u64,
    programs_of_interest_len: u64,
    flush_elapsed_us: u64,
}

impl BlockEngineStats {
    pub fn increment_heartbeat_count(&mut self, num: u64) {
        self.heartbeat_count = self.heartbeat_count.saturating_add(num)
    }

    pub fn increment_heartbeat_elapsed_us(&mut self, num: u64) {
        self.heartbeat_elapsed_us = self.heartbeat_elapsed_us.saturating_add(num)
    }

    pub fn increment_aoi_update_count(&mut self, num: u64) {
        self.aoi_update_count = self.aoi_update_count.saturating_add(num)
    }

    pub fn increment_aoi_accounts_received(&mut self, num: u64) {
        self.aoi_accounts_received = self.aoi_accounts_received.saturating_add(num)
    }

    pub fn increment_aoi_update_elapsed_us(&mut self, num: u64) {
        self.aoi_update_elapsed_us = self.aoi_update_elapsed_us.saturating_add(num)
    }

    pub fn increment_poi_update_count(&mut self, num: u64) {
        self.poi_update_count = self.poi_update_count.saturating_add(num)
    }

    pub fn increment_poi_accounts_received(&mut self, num: u64) {
        self.poi_accounts_received = self.poi_accounts_received.saturating_add(num)
    }

    pub fn increment_poi_update_elapsed_us(&mut self, num: u64) {
        self.poi_update_elapsed_us = self.poi_update_elapsed_us.saturating_add(num)
    }

    pub fn increment_num_packets_received(&mut self, num: u64) {
        self.num_packets_received = self.num_packets_received.saturating_add(num)
    }

    pub fn increment_packet_filter_elapsed_us(&mut self, num: u64) {
        self.packet_filter_elapsed_us = self.packet_filter_elapsed_us.saturating_add(num)
    }

    pub fn increment_packet_forward_elapsed_us(&mut self, num: u64) {
        self.packet_forward_elapsed_us = self.packet_forward_elapsed_us.saturating_add(num)
    }

    pub fn update_block_engine_packet_sender_len(&mut self, num: u64) {
        self.block_engine_packet_sender_len =
            std::cmp::max(self.block_engine_packet_sender_len, num);
    }

    pub fn increment_auth_refresh_count(&mut self, num: u64) {
        self.auth_refresh_count = self.auth_refresh_count.saturating_add(num)
    }

    pub fn increment_refresh_auth_elapsed_us(&mut self, num: u64) {
        self.refresh_auth_elapsed_us = self.refresh_auth_elapsed_us.saturating_add(num)
    }

    pub fn increment_packet_forward_count(&mut self, num: u64) {
        self.packet_forward_count = self.packet_forward_count.saturating_add(num)
    }

    pub fn increment_metrics_delay_us(&mut self, num: u64) {
        self.metrics_delay_us = self.metrics_delay_us.saturating_add(num)
    }

    pub fn increment_accounts_of_interest_len(&mut self, num: u64) {
        self.accounts_of_interest_len = self.accounts_of_interest_len.saturating_add(num)
    }

    pub fn increment_programs_of_interest_len(&mut self, num: u64) {
        self.programs_of_interest_len = self.programs_of_interest_len.saturating_add(num)
    }

    pub fn increment_flush_elapsed_us(&mut self, num: u64) {
        self.flush_elapsed_us = self.flush_elapsed_us.saturating_add(num)
    }

    pub fn report(&self) {
        datapoint_info!(
            "block_engine_relayer-loop_stats",
            ("heartbeat_count", self.heartbeat_count, i64),
            ("heartbeat_elapsed_us", self.heartbeat_elapsed_us, i64),
            ("aoi_update_count", self.aoi_update_count, i64),
            ("aoi_update_elapsed_us", self.aoi_update_elapsed_us, i64),
            ("aoi_accounts_received", self.aoi_accounts_received, i64),
            ("poi_update_count", self.poi_update_count, i64),
            ("poi_update_elapsed_us", self.poi_update_elapsed_us, i64),
            ("poi_accounts_received", self.poi_accounts_received, i64),
            ("num_packets_received", self.num_packets_received, i64),
            (
                "packet_filter_elapsed_us",
                self.packet_filter_elapsed_us,
                i64
            ),
            (
                "packet_forward_elapsed_us",
                self.packet_forward_elapsed_us,
                i64
            ),
            (
                "block_engine_packet_sender_len",
                self.block_engine_packet_sender_len,
                i64
            ),
            ("auth_refresh_count", self.auth_refresh_count, i64),
            ("refresh_auth_elapsed_us", self.refresh_auth_elapsed_us, i64),
            ("packet_forward_count", self.packet_forward_count, i64),
            ("metrics_delay_us", self.metrics_delay_us, i64),
            (
                "accounts_of_interest_len",
                self.accounts_of_interest_len,
                i64
            ),
            (
                "programs_of_interest_len",
                self.programs_of_interest_len,
                i64
            ),
            ("flush_elapsed_us", self.flush_elapsed_us, i64),
        )
    }
}
