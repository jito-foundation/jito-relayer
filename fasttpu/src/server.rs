use std::{
    net::SocketAddr,
    ops::AddAssign,
    os::fd::AsRawFd,
    ptr::null_mut,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc,
    },
};

use libc::{c_int, c_uint, recvmmsg};
use socket2::SockAddr;

use crate::{
    buf::{PktBufBox, EVENT_CNT},
    cnc::Cnc,
};

#[derive(Default, Debug)]
pub struct ServerMetrics {
    pub rx_pkt_cnt: AtomicU64,
    pub rx_mmsg_cnt: AtomicU64,
    pub rx_pkt_drop_garbage_cnt: AtomicU64,
    pub rx_pkt_drop_martian_cnt: AtomicU64,
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
    }
}

impl Clone for ServerMetrics {
    fn clone(&self) -> Self {
        Self {
            rx_pkt_cnt: AtomicU64::new(self.rx_pkt_cnt.load(Relaxed)),
            rx_mmsg_cnt: AtomicU64::new(self.rx_mmsg_cnt.load(Relaxed)),
            rx_pkt_drop_garbage_cnt: AtomicU64::new(self.rx_pkt_drop_garbage_cnt.load(Relaxed)),
            rx_pkt_drop_martian_cnt: AtomicU64::new(self.rx_pkt_drop_martian_cnt.load(Relaxed)),
        }
    }
}

unsafe impl Sync for ServerMetrics {}

pub struct ServerTile {
    udp_fd: c_int,
    pkt_buf: PktBufBox,
    metrics: Arc<ServerMetrics>,
    q_connreq: Vec<u16>,
    q_established: Vec<u16>,
}

impl ServerTile {
    pub fn new(udp_fd: c_int) -> Self {
        Self {
            udp_fd,
            pkt_buf: PktBufBox::new(),
            metrics: Arc::new(ServerMetrics::default()),
            q_connreq: Vec::with_capacity(EVENT_CNT),
            q_established: Vec::with_capacity(EVENT_CNT),
        }
    }

    pub fn metrics(&self) -> Arc<ServerMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn run(&mut self) {
        loop {
            self.poll();
        }
    }

    pub fn poll(&mut self) {
        self.q_connreq.clear();
        self.q_established.clear();

        // TODO timeout management
        let msg_cnt: usize;
        unsafe {
            let recvmmsg_flags: c_int = 0;
            let msg_cnt_s = recvmmsg(
                self.udp_fd,
                self.pkt_buf.msgs.as_mut_ptr(),
                EVENT_CNT as c_uint,
                recvmmsg_flags,
                null_mut(),
            );
            self.metrics.rx_mmsg_cnt.fetch_add(1, Relaxed);
            if msg_cnt_s < 0 {
                panic!("recvmmsg() failed: {}", std::io::Error::last_os_error());
            }
            msg_cnt = msg_cnt_s as usize;
        }
        self.metrics.rx_pkt_cnt.fetch_add(msg_cnt as u64, Relaxed);

        'triage: for i in 0..msg_cnt {
            let (pkt_buf, from_sock_addr) = match self.pkt_buf.get_packet(i) {
                Some(v) => v,
                None => {
                    self.metrics.rx_pkt_drop_martian_cnt.fetch_add(1, Relaxed);
                    continue 'triage;
                }
            };
            let from_ip_addr = from_sock_addr.ip();
            let from_udp_port = from_sock_addr.port();
            if matches!(from_udp_port, 53 | 443) || !crate::ip::is_global(&from_ip_addr) {
                self.metrics.rx_pkt_drop_martian_cnt.fetch_add(1, Relaxed);
                continue;
            }

            // Parse the QUIC packet's header.
            let hdr = match quiche::Header::from_slice(pkt_buf, quiche::MAX_CONN_ID_LEN) {
                Ok(v) => v,
                Err(_) => {
                    self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                    continue 'triage;
                }
            };

            // Relates to connection mangement (connection request,
            // retry, handshake, version negotiation, or 0-RTT)
            if hdr.ty != quiche::Type::Short {
                self.q_connreq.push(i as u16);
                continue 'triage;
            }

            // Check if connection is known
        }
    }
}

pub struct Server {
    pub tile_metrics: Vec<Arc<ServerMetrics>>,
    tiles: Vec<ServerTile>,
    thread_handles: Vec<std::thread::JoinHandle<()>>,
    cnc: Arc<Cnc>,
    sock: socket2::Socket,
}

pub struct ServerConfig {
    pub tile_cnt: usize,
    pub listen_addr: SocketAddr,
}

impl Server {
    pub fn new(config: &ServerConfig, cnc: Arc<Cnc>) -> Result<Self, std::io::Error> {
        let sock = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;
        sock.set_reuse_port(true)?;
        sock.bind(&SockAddr::from(config.listen_addr))?;
        let udp_fd = sock.as_raw_fd();

        let tiles = (0..config.tile_cnt)
            .map(|_| ServerTile::new(udp_fd))
            .collect::<Vec<ServerTile>>();
        let tile_metrics = tiles.iter().map(|tile| tile.metrics()).collect();

        Ok(Self {
            thread_handles: Vec::with_capacity(tiles.len()),
            tile_metrics,
            tiles,
            cnc,
            sock,
        })
    }

    pub fn start(&mut self) {
        let tiles = std::mem::take(&mut self.tiles);
        tiles
            .into_iter()
            .map(|mut tile| {
                std::thread::spawn(move || {
                    tile.run();
                })
            })
            .for_each(|hdl| self.thread_handles.push(hdl));
    }

    pub fn wait(&mut self) {
        self.thread_handles
            .drain(..)
            .for_each(|hdl| hdl.join().expect("Failed to join thread"));
    }
}
