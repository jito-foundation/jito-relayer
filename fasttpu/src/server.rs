use std::{
    collections::{HashSet, HashMap},
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
use quiche::ConnectionId;
use rand::{rngs::SmallRng, Rng, SeedableRng};
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
    pub rx_pkt_drop_unknown_cnt: AtomicU64,
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
        }
    }
}

unsafe impl Sync for ServerMetrics {}

pub struct ServerTile {
    udp_fd: c_int,
    self_addr: SocketAddr,
    pkt_buf: PktBufBox,
    metrics: Arc<ServerMetrics>,
    q_initial: Vec<u16>,
    q_handshake: Vec<(u16, ICID)>,
    q_established: Vec<(u16, ICID)>,
    conns: HashMap<ICID, Conn>, // Should probably be a LinkedList instead
    conn_ids: HashMap<SCID, ICID>,
    conns_max: usize,
    rng: SmallRng,
    quiche_cfg: quiche::Config,
    next_icid: u64,
}

impl ServerTile {
    pub fn new(udp_fd: c_int, self_addr: SocketAddr, conns_max: usize) -> Self {
        let conns = HashMap::with_capacity(conns_max);
        let conn_ids = HashMap::with_capacity(conns_max * 4);

        let mut quiche_cfg = quiche::Config::new(1).unwrap();

        Self {
            udp_fd,
            self_addr,
            pkt_buf: PktBufBox::new(),
            metrics: Arc::new(ServerMetrics::default()),
            q_initial: Vec::with_capacity(EVENT_CNT),
            q_handshake: Vec::with_capacity(EVENT_CNT),
            q_established: Vec::with_capacity(EVENT_CNT),
            conns,
            conn_ids,
            conns_max,
            rng: SmallRng::from_entropy(),
            quiche_cfg,
            next_icid: 0u64,
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
        self.q_initial.clear();
        self.q_handshake.clear();
        self.q_established.clear();

        // TODO timeout management
        let msg_cnt: usize;
        unsafe {
            let recvmmsg_flags: c_int = libc::MSG_DONTWAIT;
            let msg_cnt_s = recvmmsg(
                self.udp_fd,
                self.pkt_buf.msgs.as_mut_ptr(),
                EVENT_CNT as c_uint,
                recvmmsg_flags,
                null_mut(),
            );
            self.metrics.rx_mmsg_cnt.fetch_add(1, Relaxed);
            if msg_cnt_s < 0 {
                let last_err = std::io::Error::last_os_error();
                match last_err.kind() {
                    std::io::ErrorKind::WouldBlock => return,
                    _ => panic!("recvmmsg() failed: {}", std::io::Error::last_os_error())
                }
            }
            msg_cnt = msg_cnt_s as usize;
        }
        self.metrics.rx_pkt_cnt.fetch_add(msg_cnt as u64, Relaxed);

        // Triage packets, sorting them into different QoS classes
        'triage: for pkt_idx in 0..msg_cnt {
            let (pkt_buf, from_sock_addr) = match self.pkt_buf.get_packet(pkt_idx) {
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

            // Statelessly process connection requests
            if hdr.ty == quiche::Type::Initial {
                self.q_initial.push(pkt_idx as u16);
                continue 'triage;
            }

            // At this point, this packet pertains to an actively
            // handshaking connection, an established connection, or is
            // garbage.
            let dcid_bytes: &[u8] = hdr.dcid.as_ref();
            let dcid = u64::from_le_bytes(dcid_bytes.try_into().unwrap()) as SCID;

            // Reject if connection is not known
            let icid = match self.conn_ids.get(&dcid) {
                Some(icid) => *icid,
                None => {
                    self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                    continue 'triage;
                }
            };

            if hdr.ty == quiche::Type::Short {
                self.q_established.push((pkt_idx as u16, icid));
            } else {
                self.q_handshake.push((pkt_idx as u16, icid));
            }
        }

        // Handle packets relating to established conns first
        'established: for (pkt_idx, icid) in self.q_established.drain(..) {
            let (pkt_buf, from_sock_addr) = self.pkt_buf.get_packet(pkt_idx as usize).unwrap();

            let conn = match self.conns.get_mut(&icid) {
                Some(conn) => conn,
                None => {
                    // This should never happen
                    self.conn_ids.remove(&icid);
                    self.metrics.rx_pkt_drop_unknown_cnt.fetch_add(1, Relaxed);
                    continue 'established;
                }
            };

            // TODO handle conn packet
            let conn = conn.conn.as_mut().unwrap();
            match conn.recv(
                pkt_buf,
                quiche::RecvInfo {
                    from: from_sock_addr,
                    to: self.self_addr,
                },
            ) {
                Ok(v) => v,
                Err(_) => {
                    self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                    continue 'established;
                }
            };

            Self::update_scids(icid, conn, &mut self.conn_ids, &mut self.rng);
        }

        // Handle packets currently handshaking
        'handshake: for (pkt_idx, icid) in self.q_handshake.drain(..) {
            let (pkt_buf, from_sock_addr) = self.pkt_buf.get_packet(pkt_idx as usize).unwrap();

            let conn = match self.conns.get_mut(&icid) {
                Some(conn) => conn,
                None => {
                    // This should never happen
                    self.conn_ids.remove(&icid);
                    self.metrics.rx_pkt_drop_unknown_cnt.fetch_add(1, Relaxed);
                    continue 'handshake;
                }
            };

            let conn = conn.conn.as_mut().unwrap();
            match conn.recv(
                pkt_buf,
                quiche::RecvInfo {
                    from: from_sock_addr,
                    to: self.self_addr,
                },
            ) {
                Ok(v) => v,
                Err(_) => {
                    self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                    continue 'handshake;
                }
            };

            Self::update_scids(icid, conn, &mut self.conn_ids, &mut self.rng);
        }

        // Handle packets relating to connection requests
        'initial: for pkt_idx in self.q_initial.drain(..) {
            let (pkt_buf, from_sock_addr) = self.pkt_buf.get_packet(pkt_idx as usize).unwrap();

            // Re-parse the packet header (TODO consider buffering)
            let hdr = quiche::Header::from_slice(pkt_buf, quiche::MAX_CONN_ID_LEN).unwrap();
            if hdr.ty != quiche::Type::Initial {
                self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                continue 'initial;
            }

            if !quiche::version_is_supported(hdr.version) {
                self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                continue 'initial;
            }

            self.next_icid += 1;
            let new_icid = self.next_icid;

            let new_dcid_u: u64 = self.rng.gen();
            let new_dcid_b = new_dcid_u.to_le_bytes();
            let new_dcid = ConnectionId::from_ref(&new_dcid_b[..]);
            let quiche_conn = quiche::accept(
                &new_dcid,
                None,
                self.self_addr,
                from_sock_addr,
                &mut self.quiche_cfg,
            )
            .unwrap();
            let conn = Conn {
                conn: Some(quiche_conn),
            };

            self.conns.insert(new_icid, conn);
            self.conn_ids.insert(new_dcid_u as SCID, new_icid as ICID);
        }
    }

    fn update_scids(
        icid: ICID,
        conn: &mut quiche::Connection,
        conn_ids: &mut HashMap<SCID, ICID>,
        rng: &mut SmallRng,
    ) {
        // Remove retired SCIDs
        while let Some(retired_scid) = conn.retired_scid_next() {
            if let Some(scid) = parse_scid(&retired_scid) {
                conn_ids.remove(&scid);
            }
        }
        // Provide new SCIDs
        while conn.scids_left() > 0 {
            let scid_u: u64 = rng.gen();
            let scid_b = scid_u.to_le_bytes();
            let scid = ConnectionId::from_ref(&scid_b[..]);
            let reset_token: u128 = rng.gen();
            match conn.new_scid(&scid, reset_token, false) {
                Ok(_) => (),
                Err(quiche::Error::InvalidState) => continue, // already used
                Err(err) => panic!("Unexpected failure providing SCID: {}", err),
            };
            conn_ids.insert(scid_u as SCID, icid);
        }
    }
}

pub type ICID = u64;
pub type SCID = u64;

fn parse_scid(id: &ConnectionId) -> Option<u64> {
    let bytes = id.as_ref();
    if bytes.len() != 8 {
        return None;
    }
    Some(u64::from_le_bytes(bytes.try_into().unwrap()))
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
    pub conn_cnt: usize,
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
            .map(|_| ServerTile::new(udp_fd, config.listen_addr, config.conn_cnt))
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

// This poor thing is ~20 kB.
pub struct Conn {
    pub conn: Option<quiche::Connection>,
}
