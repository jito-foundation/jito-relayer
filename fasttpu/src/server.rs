use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    net::SocketAddr,
    ops::AddAssign,
    os::fd::AsRawFd,
    ptr::null_mut,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc,
    },
};

use boring::{
    pkey::PKey,
    ssl::{SslContextBuilder, SslMethod, SslVersion},
    x509::X509,
};
use ed25519_dalek::Keypair;
use libc::{c_int, c_uint, recvmmsg, sendmmsg};
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
    pub tx_pkt_cnt: AtomicU64,
    pub tx_drop_cnt: AtomicU64,
    pub quic_accept_cnt: AtomicU64,
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
    conns: HashMap<ICID, Conn>, // Should probably be a LinkedList
    conn_ids: HashMap<SCID, ICID>,
    conns_max: usize,
    rng: SmallRng,
    quiche_cfg: quiche::Config,
    next_icid: u64,
    pending_conns: BinaryHeap<ICID>, // Connections pending serve (this is probably slow)
}

impl ServerTile {
    pub fn new(udp_fd: c_int, self_addr: SocketAddr, keypair: &Keypair, conns_max: usize) -> Self {
        let conns = HashMap::with_capacity(conns_max);
        let conn_ids = HashMap::with_capacity(conns_max * 4);

        // TODO should probably only sign this once
        let (cert_bytes, cert_key_bytes) = crate::cert::new_dummy_x509_certificate(keypair);
        let cert = X509::from_der(&cert_bytes).unwrap();
        let cert_key = PKey::private_key_from_der(&cert_key_bytes).unwrap();

        let mut tls_cfg = SslContextBuilder::new(SslMethod::tls_server()).unwrap();
        tls_cfg.set_certificate(&cert).unwrap();
        tls_cfg.set_private_key(&cert_key).unwrap();
        tls_cfg
            .set_min_proto_version(Some(SslVersion::TLS1_3))
            .unwrap();
        tls_cfg
            .set_max_proto_version(Some(SslVersion::TLS1_3))
            .unwrap();

        let mut quiche_cfg = quiche::Config::with_boring_ssl_ctx_builder(1, tls_cfg).unwrap();
        quiche_cfg.set_application_protos(&[b"solana-tpu"]).unwrap();
        quiche_cfg.set_initial_max_data(15000);
        quiche_cfg.set_initial_max_streams_uni(168);
        quiche_cfg.set_initial_max_stream_data_uni(4096);
        quiche_cfg.set_max_idle_timeout(3000u64);

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
            pending_conns: BinaryHeap::with_capacity(EVENT_CNT),
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
        self.pending_conns.clear();

        // TODO timeout management
        let rx_pkt_cnt: usize;
        unsafe {
            let msg_cnt_s = recvmmsg(
                self.udp_fd,
                self.pkt_buf.msgs.as_mut_ptr(),
                EVENT_CNT as c_uint,
                libc::MSG_DONTWAIT,
                null_mut(),
            );
            self.metrics.rx_mmsg_cnt.fetch_add(1, Relaxed);
            if msg_cnt_s < 0 {
                let last_err = std::io::Error::last_os_error();
                match last_err.kind() {
                    std::io::ErrorKind::WouldBlock => return,
                    _ => panic!("recvmmsg() failed: {}", std::io::Error::last_os_error()),
                }
            }
            rx_pkt_cnt = msg_cnt_s as usize;
        }
        self.metrics
            .rx_pkt_cnt
            .fetch_add(rx_pkt_cnt as u64, Relaxed);

        // Triage packets, sorting them into different QoS classes
        'triage: for pkt_idx in 0..rx_pkt_cnt {
            let (pkt_buf, from_sock_addr) = match self.pkt_buf.get_packet(pkt_idx) {
                Some(v) => v,
                None => {
                    self.metrics.rx_pkt_drop_martian_cnt.fetch_add(1, Relaxed);
                    continue 'triage;
                }
            };
            let from_ip_addr = from_sock_addr.ip();
            let from_udp_port = from_sock_addr.port();
            //if matches!(from_udp_port, 53 | 443) || !crate::ip::is_global(&from_ip_addr) {
            //    self.metrics.rx_pkt_drop_martian_cnt.fetch_add(1, Relaxed);
            //    continue;
            //}

            // Parse the QUIC packet's header.
            let hdr = match quiche::Header::from_slice(pkt_buf, 8) {
                Ok(v) => v,
                Err(_) => {
                    self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                    continue 'triage;
                }
            };

            // Does a connection exist for that ICID?
            let icid: Option<ICID> = {
                let dcid_bytes: &[u8] = hdr.dcid.as_ref();
                let dcid = dcid_bytes.try_into().ok().map(u64::from_le_bytes);
                dcid.and_then(|v| self.conn_ids.get(&v).copied())
            };

            match (hdr.ty, icid) {
                (quiche::Type::Initial, None) => {
                    // Statelessly process connection requests
                    self.q_initial.push(pkt_idx as u16);
                    continue 'triage;
                }
                // Packet pertains to some known conenction
                (quiche::Type::Short, Some(icid)) => {
                    self.q_established.push((pkt_idx as u16, icid))
                }
                (_, Some(icid)) => self.q_handshake.push((pkt_idx as u16, icid)),
                (_, None) => {
                    self.metrics.rx_pkt_drop_unknown_cnt.fetch_add(1, Relaxed);
                    continue 'triage;
                }
            };
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
            let quiche_conn = conn.conn.as_mut().unwrap();
            match quiche_conn.recv(
                pkt_buf,
                quiche::RecvInfo {
                    from: from_sock_addr,
                    to: self.self_addr,
                },
            ) {
                Ok(v) => v,
                Err(err) => {
                    self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                    continue 'established;
                }
            };

            Self::update_scids(icid, quiche_conn, &mut self.conn_ids, &mut self.rng);
            self.pending_conns.push(icid);

            for stream_id in quiche_conn.readable() {
                let mut stream_buf = [0u8; 4096];
                while let Ok((read, fin)) = quiche_conn.stream_recv(stream_id, &mut stream_buf) {}
            }
        }

        // Handle packets currently handshaking
        'handshake: for (pkt_idx, icid) in self.q_handshake.drain(..) {
            let (pkt_buf, from_sock_addr) = self.pkt_buf.get_packet(pkt_idx as usize).unwrap();

            let conn = match self.conns.get_mut(&icid) {
                Some(conn) => conn,
                None => panic!("icid {} vanished", icid),
            };

            let quiche_conn = conn.conn.as_mut().unwrap();
            match quiche_conn.recv(
                pkt_buf,
                quiche::RecvInfo {
                    from: from_sock_addr,
                    to: self.self_addr,
                },
            ) {
                Ok(v) => v,
                Err(err) => {
                    self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                    continue 'handshake;
                }
            };

            Self::update_scids(icid, quiche_conn, &mut self.conn_ids, &mut self.rng);
            self.pending_conns.push(icid);
        }

        // Handle packets relating to connection requests
        'initial: for pkt_idx in self.q_initial.drain(..) {
            let (pkt_buf, from_sock_addr) = self.pkt_buf.get_packet(pkt_idx as usize).unwrap();

            // Re-parse the packet header (TODO consider buffering)
            let hdr = quiche::Header::from_slice(pkt_buf, 8).unwrap();
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
            let mut conn = Conn {
                conn: Some(quiche_conn), // expensive copy :(
            };
            let quiche_conn = conn.conn.as_mut().unwrap();

            // Handle coalesced packet content
            match quiche_conn.recv(
                pkt_buf,
                quiche::RecvInfo {
                    from: from_sock_addr,
                    to: self.self_addr,
                },
            ) {
                Ok(v) => v,
                Err(err) => {
                    self.metrics.rx_pkt_drop_garbage_cnt.fetch_add(1, Relaxed);
                    continue 'initial;
                }
            };

            self.conn_ids.insert(new_dcid_u as SCID, new_icid as ICID);
            Self::update_scids(new_icid, quiche_conn, &mut self.conn_ids, &mut self.rng);
            self.conns.insert(new_icid, conn);

            self.pending_conns.push(new_icid);
            self.metrics.quic_accept_cnt.fetch_add(1, Relaxed);
        }

        self.pkt_buf.reset_iovlens(rx_pkt_cnt);

        // At this point, we read all incoming packets.
        // We can now reuse our receive buffer for sending.
        // We assume that we won't ever generate more outgoing packets
        // than there are incoming packets.  (This is a reasonable
        // assumption because the TPU server has no outgoing traffic
        // other than QUIC mgmt things and ACKs)
        let mut send_pkt_cnt = 0usize;
        'respond: for icid in self.pending_conns.drain() {
            let conn = match self.conns.get_mut(&icid) {
                Some(conn) => conn,
                None => {
                    self.conn_ids.remove(&icid);
                    continue 'respond;
                }
            };
            let conn = conn.conn.as_mut().unwrap();

            //
            //unsafe {
            //    let storage = socket2::SockAddr::from(send_info.to).as_storage();
            //    let mut iov = libc::iovec {
            //        iov_base: buf.as_ptr() as *mut _,
            //        iov_len: out_len,
            //    };
            //    libc::sendmsg(
            //        self.udp_fd,
            //        &msghdr {
            //            msg_name: &storage as *const _ as *mut _,
            //            msg_namelen: std::mem::size_of_val(&storage) as u32,
            //            msg_iov: &mut iov,
            //            msg_iovlen: 1,
            //            msg_control: null_mut(),
            //            msg_controllen: 0,
            //            msg_flags: 0,
            //        },
            //        0,
            //    );
            //}
            'genpkt: loop {
                if send_pkt_cnt >= EVENT_CNT {
                    break 'respond;
                }
                let buf = &mut self.pkt_buf.bufs[send_pkt_cnt];
                let (out_len, send_info) = match conn.send(&mut buf[..]) {
                    Ok(v) => v,
                    Err(quiche::Error::Done) => break 'genpkt,
                    Err(err) => panic!("send failed {}", err),
                };
                self.pkt_buf.addrs[send_pkt_cnt] = SockAddr::from(send_info.to).as_storage();
                self.pkt_buf.msgs[send_pkt_cnt].msg_len = out_len as u32;
                self.pkt_buf.iovs[send_pkt_cnt].iov_len = out_len;
                send_pkt_cnt += 1;
            }
        }

        if send_pkt_cnt == 0 {
            return; // nothing to do
        }

        unsafe {
            let msg_cnt_s = sendmmsg(
                self.udp_fd,
                self.pkt_buf.msgs.as_mut_ptr(),
                send_pkt_cnt as c_uint,
                libc::MSG_DONTWAIT,
            );
            if msg_cnt_s < 0 {
                let last_err = std::io::Error::last_os_error();
                match last_err.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        self.metrics
                            .tx_drop_cnt
                            .fetch_add(send_pkt_cnt as u64, Relaxed);
                        return;
                    }
                    _ => panic!("sendmmsg() failed: {}", std::io::Error::last_os_error()),
                }
            } else if (msg_cnt_s as usize) < send_pkt_cnt {
                self.metrics
                    .tx_drop_cnt
                    .fetch_add(send_pkt_cnt as u64 - msg_cnt_s as u64, Relaxed);
            } else {
                self.metrics.tx_pkt_cnt.fetch_add(msg_cnt_s as u64, Relaxed);
            }
        }

        // reset the iovlens
        self.pkt_buf.reset_iovlens(rx_pkt_cnt);
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
    pub fn new(
        config: &ServerConfig,
        cnc: Arc<Cnc>,
        keypair: &Keypair,
    ) -> Result<Self, std::io::Error> {
        let sock = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;
        sock.set_reuse_port(true)?;
        sock.bind(&SockAddr::from(config.listen_addr))?;
        let udp_fd = sock.as_raw_fd();

        let tiles = (0..config.tile_cnt)
            .map(|_| ServerTile::new(udp_fd, config.listen_addr, keypair, config.conn_cnt))
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
