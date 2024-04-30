use std::{
    collections::HashSet,
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::Range,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use agave_validator::admin_rpc_service::StakedNodesOverrides;
use clap::Parser;
use crossbeam_channel::tick;
use dashmap::DashMap;
use env_logger::Env;
use jito_block_engine::block_engine::{BlockEngineConfig, BlockEngineRelayerHandler};
use jito_core::{
    graceful_panic,
    tpu::{Tpu, TpuSockets},
};
use jito_protos::{
    auth::auth_service_server::AuthServiceServer, relayer::relayer_server::RelayerServer,
};
use jito_relayer::{
    auth_interceptor::AuthInterceptor,
    auth_service::{AuthServiceImpl, ValidatorAuther},
    health_manager::HealthManager,
    relayer::RelayerImpl,
    schedule_cache::{LeaderScheduleCacheUpdater, LeaderScheduleUpdatingHandle},
};
use jito_relayer_web::{start_relayer_web_server, RelayerState};
use jito_rpc::load_balancer::LoadBalancer;
use jito_transaction_relayer::forwarder::start_forward_and_delay_thread;
use jwt::{AlgorithmType, PKeyWithDigest};
use log::{debug, error, info, warn};
use openssl::{hash::MessageDigest, pkey::PKey};
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_metrics::{datapoint_error, datapoint_info};
use solana_net_utils::{bind_more_with_config, multi_bind_in_range, SocketConfig};
use solana_sdk::{
    address_lookup_table_account::AddressLookupTableAccount,
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
};
use tikv_jemallocator::Jemalloc;
use tokio::{runtime::Builder, signal, sync::mpsc::channel};
use tonic::transport::Server;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// DEPRECATED, will be removed in a future release.
    #[deprecated(since = "0.1.8", note = "UDP TPU disabled")]
    #[arg(long, env, default_value_t = 0)]
    tpu_port: u16,

    /// DEPRECATED, will be removed in a future release.
    #[deprecated(since = "0.1.8", note = "UDP TPU_FWD disabled")]
    #[arg(long, env, default_value_t = 0)]
    tpu_fwd_port: u16,

    /// Port to bind to for tpu quic packets.
    /// The TPU will bind to all ports in the range of (tpu_quic_port, tpu_quic_port + num_tpu_quic_servers).
    /// Open firewall ports for this entire range
    /// Make sure to not overlap any tpu forward ports with the normal tpu ports.
    /// Note: get_tpu_configs will return ths port - 6 to validators to match old UDP TPU definition.
    #[arg(long, env, default_value_t = 11_228)]
    tpu_quic_port: u16,

    /// Number of tpu quic servers to spawn.
    #[arg(long, env, default_value_t = 1)]
    num_tpu_quic_servers: u16,

    /// Port to bind to for tpu quic fwd packets.
    /// Make sure to set this to at least (num_tpu_fwd_quic_servers + 6) higher than tpu_quic_port,
    /// to avoid overlap any tpu forward ports with the normal tpu ports.
    /// TPU_FWD will bind to all ports in the range of (tpu_fwd_quic_port, tpu_fwd_quic_port + num_tpu_fwd_quic_servers).
    /// Open firewall ports for this entire range
    /// Note: get_tpu_configs will return ths port - 6 to validators to match old UDP TPU definition.
    #[arg(long, env, default_value_t = 11_229)]
    tpu_quic_fwd_port: u16,

    /// Number of tpu fwd quic servers to spawn.
    #[arg(long, env, default_value_t = 1)]
    num_tpu_fwd_quic_servers: u16,

    /// Number of endpoints per quic server
    #[arg(long, env, default_value_t = 10)]
    num_quic_endpoints: u16,

    /// Bind IP address for GRPC server
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    grpc_bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[arg(long, env, default_value_t = 11_226)]
    grpc_bind_port: u16,

    /// RPC servers as a space-separated list. Shall be same position as websocket equivalent below
    #[arg(
        long,
        env,
        value_delimiter = ' ',
        default_value = "http://127.0.0.1:8899"
    )]
    rpc_servers: Vec<String>,

    /// Websocket servers as a space-separated list. Shall be same position as RPC equivalent above
    #[arg(
        long,
        env,
        value_delimiter = ' ',
        default_value = "ws://127.0.0.1:8900"
    )]
    websocket_servers: Vec<String>,

    #[arg(long, env, default_value = "entrypoint.mainnet-beta.solana.com:8001")]
    entrypoint_address: String,

    /// This is the IP address that will be shared with the validator. The validator will
    /// tell the rest of the network to send packets here.
    #[arg(long, env)]
    public_ip: Option<IpAddr>,

    /// Packet delay in milliseconds
    #[arg(long, env, default_value_t = 200)]
    packet_delay_ms: u32,

    /// Address for Jito Block Engine.
    /// See https://jito-labs.gitbook.io/mev/searcher-resources/block-engine#connection-details
    #[arg(long, env)]
    block_engine_url: Option<String>,

    /// Manual override for authentication service address of the block-engine.
    /// Defaults to `--block-engine-url`
    #[arg(long, env)]
    block_engine_auth_service_url: Option<String>,

    /// Path to keypair file used to authenticate with the backend.
    #[arg(long, env)]
    keypair_path: PathBuf,

    /// Validators allowed to authenticate and connect to the relayer, comma separated.
    /// If null then all validators on the leader schedule shall be permitted.
    #[arg(long, env, value_delimiter = ',')]
    allowed_validators: Option<Vec<Pubkey>>,

    /// The private key used to sign tokens by this server.
    #[arg(long, env)]
    signing_key_pem_path: PathBuf,

    /// The public key used to verify tokens by this and other services.
    #[arg(long, env)]
    verifying_key_pem_path: PathBuf,

    /// Specifies how long access_tokens are valid for, expressed in seconds.
    #[arg(long, env, default_value_t = 1_800)]
    access_token_ttl_secs: u64,

    /// Specifies how long access_tokens are valid for, expressed in seconds.
    #[arg(long, env, default_value_t = 180_000)]
    refresh_token_ttl_secs: u64,

    /// Specifies how long challenges are valid for, expressed in seconds.
    #[arg(long, env, default_value_t = 1_800)]
    challenge_ttl_secs: u64,

    /// The interval at which challenges are checked for expiration.
    #[arg(long, env, default_value_t = 180)]
    challenge_expiration_sleep_interval_secs: u64,

    /// How long it takes to miss a slot for the system to be considered unhealthy
    #[arg(long, env, default_value_t = 10)]
    missing_slot_unhealthy_secs: u64,

    /// DEPRECATED. Solana cluster name (mainnet-beta, testnet, devnet, ...)
    #[arg(long, env)]
    cluster: Option<String>,

    /// DEPRECATED. Region (amsterdam, dallas, frankfurt, ...)
    #[arg(long, env)]
    region: Option<String>,

    /// Accounts of interest cache TTL. Note this must play nicely with the refresh period that
    /// block engine uses to send full updates.
    #[arg(long, env, default_value_t = 300)]
    aoi_cache_ttl_secs: u64,

    /// How frequently to refresh the address lookup table accounts
    #[arg(long, env, default_value_t = 30)]
    lookup_table_refresh_secs: u64,

    /// Space-separated addresses to drop transactions for OFAC
    /// If any transaction mentions these addresses, the transaction will be dropped.
    #[arg(long, env, value_delimiter = ' ', value_parser = Pubkey::from_str)]
    ofac_addresses: Option<Vec<Pubkey>>,

    /// Webserver bind address that exposes diagnostic information
    #[arg(long, env, default_value_t = SocketAddr::from_str("127.0.0.1:11227").unwrap())]
    webserver_bind_addr: SocketAddr,

    /// Max unstaked connections for the QUIC server
    #[arg(long, env, default_value_t = 500)]
    max_unstaked_quic_connections: usize,

    /// Max unstaked connections for the QUIC server
    #[arg(long, env, default_value_t = 2000)]
    max_staked_quic_connections: usize,

    /// Number of packets to send in each packet batch to the validator
    #[arg(long, env, default_value_t = 4)]
    validator_packet_batch_size: usize,

    /// Disable Mempool forwarding
    #[arg(long, env, default_value_t = false)]
    disable_mempool: bool,

    /// Forward all received packets to all connected validators,
    /// regardless of leader schedule.  
    /// Note: This is required to be true for Stake Weighted Quality of Service (SWQOS)!
    #[arg(long, env, default_value_t = false)]
    forward_all: bool,

    /// Staked Nodes Overrides Path
    /// Provide path to a yaml file with custom overrides for stakes of specific
    ///  identities. Overriding the amount of stake this validator considers as valid
    ///  for other peers in network. The stake amount is used for calculating the
    ///  number of QUIC streams permitted from the peer and vote packet sender stage.
    ///  Format of the file: `staked_map_id: {<pubkey>: <SOL stake amount>}
    #[arg(long, env)]
    staked_nodes_overrides: Option<PathBuf>,
}

#[derive(Debug)]
struct Sockets {
    tpu_sockets: TpuSockets,
    tpu_ip: IpAddr,
    tpu_fwd_ip: IpAddr,
}

fn get_sockets(args: &Args) -> Sockets {
    assert!(args.num_tpu_quic_servers < u16::MAX);
    assert!(args.num_tpu_fwd_quic_servers < u16::MAX);

    let tpu_ports = Range {
        start: args.tpu_quic_port,
        end: args
            .tpu_quic_port
            .checked_add(args.num_tpu_quic_servers)
            .unwrap(),
    };
    let tpu_fwd_ports = Range {
        start: args.tpu_quic_fwd_port,
        end: args
            .tpu_quic_fwd_port
            .checked_add(args.num_tpu_fwd_quic_servers)
            .unwrap(),
    };

    for tpu_port in tpu_ports.start..tpu_ports.end {
        assert!(!tpu_fwd_ports.contains(&tpu_port));
    }

    let (tpu_p, tpu_quic_sockets): (Vec<_>, Vec<Vec<_>>) = (0..args.num_tpu_quic_servers)
        .map(|i| {
            let (port, mut sock) = multi_bind_in_range(
                IpAddr::V4(Ipv4Addr::from([0, 0, 0, 0])),
                (tpu_ports.start + i, tpu_ports.start + i + 1),
                1,
            )
            .unwrap();

            let quic_config = SocketConfig {
                reuseaddr: false,
                reuseport: true,
            };

            let transactions_quic_sockets = bind_more_with_config(
                sock.pop().unwrap(),
                args.num_quic_endpoints.into(),
                quic_config.clone(),
            )
            .unwrap();

            (
                (port..port + args.num_quic_endpoints).collect(),
                transactions_quic_sockets,
            )
        })
        .unzip();

    let (tpu_fwd_p, tpu_fwd_quic_sockets): (Vec<_>, Vec<Vec<_>>) = (0..args
        .num_tpu_fwd_quic_servers)
        .map(|i| {
            let (port, mut sock) = multi_bind_in_range(
                IpAddr::V4(Ipv4Addr::from([0, 0, 0, 0])),
                (tpu_fwd_ports.start + i, tpu_fwd_ports.start + i + 1),
                1,
            )
            .unwrap();

            let quic_config = SocketConfig {
                reuseaddr: false,
                reuseport: true,
            };

            let transactions_forwards_quic_sockets = bind_more_with_config(
                sock.pop().unwrap(),
                args.num_quic_endpoints.into(),
                quic_config,
            )
            .unwrap();

            (
                (port..port + args.num_quic_endpoints).collect(),
                transactions_forwards_quic_sockets,
            )
        })
        .unzip();

    assert_eq!(tpu_ports.collect::<Vec<_>>(), tpu_p);
    assert_eq!(tpu_fwd_ports.collect::<Vec<_>>(), tpu_fwd_p);

    Sockets {
        tpu_sockets: TpuSockets {
            transactions_quic_sockets: tpu_quic_sockets,
            transactions_forwards_quic_sockets: tpu_fwd_quic_sockets,
        },
        tpu_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        tpu_fwd_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    }
}

fn main() {
    const MAX_BUFFERED_REQUESTS: usize = 10;
    const REQUESTS_PER_SECOND: u64 = 5;

    // one can override the default log level by setting the env var RUST_LOG
    env_logger::Builder::from_env(Env::new().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let args: Args = Args::parse();
    info!("args: {:?}", args);

    // Warn about deprecated args
    if args.cluster.is_some() {
        warn!("--cluster arg is deprecated and may be removed in the next release.")
    }
    if args.region.is_some() {
        warn!("--region arg is deprecated and may be removed in the next release.")
    }

    let public_ip = if args.public_ip.is_some() {
        args.public_ip.unwrap()
    } else {
        let entrypoint = solana_net_utils::parse_host_port(args.entrypoint_address.as_str())
            .expect("parse entrypoint");
        info!(
            "Contacting {} to determine the validator's public IP address",
            entrypoint
        );
        solana_net_utils::get_public_ip_addr(&entrypoint).expect("get public ip address")
    };

    info!("public ip: {:?}", public_ip);
    assert!(
        public_ip.is_ipv4(),
        "Your public IP address needs to be IPv4 but is currently listed as {}. \
    If you are seeing this error and not passing in --public-ip, \
    please find your public ip address and pass it in on the command line",
        public_ip
    );
    assert!(
        !public_ip.is_loopback(),
        "Your public IP can't be the loopback interface"
    );

    // Supporting IPV6 addresses is a DOS vector since they are cheap and there's a much larger amount of them.
    // The DOS is specifically with regards to the challenges queue filling up and starving other legitimate
    // challenge requests.
    assert!(args.grpc_bind_ip.is_ipv4(), "must bind to IPv4 address");

    let sockets = get_sockets(&args);

    // make sure to allow your firewall to accept UDP packets on these ports
    // if you're using staked overrides, you can provide one of these addresses
    // to --rpc-send-transaction-tpu-peer
    for s in sockets
        .tpu_sockets
        .transactions_quic_sockets
        .iter()
        .flatten()
    {
        info!(
            "TPU quic socket is listening at: {}:{}",
            public_ip.to_string(),
            s.local_addr().unwrap().port()
        );
    }

    for s in sockets
        .tpu_sockets
        .transactions_forwards_quic_sockets
        .iter()
        .flatten()
    {
        info!(
            "TPU forward quic socket is listening at: {}:{}",
            public_ip.to_string(),
            s.local_addr().unwrap().port()
        );
    }

    let keypair =
        Arc::new(read_keypair_file(args.keypair_path).expect("keypair file does not exist"));
    solana_metrics::set_host_id(format!(
        "{}_{}",
        hostname::get().unwrap().to_str().unwrap(), // hostname should follow RFC1123
        keypair.pubkey()
    ));
    info!("Relayer started with pubkey: {}", keypair.pubkey());
    datapoint_info!(
        "relayer-mempool-enabled",
        ("mempool_enabled", !args.disable_mempool, bool)
    );

    let exit = graceful_panic(None);

    assert_eq!(
        args.rpc_servers.len(),
        args.websocket_servers.len(),
        "number of rpc servers must match number of websocket servers"
    );

    let servers: Vec<(String, String)> = args
        .rpc_servers
        .into_iter()
        .zip(args.websocket_servers)
        .collect();

    let ofac_addresses: HashSet<Pubkey> = args
        .ofac_addresses
        .map(|a| a.into_iter().collect())
        .unwrap_or_default();
    info!("ofac addresses: {:?}", ofac_addresses);

    let (rpc_load_balancer, slot_receiver) = LoadBalancer::new(&servers, &exit);
    let rpc_load_balancer = Arc::new(rpc_load_balancer);

    // Lookup table refresher
    let address_lookup_table_cache: Arc<DashMap<Pubkey, AddressLookupTableAccount>> =
        Arc::new(DashMap::new());
    let lookup_table_refresher = start_lookup_table_refresher(
        &rpc_load_balancer,
        &address_lookup_table_cache,
        Duration::from_secs(args.lookup_table_refresh_secs),
        &exit,
    );

    let staked_nodes_overrides = match args.staked_nodes_overrides {
        None => StakedNodesOverrides::default(),
        Some(p) => {
            let file = fs::File::open(&p).expect(&format!(
                "Failed to open staked nodes overrides file: {:?}",
                &p
            ));
            serde_yaml::from_reader(file).expect(&format!(
                "Failed to read staked nodes overrides file: {:?}",
                &p,
            ))
        }
    };
    let (tpu, verified_receiver) = Tpu::new(
        sockets.tpu_sockets,
        &exit,
        &keypair,
        &sockets.tpu_ip,
        &sockets.tpu_fwd_ip,
        &rpc_load_balancer,
        args.max_unstaked_quic_connections,
        args.max_staked_quic_connections,
        staked_nodes_overrides.staked_map_id,
    );

    let leader_cache = LeaderScheduleCacheUpdater::new(&rpc_load_balancer, &exit);

    // receiver tracked as relayer_metrics.delay_packet_receiver_len
    let (delay_packet_sender, delay_packet_receiver) =
        crossbeam_channel::bounded(Tpu::TPU_QUEUE_CAPACITY);

    // NOTE: make sure the channel here isn't too big because it will get backed up
    // with packets when the block engine isn't connected
    // tracked as forwarder_metrics.block_engine_sender_len
    let (block_engine_sender, block_engine_receiver) =
        channel(jito_transaction_relayer::forwarder::BLOCK_ENGINE_FORWARDER_QUEUE_CAPACITY);

    let forward_and_delay_threads = start_forward_and_delay_thread(
        verified_receiver,
        delay_packet_sender,
        args.packet_delay_ms,
        block_engine_sender,
        1,
        args.disable_mempool,
        &exit,
    );

    let is_connected_to_block_engine = Arc::new(AtomicBool::new(false));
    let block_engine_config = if !args.disable_mempool && args.block_engine_url.is_some() {
        let block_engine_url = args.block_engine_url.unwrap();
        let auth_service_url = args
            .block_engine_auth_service_url
            .unwrap_or(block_engine_url.clone());
        Some(BlockEngineConfig {
            block_engine_url,
            auth_service_url,
        })
    } else {
        None
    };
    let block_engine_forwarder = BlockEngineRelayerHandler::new(
        block_engine_config,
        block_engine_receiver,
        keypair,
        exit.clone(),
        args.aoi_cache_ttl_secs,
        address_lookup_table_cache.clone(),
        &is_connected_to_block_engine,
        ofac_addresses.clone(),
    );

    // receiver tracked as relayer_metrics.slot_receiver_len
    // downstream channel gets data that was duplicated by HealthManager
    let (downstream_slot_sender, downstream_slot_receiver) =
        crossbeam_channel::bounded(LoadBalancer::SLOT_QUEUE_CAPACITY);
    let health_manager = HealthManager::new(
        slot_receiver,
        downstream_slot_sender,
        Duration::from_secs(args.missing_slot_unhealthy_secs),
        exit.clone(),
    );

    let server_addr = SocketAddr::new(args.grpc_bind_ip, args.grpc_bind_port);
    let relayer_svc = RelayerImpl::new(
        downstream_slot_receiver,
        delay_packet_receiver,
        leader_cache.handle(),
        public_ip,
        (0..args.num_tpu_quic_servers)
            .map(|i| (args.tpu_quic_port + i..args.tpu_quic_port + i + 1).collect())
            .collect(),
        (0..args.num_tpu_quic_servers)
            .map(|i| {
                (args.tpu_quic_fwd_port + i * args.num_quic_endpoints
                    ..args.tpu_quic_fwd_port + (i + 1) * args.num_quic_endpoints)
                    .collect()
            })
            .collect(),
        health_manager.handle(),
        exit.clone(),
        ofac_addresses,
        address_lookup_table_cache,
        args.validator_packet_batch_size,
        args.forward_all,
    );

    let priv_key = fs::read(&args.signing_key_pem_path).unwrap_or_else(|_| {
        panic!(
            "Failed to read signing key file: {:?}",
            &args.verifying_key_pem_path
        )
    });
    let signing_key = PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::private_key_from_pem(&priv_key).unwrap(),
    };

    let key = fs::read(&args.verifying_key_pem_path).unwrap_or_else(|_| {
        panic!(
            "Failed to read verifying key file: {:?}",
            &args.verifying_key_pem_path
        )
    });
    let verifying_key = Arc::new(PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::public_key_from_pem(&key).unwrap(),
    });

    let validator_store = match args.allowed_validators {
        Some(pubkeys) => ValidatorStore::UserDefined(HashSet::from_iter(pubkeys)),
        None => ValidatorStore::LeaderSchedule(leader_cache.handle()),
    };

    let relayer_state = Arc::new(RelayerState::new(
        health_manager.handle(),
        &is_connected_to_block_engine,
        relayer_svc.handle(),
    ));

    let rt = Builder::new_multi_thread().enable_all().build().unwrap();
    rt.spawn({
        let relayer_state = relayer_state.clone();
        start_relayer_web_server(
            relayer_state,
            args.webserver_bind_addr,
            MAX_BUFFERED_REQUESTS,
            REQUESTS_PER_SECOND,
        )
    });

    rt.block_on(async {
        let auth_svc = AuthServiceImpl::new(
            ValidatorAutherImpl {
                store: validator_store,
            },
            signing_key,
            verifying_key.clone(),
            Duration::from_secs(args.access_token_ttl_secs),
            Duration::from_secs(args.refresh_token_ttl_secs),
            Duration::from_secs(args.challenge_ttl_secs),
            Duration::from_secs(args.challenge_expiration_sleep_interval_secs),
            &exit,
            health_manager.handle(),
        );

        info!("starting relayer at: {:?}", server_addr);
        Server::builder()
            .add_service(RelayerServer::with_interceptor(
                relayer_svc,
                AuthInterceptor::new(verifying_key.clone(), AlgorithmType::Rs256),
            ))
            .add_service(AuthServiceServer::new(auth_svc))
            .serve_with_shutdown(server_addr, shutdown_signal(exit.clone()))
            .await
            .expect("serve relayer");
    });

    exit.store(true, Ordering::Relaxed);

    tpu.join().unwrap();
    health_manager.join().unwrap();
    leader_cache.join().unwrap();
    for t in forward_and_delay_threads {
        t.join().unwrap();
    }
    lookup_table_refresher.join().unwrap();
    block_engine_forwarder.join();
}

pub async fn shutdown_signal(exit: Arc<AtomicBool>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    exit.store(true, Ordering::Relaxed);
    warn!("signal received, starting graceful shutdown");
}

enum ValidatorStore {
    LeaderSchedule(LeaderScheduleUpdatingHandle),
    UserDefined(HashSet<Pubkey>),
}

struct ValidatorAutherImpl {
    store: ValidatorStore,
}

impl ValidatorAuther for ValidatorAutherImpl {
    fn is_authorized(&self, pubkey: &Pubkey) -> bool {
        match &self.store {
            ValidatorStore::LeaderSchedule(cache) => cache.is_scheduled_validator(pubkey),
            ValidatorStore::UserDefined(pubkeys) => pubkeys.contains(pubkey),
        }
    }
}

fn start_lookup_table_refresher(
    rpc_load_balancer: &Arc<LoadBalancer>,
    lookup_table: &Arc<DashMap<Pubkey, AddressLookupTableAccount>>,
    refresh_duration: Duration,
    exit: &Arc<AtomicBool>,
) -> JoinHandle<()> {
    let rpc_load_balancer = rpc_load_balancer.clone();
    let exit = exit.clone();
    let lookup_table = lookup_table.clone();

    thread::Builder::new()
        .name("lookup_table_refresher".to_string())
        .spawn(move || {
            // seed lookup table
            if let Err(e) = refresh_address_lookup_table(&rpc_load_balancer, &lookup_table) {
                error!("error refreshing address lookup table: {e:?}");
            }

            let tick_receiver = tick(Duration::from_secs(1));
            let mut last_refresh = Instant::now();

            while !exit.load(Ordering::Relaxed) {
                let _ = tick_receiver.recv();
                if last_refresh.elapsed() < refresh_duration {
                    continue;
                }

                let now = Instant::now();
                let refresh_result =
                    refresh_address_lookup_table(&rpc_load_balancer, &lookup_table);
                let updated_elapsed = now.elapsed().as_micros();
                match refresh_result {
                    Ok(_) => {
                        datapoint_info!(
                            "lookup_table_refresher-ok",
                            ("count", 1, i64),
                            ("lookup_table_size", lookup_table.len(), i64),
                            ("updated_elapsed_us", updated_elapsed, i64),
                        );
                    }
                    Err(e) => {
                        datapoint_error!(
                            "lookup_table_refresher-error",
                            ("count", 1, i64),
                            ("lookup_table_size", lookup_table.len(), i64),
                            ("updated_elapsed_us", updated_elapsed, i64),
                            ("error", e.to_string(), String),
                        );
                    }
                }
                last_refresh = Instant::now();
            }
        })
        .unwrap()
}

fn refresh_address_lookup_table(
    rpc_load_balancer: &Arc<LoadBalancer>,
    lookup_table: &DashMap<Pubkey, AddressLookupTableAccount>,
) -> solana_client::client_error::Result<()> {
    let rpc_client = rpc_load_balancer.rpc_client();

    let address_lookup_table =
        Pubkey::from_str("AddressLookupTab1e1111111111111111111111111").unwrap();
    let start = Instant::now();
    let accounts = rpc_client.get_program_accounts(&address_lookup_table)?;
    info!(
        "Fetched {} lookup tables from RPC in {:?}",
        accounts.len(),
        start.elapsed()
    );

    let mut new_pubkeys = HashSet::new();
    for (pubkey, account_data) in accounts {
        match AddressLookupTable::deserialize(&account_data.data) {
            Err(e) => {
                error!("error deserializing AddressLookupTable pubkey: {pubkey}, error: {e}");
            }
            Ok(table) => {
                debug!("lookup table loaded pubkey: {pubkey:?}, table: {table:?}");
                new_pubkeys.insert(pubkey);
                lookup_table.insert(
                    pubkey,
                    AddressLookupTableAccount {
                        key: pubkey,
                        addresses: table.addresses.to_vec(),
                    },
                );
            }
        }
    }

    // remove all the closed lookup tables
    lookup_table.retain(|pubkey, _| new_pubkeys.contains(pubkey));

    Ok(())
}
