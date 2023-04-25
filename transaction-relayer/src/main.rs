use std::{
    collections::HashSet,
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
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

use clap::Parser;
use crossbeam_channel::tick;
use dashmap::DashMap;
use jito_block_engine::block_engine::BlockEngineRelayerHandler;
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
use jito_rpc::load_balancer::LoadBalancer;
use jito_transaction_relayer::forwarder::start_forward_and_delay_thread;
use jwt::{AlgorithmType, PKeyWithDigest};
use log::{debug, error, info, warn};
use openssl::{hash::MessageDigest, pkey::PKey};
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_metrics::{datapoint_error, datapoint_info};
use solana_net_utils::multi_bind_in_range;
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
    /// IP address to bind to for transaction packets
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    tpu_bind_ip: IpAddr,

    /// Port to bind to advertise for TPU
    /// NOTE: There is no longer a socket created at this port since UDP transaction receiving is
    /// deprecated.
    #[arg(long, env, default_value_t = 11_222)]
    tpu_port: u16,

    /// Port to bind to for tpu fwd packets
    /// NOTE: There is no longer a socket created at this port since UDP transaction receiving is
    /// deprecated.
    #[arg(long, env, default_value_t = 11_223)]
    tpu_fwd_port: u16,

    /// Port to bind to for tpu packets. Needs to be tpu_port + 6
    #[arg(long, env, default_value_t = 11_228)]
    tpu_quic_port: u16,

    /// Port to bind to for tpu fwd packets. Needs to be tpu_fwd_port + 6
    #[arg(long, env, default_value_t = 11_229)]
    tpu_quic_fwd_port: u16,

    /// Bind IP address for GRPC server
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    grpc_bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[arg(long, env, default_value_t = 11_226)]
    grpc_bind_port: u16,

    /// Number of TPU threads
    #[arg(long, env, default_value_t = 32)]
    num_tpu_binds: usize,

    /// Number of TPU forward threads
    #[arg(long, env, default_value_t = 16)]
    num_tpu_fwd_binds: usize,

    /// RPC servers as a space-separated list. Shall be same position as websocket equivalent below
    #[arg(
        long,
        env,
        value_delimiter = ' ',
        required = true,
        default_value = "http://127.0.0.1:8899"
    )]
    rpc_servers: Vec<String>,

    /// Websocket servers as a space-separated list. Shall be same position as RPC equivalent above
    #[arg(
        long,
        env,
        value_delimiter = ' ',
        required = true,
        default_value = "ws://127.0.0.1:8900"
    )]
    websocket_servers: Vec<String>,

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
    block_engine_url: String,

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
}

#[derive(Debug)]
struct Sockets {
    tpu_sockets: TpuSockets,
    tpu_ip: IpAddr,
    tpu_fwd_ip: IpAddr,
}

fn get_sockets(args: &Args) -> Sockets {
    let (tpu_quic_bind_port, mut tpu_quic_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_quic_port, args.tpu_quic_port + 1),
        1,
    )
    .expect("to bind tpu_quic sockets");

    let (tpu_fwd_quic_bind_port, mut tpu_fwd_quic_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_quic_fwd_port, args.tpu_quic_fwd_port + 1),
        1,
    )
    .expect("to bind tpu_quic sockets");

    assert_eq!(tpu_quic_bind_port, args.tpu_quic_port);
    assert_eq!(tpu_fwd_quic_bind_port, args.tpu_quic_fwd_port);
    assert_eq!(args.tpu_port + 6, tpu_quic_bind_port); // QUIC is expected to be at TPU + 6
    assert_eq!(args.tpu_fwd_port + 6, tpu_fwd_quic_bind_port); // QUIC is expected to be at TPU forward + 6

    Sockets {
        tpu_sockets: TpuSockets {
            transactions_quic_sockets: tpu_quic_sockets.pop().unwrap(),
            transactions_forwards_quic_sockets: tpu_fwd_quic_sockets.pop().unwrap(),
        },
        tpu_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        tpu_fwd_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    }
}

fn main() {
    env_logger::builder().format_timestamp_millis().init();

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
        info!("reading public ip from ifconfig.me...");
        let response = reqwest::blocking::get("https://ifconfig.me")
            .expect("response from ifconfig.me")
            .text()
            .expect("public ip response");
        response.parse().unwrap()
    };
    info!("public ip: {:?}", public_ip);

    // Supporting IPV6 addresses is a DOS vector since they are cheap and there's a much larger amount of them.
    // The DOS is specifically with regards to the challenges queue filling up and starving other legitimate
    // challenge requests.
    assert!(args.grpc_bind_ip.is_ipv4(), "must bind to IPV4 address");

    let sockets = get_sockets(&args);
    info!("Relayer listening at: {sockets:?}");

    let keypair =
        Arc::new(read_keypair_file(args.keypair_path).expect("keypair file does not exist"));
    solana_metrics::set_host_id(format!(
        "{}_{}",
        hostname::get().unwrap().to_str().unwrap(), // hostname should follow RFC1123
        keypair.pubkey()
    ));
    info!("Relayer started with pubkey: {}", keypair.pubkey());

    let exit = graceful_panic(None);

    assert_eq!(
        args.rpc_servers.len(),
        args.websocket_servers.len(),
        "number of rpc servers must match number of websocket servers"
    );

    let servers: Vec<(String, String)> = args
        .rpc_servers
        .into_iter()
        .zip(args.websocket_servers.into_iter())
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

    let (tpu, verified_receiver) = Tpu::new(
        sockets.tpu_sockets,
        &exit,
        &keypair,
        &sockets.tpu_ip,
        &sockets.tpu_fwd_ip,
        &rpc_load_balancer,
        &ofac_addresses,
        &address_lookup_table_cache,
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
        &exit,
    );
    let block_engine_forwarder = BlockEngineRelayerHandler::new(
        args.block_engine_url.clone(),
        args.block_engine_auth_service_url
            .unwrap_or(args.block_engine_url),
        block_engine_receiver,
        keypair,
        exit.clone(),
        args.aoi_cache_ttl_secs,
        address_lookup_table_cache,
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
        args.tpu_port,
        args.tpu_fwd_port,
        health_manager.handle(),
        exit.clone(),
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
        Some(pubkeys) => ValidatorStore::UserDefined(HashSet::from_iter(pubkeys.into_iter())),
        None => ValidatorStore::LeaderSchedule(leader_cache.handle()),
    };

    let rt = Builder::new_multi_thread().enable_all().build().unwrap();
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
    block_engine_forwarder.join().unwrap();
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
