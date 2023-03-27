use std::{
    collections::HashSet,
    fs::File,
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr},
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
use crossbeam_channel::{tick, unbounded};
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
use tokio::{runtime::Builder, signal, sync::mpsc::channel};
use tonic::transport::Server;

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

    /// Block engine address
    #[arg(long, env)]
    block_engine_url: String,

    /// Authentication service address of the block-engine. Keypairs are authenticated against the block engine
    #[arg(long, env)]
    block_engine_auth_service_url: String,

    /// Keypair path
    #[arg(long, env)]
    keypair_path: String,

    /// Validators allowed to authenticate and connect to the relayer, comma separated.
    /// If null then all validators on the leader schedule shall be permitted.
    #[arg(long, env, value_delimiter = ',', value_parser = Pubkey::from_str)]
    allowed_validators: Option<Vec<Pubkey>>,

    /// The private key used to sign tokens by this server.
    #[arg(long, env)]
    signing_key_pem_path: String,

    /// The public key used to verify tokens by this and other services.
    #[arg(long, env)]
    verifying_key_pem_path: String,

    /// Specifies how long access_tokens are valid for, expressed in seconds.
    #[arg(long, env, default_value_t = 1_800)]
    access_token_ttl_secs: i64,

    /// Specifies how long access_tokens are valid for, expressed in seconds.
    #[arg(long, env, default_value_t = 180_000)]
    refresh_token_ttl_secs: i64,

    /// Specifies how long challenges are valid for, expressed in seconds.
    #[arg(long, env, default_value_t = 1800)]
    challenge_ttl_secs: i64,

    /// The interval at which challenges are checked for expiration.
    #[arg(long, env, default_value_t = 180)]
    challenge_expiration_sleep_interval: i64,

    /// How long it takes to miss a slot for the system to be considered unhealthy
    #[arg(long, env, default_value_t = 10)]
    missing_slot_unhealthy_secs: u64,

    /// Solana cluster name (mainnet-beta, testnet, devnet, ...)
    #[arg(long, env)]
    cluster: String,

    /// Region (amsterdam, dallas, frankfurt, ...)
    #[arg(long, env)]
    region: String,

    /// Accounts of interest cache TTL. Note this must play nicely with the refresh period that
    /// block engine uses to send full updates.
    #[arg(long, env, default_value_t = 300)]
    aoi_cache_ttl_s: u64,

    /// How frequently to refresh the address lookup table accounts
    #[arg(long, env, default_value_t = 30)]
    lookup_table_refresh_s: u64,
}

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

    let (rpc_load_balancer, health_manager_slot_receiver) =
        LoadBalancer::new(&servers, &exit, args.cluster.clone(), args.region.clone());
    let rpc_load_balancer = Arc::new(rpc_load_balancer);

    // Lookup table refresher
    let address_lookup_table_cache: DashMap<Pubkey, AddressLookupTableAccount> = DashMap::new();
    let lookup_table_refresher = start_lookup_table_refresher(
        &rpc_load_balancer,
        &address_lookup_table_cache,
        args.lookup_table_refresh_s,
        &exit,
        args.cluster.clone(),
        args.region.clone(),
    );

    let (tpu, packet_receiver) = Tpu::new(
        sockets.tpu_sockets,
        &exit,
        &keypair,
        &sockets.tpu_ip,
        &sockets.tpu_fwd_ip,
        &rpc_load_balancer,
    );

    let leader_cache = LeaderScheduleCacheUpdater::new(
        &rpc_load_balancer,
        &exit,
        args.cluster.clone(),
        args.region.clone(),
    );

    // receiver tracked as relayer_impl-channel_stats.delay_packet_receiver-len
    let (delay_sender, delay_receiver) = unbounded();

    // NOTE: make sure the channel here isn't too big because it will get backed up
    // with packets when the block engine isn't connected
    // tracked as forwarder_metrics.block_engine_sender-len
    let (block_engine_sender, block_engine_receiver) =
        channel(jito_transaction_relayer::forwarder::BLOCK_ENGINE_QUEUE_CAPACITY);

    let forward_and_delay_threads = start_forward_and_delay_thread(
        packet_receiver,
        delay_sender,
        args.packet_delay_ms,
        block_engine_sender,
        1,
        &exit,
        args.cluster.clone(),
        args.region.clone(),
    );
    let block_engine_forwarder = BlockEngineRelayerHandler::new(
        args.block_engine_url,
        args.block_engine_auth_service_url,
        block_engine_receiver,
        keypair,
        exit.clone(),
        args.cluster.clone(),
        args.region.clone(),
        args.aoi_cache_ttl_s,
        address_lookup_table_cache,
    );

    // receiver tracked as relayer_impl-channel_stats.slot_receiver-len
    let (slot_sender, slot_receiver) = unbounded();
    let health_manager = HealthManager::new(
        health_manager_slot_receiver,
        slot_sender,
        Duration::from_secs(args.missing_slot_unhealthy_secs),
        exit.clone(),
        args.cluster.clone(),
        args.region.clone(),
    );

    let server_addr = SocketAddr::new(args.grpc_bind_ip, args.grpc_bind_port);
    let relayer_svc = RelayerImpl::new(
        slot_receiver,
        delay_receiver,
        leader_cache.handle(),
        exit.clone(),
        public_ip,
        args.tpu_port,
        args.tpu_fwd_port,
        health_manager.handle(),
        args.cluster.clone(),
        args.region.clone(),
    );

    let mut buf = Vec::new();
    File::open(args.signing_key_pem_path)
        .expect("signing key file to be found")
        .read_to_end(&mut buf)
        .expect("to read signing key file");
    let signing_key = PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::private_key_from_pem(&buf[..]).unwrap(),
    };

    let mut buf = Vec::new();
    File::open(args.verifying_key_pem_path)
        .expect("verifying key file to be found")
        .read_to_end(&mut buf)
        .expect("to read verifying key file");
    let verifying_key = Arc::new(PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::public_key_from_pem(&buf[..]).unwrap(),
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
            Duration::from_secs(args.access_token_ttl_secs as u64),
            Duration::from_secs(args.refresh_token_ttl_secs as u64),
            Duration::from_secs(args.challenge_ttl_secs as u64),
            Duration::from_secs(args.challenge_expiration_sleep_interval as u64),
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
            .serve_with_shutdown(server_addr, shutdown_signal())
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

pub async fn shutdown_signal() {
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
    lookup_table: &DashMap<Pubkey, AddressLookupTableAccount>,
    lookup_table_refresh_s: u64,
    exit: &Arc<AtomicBool>,
    cluster: String,
    region: String,
) -> JoinHandle<()> {
    let rpc_load_balancer = rpc_load_balancer.clone();
    let lookup_table = lookup_table.clone();
    let exit = exit.clone();

    thread::Builder::new()
        .name("lookup_table_refresher".to_string())
        .spawn(move || {
            let refresh_duration = Duration::from_secs(lookup_table_refresh_s);

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
                        datapoint_info!("lookup_table_refresher-ok",
                            "cluster" => cluster,
                            "region" => region,
                            ("count", 1, i64),
                            ("lookup_table_size", lookup_table.len(), i64),
                            ("updated_elapsed_us", updated_elapsed, i64),
                        );
                    }
                    Err(e) => {
                        datapoint_error!("lookup_table_refresher-error",
                            "cluster" => cluster,
                            "region" => region,
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
