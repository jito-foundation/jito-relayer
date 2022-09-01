use std::{
    collections::HashSet,
    fs::File,
    io::Read,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use clap::Parser;
use crossbeam_channel::unbounded;
use jito_block_engine::block_engine::BlockEngineRelayerHandler;
use jito_core::tpu::{Tpu, TpuSockets};
use jito_relayer::{
    auth_service::{AuthServiceImpl, ValidatorAuther},
    relayer::RelayerImpl,
    schedule_cache::{LeaderScheduleCacheUpdater, LeaderScheduleUpdatingHandle},
    start_server,
};
use jito_rpc::load_balancer::LoadBalancer;
use jito_transaction_relayer::forwarder::start_forward_and_delay_thread;
use jwt::PKeyWithDigest;
use log::info;
use openssl::{hash::MessageDigest, pkey::PKey};
use solana_net_utils::multi_bind_in_range;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
};
use tokio::sync::mpsc::channel;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// IP address to bind to for transaction packets
    #[clap(long, env, value_parser, default_value_t = IpAddr::from_str("127.0.0.1").unwrap())]
    tpu_bind_ip: IpAddr,

    /// Port to bind to for tpu packets
    #[clap(long, env, value_parser, default_value_t = 11_222)]
    tpu_port: u16,

    /// Port to bind to for tpu fwd packets
    #[clap(long, env, value_parser, default_value_t = 11_223)]
    tpu_fwd_port: u16,

    /// Port to bind to for tpu packets
    #[clap(long, env, value_parser, default_value_t = 11_228)]
    tpu_quic_port: u16,

    /// Port to bind to for tpu fwd packets
    #[clap(long, env, value_parser, default_value_t = 11_229)]
    tpu_quic_fwd_port: u16,

    /// Bind IP address for GRPC server
    #[clap(long, env, value_parser, default_value_t = IpAddr::from_str("127.0.0.1").unwrap())]
    grpc_bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[clap(long, env, value_parser, default_value_t = 11_226)]
    grpc_bind_port: u16,

    /// Number of TPU threads
    #[clap(long, env, value_parser, default_value_t = 32)]
    num_tpu_binds: usize,

    /// Number of TPU forward threads
    #[clap(long, env, value_parser, default_value_t = 16)]
    num_tpu_fwd_binds: usize,

    /// RPC servers as a space-separated list. Shall be same position as websocket equivalent below
    #[clap(long, env, value_parser, default_value = "http://127.0.0.1:8899")]
    rpc_servers: String,

    /// Websocket servers as a space-separated list. Shall be same position as RPC equivalent above
    #[clap(long, env, value_parser, default_value = "ws://127.0.0.1:8900")]
    websocket_servers: String,

    /// This is the IP address that will be shared with the validator. The validator will
    /// tell the rest of the network to send packets here.
    #[clap(long, env, value_parser, default_value_t = IpAddr::from_str("127.0.0.1").unwrap())]
    public_ip: IpAddr,

    /// Packet delay in milliseconds
    #[clap(long, env, value_parser, default_value_t = 200)]
    packet_delay_ms: u32,

    /// Block engine address
    #[clap(long, env, value_parser, default_value = "http://127.0.0.1:13334")]
    block_engine_url: String,

    /// Authentication service address of the block-engine. Keypairs are authenticated against the block engine
    #[clap(long, env, value_parser, default_value = "http://127.0.0.1:14444")]
    block_engine_auth_service_url: String,

    /// Keypair path
    #[clap(long, env, value_parser, default_value = "/etc/solana/id.json")]
    keypair_path: String,

    /// Validators allowed to authenticate and connect to the relayer.
    /// If null then all validators on the leader schedule shall be permitted.
    #[clap(long, env)]
    allowed_validators: Option<Vec<String>>,

    /// The private key used to sign tokens by this server.
    #[clap(long, env)]
    signing_key_pem_path: String,

    /// The public key used to verify tokens by this and other services.
    #[clap(long, env)]
    verifying_key_pem_path: String,

    /// Specifies how long access_tokens are valid for, expressed in seconds.
    #[clap(long, env, default_value_t = 1800)]
    access_token_ttl_secs: i64,

    /// Specifies how long access_tokens are valid for, expressed in seconds.
    #[clap(long, env, default_value_t = 180000)]
    refresh_token_ttl_secs: i64,

    /// Specifies how long challenges are valid for, expressed in seconds.
    #[clap(long, env, default_value_t = 1800)]
    challenge_ttl_secs: i64,

    /// The interval at which challenges are checked for expiration.
    #[clap(long, env, default_value_t = 180)]
    challenge_expiration_sleep_interval: i64,
}

struct Sockets {
    tpu_sockets: TpuSockets,
    tpu_ip: IpAddr,
    tpu_fwd_ip: IpAddr,
}

fn get_sockets(args: &Args) -> Sockets {
    let (tpu_bind_port, transactions_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_port, args.tpu_port + 1),
        args.num_tpu_binds,
    )
    .expect("to bind tpu sockets");

    let (tpu_bind_fwd_port, transactions_forward_sockets) = multi_bind_in_range(
        args.tpu_bind_ip,
        (args.tpu_fwd_port, args.tpu_fwd_port + 1),
        args.num_tpu_fwd_binds,
    )
    .expect("to bind tpu_forward sockets");

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

    assert_eq!(tpu_bind_port, args.tpu_port);
    assert_eq!(tpu_bind_fwd_port, args.tpu_fwd_port);
    assert_eq!(tpu_quic_bind_port, args.tpu_quic_port);
    assert_eq!(tpu_fwd_quic_bind_port, args.tpu_quic_fwd_port);
    assert_eq!(tpu_bind_port + 6, tpu_quic_bind_port); // QUIC is expected to be at TPU + 6
    assert_eq!(tpu_bind_fwd_port + 6, tpu_fwd_quic_bind_port); // QUIC is expected to be at TPU + 6

    Sockets {
        tpu_sockets: TpuSockets {
            transactions_sockets,
            transactions_forward_sockets,
            transactions_quic_sockets: tpu_quic_sockets.pop().unwrap(),
            transactions_forwards_quic_sockets: tpu_fwd_quic_sockets.pop().unwrap(),
        },
        tpu_ip: IpAddr::from_str("0.0.0.0").unwrap(),
        tpu_fwd_ip: IpAddr::from_str("0.0.0.0").unwrap(),
    }
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let sockets = get_sockets(&args);

    let keypair =
        Arc::new(read_keypair_file(args.keypair_path).expect("keypair file does not exist"));
    solana_metrics::set_host_id(keypair.pubkey().to_string());
    info!("Relayer started with pubkey: {}", keypair.pubkey());

    let exit = Arc::new(AtomicBool::new(false));

    let rpc_servers: Vec<String> = args.rpc_servers.split(" ").map(String::from).collect();
    let websocket_servers: Vec<String> = args
        .websocket_servers
        .split(" ")
        .map(String::from)
        .collect();

    assert!(!rpc_servers.is_empty(), "num rpc servers >= 1");
    assert_eq!(
        rpc_servers.len(),
        websocket_servers.len(),
        "num rpc servers = num websocket servers"
    );

    let servers: Vec<(String, String)> = rpc_servers
        .into_iter()
        .zip(websocket_servers.into_iter())
        .collect();

    let (rpc_load_balancer, slot_receiver) = LoadBalancer::new(&servers, &exit);
    let rpc_load_balancer = Arc::new(Mutex::new(rpc_load_balancer));

    let (tpu, packet_receiver) = Tpu::new(
        sockets.tpu_sockets,
        &exit,
        5,
        &keypair,
        &sockets.tpu_ip,
        &sockets.tpu_fwd_ip,
        &rpc_load_balancer,
    );

    let leader_cache = LeaderScheduleCacheUpdater::new(&rpc_load_balancer, exit.clone());

    let (delay_sender, delay_receiver) = unbounded();

    // NOTE: make sure the channel here isn't too big because it will get backed up
    // with packets when the block engine isn't connected
    let (block_engine_sender, block_engine_receiver) = channel(1000);

    let forward_and_delay_threads = start_forward_and_delay_thread(
        packet_receiver,
        delay_sender,
        args.packet_delay_ms,
        block_engine_sender,
        1,
    );
    let block_engine_forwarder = BlockEngineRelayerHandler::new(
        args.block_engine_url,
        args.block_engine_auth_service_url,
        block_engine_receiver,
        keypair,
    );

    let server_addr = SocketAddr::new(args.grpc_bind_ip, args.grpc_bind_port);
    let relayer_svc = RelayerImpl::new(
        slot_receiver,
        delay_receiver,
        leader_cache.handle(),
        exit.clone(),
        args.public_ip,
        args.tpu_port,
        args.tpu_fwd_port,
    );

    let auth_svc = {
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
        let verifying_key = PKeyWithDigest {
            digest: MessageDigest::sha256(),
            key: PKey::public_key_from_pem(&buf[..]).unwrap(),
        };

        let validator_store = if let Some(pubkeys) = args.allowed_validators {
            let pubkeys = pubkeys
                .into_iter()
                .map(|pk| {
                    Pubkey::from_str(&pk)
                        .unwrap_or_else(|_| panic!("failed to parse pubkey from string: {}", pk))
                })
                .collect::<HashSet<Pubkey>>();
            ValidatorStore::UserDefined(pubkeys)
        } else {
            ValidatorStore::LeaderSchedule(leader_cache.handle())
        };

        AuthServiceImpl::new(
            ValidatorAutherImpl {
                store: validator_store,
            },
            signing_key,
            Arc::new(verifying_key),
            Duration::from_secs(args.access_token_ttl_secs as u64),
            Duration::from_secs(args.refresh_token_ttl_secs as u64),
            Duration::from_secs(args.challenge_ttl_secs as u64),
            Duration::from_secs(args.challenge_expiration_sleep_interval as u64),
        )
    };

    start_server(auth_svc, relayer_svc, server_addr);

    exit.store(true, Ordering::Relaxed);

    tpu.join().unwrap();
    leader_cache.join().unwrap();
    for t in forward_and_delay_threads {
        t.join().unwrap();
    }
    block_engine_forwarder.join().unwrap();
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
