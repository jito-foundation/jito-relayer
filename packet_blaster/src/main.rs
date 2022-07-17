use std::{
    io,
    net::{SocketAddr, UdpSocket},
    str::FromStr,
    sync::Arc,
    thread::Builder,
    time::{Duration, Instant},
};

use bincode::serialize;
use clap::Parser;
use log::*;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    signature::{Keypair, Signature, Signer},
    system_transaction::transfer,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// RPC address
    #[clap(long, env, value_parser, default_value = "http://127.0.0.1:8899")]
    rpc_addr: String,

    /// Number of keypairs
    #[clap(long, env, value_parser, default_value_t = 10)]
    num_keypairs: u64,

    /// Socket address for relayer TPU
    #[clap(long, env, value_parser, default_value = "127.0.0.1:11222")]
    tpu_addr: String,
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let keypairs: Vec<_> = (0..args.num_keypairs)
        .map(|_| Arc::new(Keypair::new()))
        .collect();

    let pubkeys: Vec<_> = keypairs.iter().map(|kp| kp.pubkey()).collect();
    let mut pubkeys_str = pubkeys.iter().fold(String::new(), |mut s, pubkey| {
        s.push_str(&format!("{},", pubkey.to_string()));
        s
    });
    let _ = pubkeys_str.pop();
    info!("using keypairs: {:?}", pubkeys_str);

    let client = Arc::new(RpcClient::new(&args.rpc_addr));
    assert!(request_and_confirm_airdrop(&client, &pubkeys));

    let tpu_addr = SocketAddr::from_str(&args.tpu_addr).unwrap();
    let threads: Vec<_> = keypairs
        .into_iter()
        .map(|keypair| {
            let client = Arc::new(RpcClient::new(&args.rpc_addr));
            Builder::new()
                .spawn(move || {
                    let udp_sender = UdpSocket::bind("0.0.0.0:0").unwrap();
                    let mut last_blockhash_refresh = Instant::now();
                    let mut latest_blockhash = client.get_latest_blockhash().unwrap();
                    let mut last_count = 0;
                    info!("sending packets...");
                    let mut count: u64 = 0;
                    loop {
                        if last_blockhash_refresh.elapsed() > Duration::from_secs(5) {
                            let packets_per_second = (count - last_count) as f64
                                / last_blockhash_refresh.elapsed().as_secs_f64();
                            info!(
                                "packets sent/s: {:.2} ({} total)",
                                packets_per_second, count
                            );

                            last_blockhash_refresh = Instant::now();
                            latest_blockhash = client.get_latest_blockhash().unwrap();
                            last_count = count;
                        }

                        let serialized_txs: Vec<Vec<u8>> = (0..10)
                            .map(|_| {
                                count += 1;
                                serialize(&transfer(
                                    &keypair,
                                    &keypair.pubkey(),
                                    count as u64,
                                    latest_blockhash,
                                ))
                                .unwrap()
                            })
                            .collect();

                        let _: Vec<io::Result<usize>> = serialized_txs
                            .iter()
                            .map(|tx| udp_sender.send_to(tx, tpu_addr))
                            .collect();
                    }
                })
                .unwrap()
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }
}

fn request_and_confirm_airdrop(client: &RpcClient, pubkeys: &[solana_sdk::pubkey::Pubkey]) -> bool {
    let sigs: Vec<_> = pubkeys
        .iter()
        .map(|pubkey| client.request_airdrop(pubkey, 100_000_000_000))
        .collect();

    if sigs.iter().any(|s| s.is_err()) {
        return false;
    }
    let sigs: Vec<Signature> = sigs.into_iter().map(|s| s.unwrap()).collect();

    let now = Instant::now();
    while now.elapsed() < Duration::from_secs(20) {
        let r = client.get_signature_statuses(&sigs).expect("got statuses");
        if r.value.iter().all(|s| s.is_some()) {
            return true;
        }
    }
    false
}
