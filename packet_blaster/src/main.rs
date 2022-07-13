use std::{
    io,
    net::{SocketAddr, UdpSocket},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use bincode::serialize;
use log::*;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    signature::{Keypair, Signature, Signer},
    system_transaction::transfer,
};

fn main() {
    env_logger::init();

    let client = Arc::new(RpcClient::new("http://127.0.0.1:8899"));
    let keypair = Arc::new(Keypair::new());

    assert!(request_and_confirm_airdrop(&client, &[keypair.pubkey()]));

    let tpu_addr = SocketAddr::from_str("127.0.0.1:11222").unwrap();

    let udp_sender = UdpSocket::bind("0.0.0.0:0").unwrap();

    let mut last_blockhash_refresh = Instant::now();
    let mut latest_blockhash = client.get_latest_blockhash().unwrap();
    let mut last_count = 0;

    info!("sending packets...");

    let mut count: u64 = 0;
    loop {
        if last_blockhash_refresh.elapsed() > Duration::from_secs(5) {
            let packets_per_second =
                (count - last_count) as f64 / last_blockhash_refresh.elapsed().as_secs_f64();
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
