use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use bincode::serialize;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    signature::{Keypair, Signature, Signer},
    system_transaction::transfer,
};

fn main() {
    env_logger::init();

    let client = RpcClient::new("http://127.0.0.1:8899");
    let keypair = Keypair::new();
    assert!(request_and_confirm_airdrop(&client, &[keypair.pubkey()]));

    let exit = Arc::new(AtomicBool::new(false));

    const NUM_THREADS: usize = 1;
    const NUM_PACKETS_PER_ITER: usize = 1000;
    const PACKET_RATE_PER_SEC_PER_SERVER: usize = 1000;
    const PACKET_RATE_PER_THREAD: usize =
        ((PACKET_RATE_PER_SEC_PER_SERVER as f32) / (NUM_THREADS as f32)) as usize;
    const LOOP_DURATION: f32 = (NUM_PACKETS_PER_ITER as f32) / (PACKET_RATE_PER_THREAD as f32);

    let tpu_ip: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    // let (tpu_port, tpu_sockets) =
    //     multi_bind_in_range(tpu_ip, (40_000, 41_000), 32).expect("tpu multi_bind");
    let tpu_addr = SocketAddr::new(tpu_ip, 10_500);

    let kp_string = keypair.to_base58_string();
    let client = Arc::new(client);
    let send_threads: Vec<JoinHandle<()>> = (0..NUM_THREADS)
        .map(|_| {
            let send_exit = exit.clone();
            let client = client.clone();
            let keypair = Keypair::from_base58_string(&kp_string.clone());
            Builder::new()
                .name(String::from("send_thread"))
                .spawn(move || {
                    let udp_sender = UdpSocket::bind("0.0.0.0:0").unwrap();
                    loop {
                        let exec_start = Instant::now();
                        if send_exit.load(Ordering::Acquire) {
                            break;
                        }

                        let latest = client.get_latest_blockhash().unwrap();

                        // transfer()
                        // TODO (LB): need to generate this faster
                        let serialized_txs: Vec<Vec<u8>> = (0..NUM_PACKETS_PER_ITER)
                            .map(|c| {
                                serialize(&transfer(
                                    &keypair,
                                    &keypair.pubkey(),
                                    (c * 100) as u64,
                                    latest,
                                ))
                                .unwrap()
                            })
                            .collect();

                        let _: Vec<io::Result<usize>> = serialized_txs
                            .iter()
                            .map(|tx| udp_sender.send_to(tx, tpu_addr))
                            .collect();

                        let sleep_duration = Duration::from_secs_f32(LOOP_DURATION)
                            .checked_sub(exec_start.elapsed())
                            .unwrap_or_else(|| Duration::from_secs(0));

                        sleep(sleep_duration);
                    }
                })
                .unwrap()
        })
        .collect();

    // Run the test for this long
    sleep(Duration::from_secs(60));

    exit.store(true, Ordering::Relaxed);
    for s in send_threads {
        let _ = s.join();
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
