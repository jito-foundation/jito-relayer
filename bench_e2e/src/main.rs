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
    const NUM_PACKETS_PER_ITER: usize = 10;
    const PACKET_RATE_PER_SEC_PER_SERVER: usize = 10;
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

    // Connect to Relayer over GRPC
    // let rt = TokBuilder::new_multi_thread().enable_all().build().unwrap();
    // rt.block_on(async move {
    //     let mut client = ValidatorInterfaceClient::connect("http://0.0.0.0:42069")
    //         .await
    //         .unwrap();
    //     let mut request = client
    //         .subscribe_packets(SubscribePacketsRequest {})
    //         .await
    //         .unwrap()
    //         .into_inner();
    //
    //     // info!(
    //     //     "connected to {}, starting to receive stream of packets",
    //     //     server_ip
    //     // );
    //
    //     let mut last_hb_time = SystemTime::now();
    //     let mut first_iteration = true;
    //     loop {
    //         if let Ok(msg) = request.message().await {
    //             // let now = SystemTime::now();
    //             if let Some(msg) = msg {
    //                 match msg.msg {
    //                     Some(subscribe_packets_response::Msg::BatchList(batch)) => {
    //                         let batch_count = batch.batch_list.len();
    //                         let tx_count = batch
    //                             .batch_list
    //                             .iter()
    //                             .map(|b| b.packets.len())
    //                             .sum::<usize>();
    //                         info!("Received {} batches with {} txs!!!", batch_count, tx_count);
    //                         // let _ = stats_sender.send(Stats {
    //                         //     tx_count,
    //                         //     batch_count,
    //                         // });
    //                         // let header = msg.header.unwrap();
    //                         // let ts = SystemTime::try_from(header.ts.unwrap()).unwrap();
    //                         // NOTE: this isn't reliable when the server and client are
    //                         // on two separate servers unless you have good time sync
    //                         // if let Ok(elapsed) = now.duration_since(ts) {
    //                         //     info!("latency: {:?}ms", elapsed.as_millis());
    //                         // }
    //                     }
    //
    //                     Some(subscribe_packets_response::Msg::Heartbeat(_)) => {
    //                         let hb_time = SystemTime::now();
    //                         if first_iteration {
    //                             info!("First Heartbeat received from relayer at {:?}!!", hb_time);
    //                         } else {
    //                             info!("Thump!!");
    //                         }
    //                         let hb_elapsed = hb_time
    //                             .duration_since(last_hb_time)
    //                             .expect("couldn't get elapsed hb");
    //                         if !first_iteration && hb_elapsed > Duration::from_millis(1500) {
    //                             warn!("Missing Heartbeat signal!!  Need appropriate Handler");
    //                         }
    //                         first_iteration = false;
    //                         last_hb_time = hb_time;
    //                     }
    //                     _ => {
    //                         panic!("wtf?");
    //                     }
    //                 }
    //             } else {
    //                 warn!("done, got a non-message, exiting");
    //                 break;
    //             }
    //         } else {
    //             warn!("done, sender hung up, exiting");
    //             // break;
    //         }
    //     }
    // });

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
