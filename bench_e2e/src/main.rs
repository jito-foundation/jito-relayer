use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{sleep, Builder, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use bincode::serialize;
// use solana_net_utils::multi_bind_in_range;
use solana_perf::test_tx::test_tx;
use tokio::runtime::Builder as TokBuilder;

use jito_protos::validator_interface_service::{
    // subscribe_packets_response::Msg,
    subscribe_packets_response,
    validator_interface_client::ValidatorInterfaceClient,
    SubscribePacketsRequest,
    // SubscribePacketsResponse,
};

fn main() {
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
    let tpu_addr = SocketAddr::new(tpu_ip, 8005);

    let send_threads: Vec<JoinHandle<()>> = (0..NUM_THREADS)
        .map(|_| {
            let send_exit = exit.clone();
            Builder::new()
                .name(String::from("send_thread"))
                .spawn(move || {
                    let udp_sender = UdpSocket::bind("0.0.0.0:0").unwrap();
                    loop {
                        let exec_start = Instant::now();
                        if send_exit.load(Ordering::Acquire) {
                            break;
                        }

                        // TODO (LB): need to generate this faster
                        let serialized_txs: Vec<Vec<u8>> = (0..NUM_PACKETS_PER_ITER)
                            .map(|_| serialize(&test_tx()).unwrap())
                            .collect();

                        let _: Vec<io::Result<usize>> = serialized_txs
                            .iter()
                            .map(|tx| udp_sender.send_to(tx, tpu_addr))
                            .collect();

                        let sleep_duration = Duration::from_secs_f32(LOOP_DURATION)
                            .checked_sub(exec_start.elapsed())
                            .unwrap_or_else(|| Duration::from_secs(0));

                        // println!(
                        //     "Sent Packet Batch, sleeping for {} seconds",
                        //     sleep_duration.as_secs()
                        // );

                        sleep(sleep_duration);
                    }
                })
                .unwrap()
        })
        .collect();

    // Connect to Relayer over GRPC
    let rt = TokBuilder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async move {
        let mut client = ValidatorInterfaceClient::connect("http://0.0.0.0:42069")
            .await
            .unwrap();
        let mut request = client
            .subscribe_packets(SubscribePacketsRequest {})
            .await
            .unwrap()
            .into_inner();

        // info!(
        //     "connected to {}, starting to receive stream of packets",
        //     server_ip
        // );

        let mut last_hb_time = SystemTime::now();
        let mut first_iteration = true;
        loop {
            if let Ok(msg) = request.message().await {
                // let now = SystemTime::now();
                if let Some(msg) = msg {
                    match msg.msg {
                        Some(subscribe_packets_response::Msg::BatchList(batch)) => {
                            let batch_count = batch.batch_list.len();
                            let tx_count = batch
                                .batch_list
                                .iter()
                                .map(|b| b.packets.len())
                                .sum::<usize>();
                            println!("Received {} batches with {} txs!!!", batch_count, tx_count)
                            // let _ = stats_sender.send(Stats {
                            //     tx_count,
                            //     batch_count,
                            // });
                            // let header = msg.header.unwrap();
                            // let ts = SystemTime::try_from(header.ts.unwrap()).unwrap();
                            // NOTE: this isn't reliable when the server and client are
                            // on two separate servers unless you have good time sync
                            // if let Ok(elapsed) = now.duration_since(ts) {
                            //     info!("latency: {:?}ms", elapsed.as_millis());
                            // }
                        }

                        Some(subscribe_packets_response::Msg::Heartbeat(_)) => {
                            // println!("Got Heartbeat !!",)
                            let hb_time = SystemTime::now();
                            if first_iteration {
                                println!(
                                    "First Heartbeat received from relayer at {:?}!!",
                                    hb_time
                                );
                            }
                            let hb_elapsed = hb_time
                                .duration_since(last_hb_time)
                                .expect("couldn't get elapsded hb");
                            if !first_iteration && hb_elapsed > Duration::from_millis(1500) {
                                println!("Missing Heartbeat signal!!  Need appropriate Handler");
                            }
                            first_iteration = false;
                            last_hb_time = hb_time;
                        }
                        _ => {
                            panic!("wtf?");
                        }
                    }
                } else {
                    println!("Got something. Not a Message!!");
                    // warn!("done, exiting");
                    break;
                }
            } else {
                println!("request failed!!!");
                // warn!("done, exiting");
                break;
            }
        }
    });

    // Run the test for this long
    // sleep(Duration::from_secs(5));

    exit.store(true, Ordering::Relaxed);
    // for s in send_threads {
    //     let _ = s.join();
    // }
}
