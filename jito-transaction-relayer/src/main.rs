use clap::Parser;
use std::{
    net::{IpAddr},
    // str::FromStr,
    sync::{
        atomic::{AtomicBool},
        Arc,
    },
    thread::{sleep},
    time::{Duration},
};
use jito_fetch_verify::fetch_verify::FetchVerify;
use solana_net_utils::multi_bind_in_range;

use jito_transaction_relayer::*;

const COALESCE_MS: u64 = 5;
const NUM_SOCKETS: usize = 32;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {

    /// IP address to bind to for transaction packets
    #[clap( long, env)]
    tpu_ip: IpAddr,

    /// Port to bind to for tpu packets
    #[clap( long, env)]
    tpu_port: u16,

    /// Port to bind to for tpu fwd packets
    #[clap( long, env)]
    tpu_fwd_port: u16,
}

fn main() {

    //  ************** Following Chunk from Transaction Receiver Service ****************
    let args = Args::parse();

    let exit = Arc::new(AtomicBool::new(false));

    let (tpu_bind_port, tpu_sockets) =
        multi_bind_in_range(args.tpu_ip, (args.tpu_port, args.tpu_port + 1), NUM_SOCKETS)
            .expect("to bind tpu sockets");

    assert_eq!(tpu_bind_port, args.tpu_port);

    let (tpu_bind_fwd_port, tpu_fwd_sockets) = multi_bind_in_range(
        args.tpu_ip,
        (args.tpu_fwd_port, args.tpu_fwd_port + 1),
        NUM_SOCKETS,
    )
        .expect("to bind tpu_forward sockets");
    assert_eq!(tpu_bind_fwd_port, args.tpu_fwd_port);

    let (fetch_verify, verified_rx) =
        FetchVerify::new(tpu_sockets, tpu_fwd_sockets, &exit, COALESCE_MS);


    // sleep(Duration::from_secs(5));
    // test_fetch_verify(verified_rx, &exit, args.tpu_ip, args.tpu_port);

    fetch_verify.join();

}



