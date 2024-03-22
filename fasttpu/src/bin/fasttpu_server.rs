use std::{net::SocketAddr, ptr::null_mut, str::FromStr, sync::Arc};

use ed25519_dalek::Keypair;
use jito_fasttpu::{
    cnc::Cnc,
    server::{Server, ServerConfig, ServerMetrics},
};

fn main() {
    let keypair = Keypair::generate(&mut rand::rngs::OsRng);
    let cnc = Arc::new(Cnc::new());
    let mut server = Server::new(
        &ServerConfig {
            tile_cnt: 1,
            listen_addr: SocketAddr::from_str("127.0.0.1:8080").unwrap(),
            conn_cnt: 10000,
        },
        Arc::clone(&cnc),
        &keypair,
    )
    .expect("Failed to init server");
    server.start();

    // Mem stats
    unsafe {
        let mut cptr: *mut i8 = null_mut();
        let mut size: usize = 0;
        let fd = libc::open_memstream(&mut cptr, &mut size);
        let info = libc::malloc_info(0, fd);
        libc::fclose(fd);
        println!(
            "{}",
            String::from_utf8_unchecked(
                std::slice::from_raw_parts(cptr as *const u8, size).to_vec()
            )
        );
        println!("{}", std::mem::size_of::<quiche::Connection>());
    };

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut metrics = ServerMetrics::default();
        for sub_metrics in server.tile_metrics.iter() {
            metrics += sub_metrics.as_ref().clone();
        }
        println!("{:?}", metrics);
    }
}
