use std::{net::SocketAddr, str::FromStr, sync::Arc};

use jito_fasttpu::{
    cnc::Cnc,
    server::{Server, ServerConfig, ServerMetrics},
};

fn main() {
    let cnc = Arc::new(Cnc::new());
    let mut server = Server::new(
        &ServerConfig {
            tile_cnt: 4,
            listen_addr: SocketAddr::from_str("127.0.0.1:8080").unwrap(),
        },
        Arc::clone(&cnc),
    )
    .expect("Failed to init server");
    server.start();

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut metrics = ServerMetrics::default();
        for sub_metrics in server.tile_metrics.iter() {
            metrics += sub_metrics.as_ref().clone();
        }
        println!("{:?}", metrics);
    }
}
