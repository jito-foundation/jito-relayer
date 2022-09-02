pub mod auth_interceptor;
pub mod auth_service;
pub mod relayer;
pub mod schedule_cache;

use std::net::SocketAddr;

use jito_protos::{
    auth::auth_service_server::AuthServiceServer, relayer::relayer_server::RelayerServer,
};
use jwt::AlgorithmType;
use log::info;
use tokio::runtime::Builder;
use tonic::transport::Server;

use crate::{
    auth_interceptor::AuthInterceptor,
    auth_service::{AuthServiceImpl, ValidatorAuther},
    relayer::RelayerImpl,
};

pub fn start_server<V: ValidatorAuther>(
    auth_svc: AuthServiceImpl<V>,
    relayer_svc: RelayerImpl,
    addr: SocketAddr,
) {
    // Todo (JL): Async Exit here?
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        info!("starting relayer at: {:?}", addr);

        Server::builder()
            .add_service(RelayerServer::with_interceptor(
                relayer_svc,
                AuthInterceptor::new(auth_svc.verifying_key.clone(), AlgorithmType::Rs256),
            ))
            .add_service(AuthServiceServer::new(auth_svc))
            .serve(addr)
            .await
            .expect("serve server");
    });
}
