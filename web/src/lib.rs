use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use axum::{routing::get, Extension, Json, Router};
use jito_relayer::health_manager::HealthState;
use serde::Serialize;

/// State object that exposes info inside relayer
pub struct RelayerState {
    pub slot_health: Arc<RwLock<HealthState>>,
    pub is_block_engine_connected: AtomicBool,
    pub validators_connected: Arc<RwLock<HashSet<Pubkey>>>,
}

#[derive(Serialize, Debug)]
pub struct RelayerStatus {}

/// Returns an axum router with endpoints to get status of relayer
pub fn build_relayer_router(state: Arc<RelayerState>) -> Router {
    /// Returns a simple string for relayer health
    async fn get_health(Extension(state): Extension<Arc<RelayerState>>) -> String {
        String::new()
    }

    /// Returns the status of different components inside the relayer
    async fn get_status(Extension(state): Extension<Arc<RelayerState>>) -> Json<RelayerStatus> {
        Json(RelayerStatus {})
    }

    // TODO (LB): add rate limits!!!!!
    Router::new()
        .route("/health", get(get_health))
        .route("/status", get(get_status))
        .layer(Extension(state))
}

/// Spawns the relayer webserver to serve HTTP requests against
pub async fn start_relayer_web_server(state: Arc<RelayerState>, addr: SocketAddr) {
    let app = build_relayer_router(state);
    let _ = axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await;
}
