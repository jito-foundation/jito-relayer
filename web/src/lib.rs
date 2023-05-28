use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use axum::{
    error_handling::HandleErrorLayer, http::StatusCode, routing::get, BoxError, Extension, Json,
    Router,
};
use jito_relayer::{health_manager::HealthState, relayer::RelayerHandle};
use log::debug;
use serde::Serialize;
use tower::{buffer::BufferLayer, limit::RateLimitLayer, ServiceBuilder};

/// State object that exposes info inside relayer
pub struct RelayerState {
    slot_health: Arc<RwLock<HealthState>>,
    is_connected_to_block_engine: Arc<AtomicBool>,
    relayer_handle: RelayerHandle,
}

impl RelayerState {
    pub fn new(
        slot_health: Arc<RwLock<HealthState>>,
        is_connected_to_block_engine: &Arc<AtomicBool>,
        relayer_handle: RelayerHandle,
    ) -> RelayerState {
        RelayerState {
            slot_health,
            is_connected_to_block_engine: is_connected_to_block_engine.clone(),
            relayer_handle,
        }
    }
}

#[derive(Serialize, Debug)]
pub struct RelayerStatus {
    slots_healthy: bool,
    is_connected_to_block_engine: bool,
    validators_connected: Vec<String>,
}

/// Returns an axum router with endpoints to get status of relayer
pub fn build_relayer_router(
    state: Arc<RelayerState>,
    max_buffered_request: usize,
    requests_per_second: u64,
) -> Router {
    async fn homepage(Extension(_state): Extension<Arc<RelayerState>>) -> String {
        "jito relayer".to_string()
    }

    /// Returns a simple string for relayer health
    async fn get_health(Extension(state): Extension<Arc<RelayerState>>) -> String {
        let slots_healthy = *state.slot_health.read().unwrap() == HealthState::Healthy;
        let is_connected_to_block_engine =
            state.is_connected_to_block_engine.load(Ordering::Relaxed);

        let health = if slots_healthy && is_connected_to_block_engine {
            "ok".to_string()
        } else {
            "unhealthy".to_string()
        };

        debug!("get_health: {}", health);
        health
    }

    /// Returns the status of different components inside the relayer
    async fn get_status(Extension(state): Extension<Arc<RelayerState>>) -> Json<RelayerStatus> {
        let status = RelayerStatus {
            slots_healthy: *state.slot_health.read().unwrap() == HealthState::Healthy,
            is_connected_to_block_engine: state
                .is_connected_to_block_engine
                .load(Ordering::Relaxed),
            validators_connected: state
                .relayer_handle
                .connected_validators()
                .iter()
                .map(|p| p.to_string())
                .collect(),
        };
        debug!("get_status: {:?}", status);

        Json(status)
    }

    Router::new()
        .route("/", get(homepage))
        .route("/health", get(get_health))
        .route("/status", get(get_status))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|err: BoxError| async move {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Unhandled error: {}", err),
                    )
                }))
                .layer(BufferLayer::new(max_buffered_request))
                .layer(RateLimitLayer::new(
                    requests_per_second,
                    Duration::from_secs(1),
                )),
        )
        .layer(Extension(state))
}

/// Starts the relayer webserver to serve HTTP requests against
/// Note this is a blocking call, so call spawn in tokio
pub async fn start_relayer_web_server(
    state: Arc<RelayerState>,
    addr: SocketAddr,
    max_buffered_request: usize,
    requests_per_second: u64,
) {
    let app = build_relayer_router(state, max_buffered_request, requests_per_second);
    let _ = axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await;
}
