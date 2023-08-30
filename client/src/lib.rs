pub mod auth;

use std::{
    net::{AddrParseError, SocketAddr},
    result,
};

use thiserror::Error;
use tonic::Status;

pub type Result<T> = result::Result<T, ProxyError>;
// type HeartbeatEvent = (SocketAddr, SocketAddr);

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),

    #[error("stream disconnected")]
    GrpcStreamDisconnected,

    #[error("heartbeat error")]
    HeartbeatChannelError,

    #[error("heartbeat expired")]
    HeartbeatExpired,

    #[error("error forwarding packet to banking stage")]
    PacketForwardError,

    #[error("missing tpu config: {0:?}")]
    MissingTpuSocket(String),

    #[error("invalid socket address: {0:?}")]
    InvalidSocketAddress(#[from] AddrParseError),

    #[error("invalid gRPC data: {0:?}")]
    InvalidData(String),

    #[error("timeout: {0:?}")]
    ConnectionError(#[from] tonic::transport::Error),

    #[error("AuthenticationConnectionTimeout")]
    AuthenticationConnectionTimeout,

    #[error("AuthenticationTimeout")]
    AuthenticationTimeout,

    #[error("AuthenticationConnectionError: {0:?}")]
    AuthenticationConnectionError(String),

    #[error("BlockEngineConnectionTimeout")]
    BlockEngineConnectionTimeout,

    #[error("BlockEngineTimeout")]
    BlockEngineTimeout,

    #[error("BlockEngineConnectionError: {0:?}")]
    BlockEngineConnectionError(String),

    #[error("RelayerConnectionTimeout")]
    RelayerConnectionTimeout,

    #[error("RelayerTimeout")]
    RelayerEngineTimeout,

    #[error("RelayerConnectionError: {0:?}")]
    RelayerConnectionError(String),

    #[error("AuthenticationError: {0:?}")]
    AuthenticationError(String),

    #[error("AuthenticationPermissionDenied")]
    AuthenticationPermissionDenied,

    #[error("BadAuthenticationToken: {0:?}")]
    BadAuthenticationToken(String),

    #[error("MethodTimeout: {0:?}")]
    MethodTimeout(String),

    #[error("MethodError: {0:?}")]
    MethodError(String),
}
