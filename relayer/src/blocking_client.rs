use std::{
    fs::File,
    io::{self, Read},
    net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
};

use crossbeam_channel::{unbounded, Sender};
use jito_protos::validator_interface::{
    validator_interface_client::ValidatorInterfaceClient, PacketStreamMsg,
};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use thiserror::Error;
use tokio::{
    runtime::{Builder, Runtime},
    sync::mpsc::channel,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::{http::uri::InvalidUri, InterceptedService},
    metadata::MetadataValue,
    service::Interceptor,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Error},
    Status,
};

type ValidatorInterfaceClientType =
    ValidatorInterfaceClient<InterceptedService<Channel, AuthenticationInjector>>;

type SubscribePacketsSender = Sender<std::result::Result<Option<PacketStreamMsg>, Status>>;

pub struct BlockingClient {
    rt: Runtime,
    client: ValidatorInterfaceClientType,
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("bad uri error: {0}")]
    BadUrl(#[from] InvalidUri),
    #[error("connecting error: {0}")]
    ConnectionError(#[from] Error),
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),
    #[error("missing tpu socket: {0}")]
    MissingTpuSocket(String),
    #[error("invalid tpu socket: {0}")]
    BadTpuSocket(#[from] AddrParseError),
    #[error("missing tls cert: {0}")]
    MissingTlsCert(#[from] io::Error),
}

pub type ClientResult<T> = std::result::Result<T, ClientError>;

/// Blocking interface to the validator interface server
impl BlockingClient {
    pub fn new(
        validator_interface_address: String,
        auth_interceptor: &AuthenticationInjector,
    ) -> ClientResult<Self> {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let mut validator_interface_endpoint =
            Endpoint::from_shared(validator_interface_address.clone())?;

        // Todo (JL): This is not needed for relayer, right?
        // if validator_interface_address.as_str().contains("https") {
        //     let mut buf = Vec::new();
        //     File::open("/etc/ssl/certs/jito_ca.pem")?.read_to_end(&mut buf)?;
        //     validator_interface_endpoint = validator_interface_endpoint.tls_config(
        //         ClientTlsConfig::new()
        //             .domain_name("jito.wtf")
        //             .ca_certificate(Certificate::from_pem(buf)),
        //     )?;
        // }
        let channel = rt.block_on(validator_interface_endpoint.connect())?;
        let client = ValidatorInterfaceClient::with_interceptor(channel, auth_interceptor.clone());
        Ok(Self { rt, client })
    }

    // ToDo (JL): Stuck Here, need to create client side stream
    // pub fn start_bi_directional_packet_stream(&mut self) -> ClientResult<SubscribePacketsSender> {
    //
    //     let (sender, receiver) = channel(1_000_000);
    //
    //     self
    //         .rt
    //         .block_on(
    //             self.client
    //                 .start_bi_directional_packet_stream(ReceiverStream::new(receiver)),
    //         )?
    //         .into_inner();
    //
    //
    //
    //     Ok(sender)
    // }
}

#[derive(Clone)]
pub struct AuthenticationInjector {
    msg: Vec<u8>,
    sig: Signature,
    pubkey: Pubkey,
}

impl AuthenticationInjector {
    pub fn new(msg: Vec<u8>, sig: Signature, pubkey: Pubkey) -> Self {
        AuthenticationInjector { msg, sig, pubkey }
    }
}

impl Interceptor for AuthenticationInjector {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        request.metadata_mut().append_bin(
            "public-key-bin",
            MetadataValue::from_bytes(&self.pubkey.to_bytes()),
        );
        request.metadata_mut().append_bin(
            "message-bin",
            MetadataValue::from_bytes(self.msg.as_slice()),
        );
        request.metadata_mut().append_bin(
            "signature-bin",
            MetadataValue::from_bytes(self.sig.as_ref()),
        );
        Ok(request)
    }
}
