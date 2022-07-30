use std::{cell::RefCell, rc::Rc, sync::Arc};

use jito_protos::block_engine::{
    block_engine_relayer_client::BlockEngineRelayerClient, AccountsOfInterestRequest,
    AccountsOfInterestUpdate, PacketBatchUpdate, SubscribeExpiringPacketsResponse,
};
use solana_sdk::signature::{Keypair, Signer};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::InterceptedService, metadata::MetadataValue, service::Interceptor, transport::Channel,
    Code, Response, Status, Streaming,
};

// Auth header keys
pub const MESSAGE_BIN: &str = "message-bin";
pub const PUBKEY_BIN: &str = "public-key-bin";
pub const SIGNATURE_BIN: &str = "signature-bin";

/// Intercepts requests and adds the necessary headers for auth.
#[derive(Clone)]
pub struct AuthInterceptor {
    /// Used to sign the server generated token.
    keypair: Arc<Keypair>,
    token: Rc<RefCell<String>>,
}

impl AuthInterceptor {
    pub fn new(keypair: Arc<Keypair>, token: Rc<RefCell<String>>) -> Self {
        AuthInterceptor { keypair, token }
    }

    pub fn should_retry(
        status: &Status,
        token: Rc<RefCell<String>>,
        max_retries: usize,
        n_retries: usize,
    ) -> bool {
        if max_retries == n_retries {
            return false;
        }

        let mut token = token.borrow_mut();
        if let Some(new_token) = Self::maybe_new_auth_token(status, &token) {
            *token = new_token;
            true
        } else {
            false
        }
    }

    /// Checks to see if the server returned a token to be signed and if it does not equal the current
    /// token then the new token is returned and authentication can be retried.
    fn maybe_new_auth_token(status: &Status, current_token: &str) -> Option<String> {
        if status.code() != Code::Unauthenticated {
            return None;
        }

        let msg = status.message().split_whitespace().collect::<Vec<&str>>();
        if msg.len() != 2 {
            return None;
        }

        if msg[0] != "token:" {
            return None;
        }

        if msg[1] != current_token {
            Some(msg[1].to_string())
        } else {
            None
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        // Prefix with pubkey and hash it in order to ensure BlockEngine doesn't have us sign a malicious transaction.
        let token = format!("{}-{}", self.keypair.pubkey(), self.token.take(),);
        let hashed_token = solana_sdk::hash::hash(token.as_bytes());

        request.metadata_mut().append_bin(
            PUBKEY_BIN,
            MetadataValue::from_bytes(&self.keypair.pubkey().to_bytes()),
        );
        request.metadata_mut().append_bin(
            MESSAGE_BIN,
            MetadataValue::from_bytes(hashed_token.to_bytes().as_slice()),
        );
        request.metadata_mut().append_bin(
            SIGNATURE_BIN,
            MetadataValue::from_bytes(
                self.keypair
                    .sign_message(hashed_token.to_bytes().as_slice())
                    .as_ref(),
            ),
        );

        Ok(request)
    }
}

/// Wrapper client that takes care of extracting the auth challenge and retrying requests.
pub struct AuthClient {
    inner: BlockEngineRelayerClient<InterceptedService<Channel, AuthInterceptor>>,
    token: Rc<RefCell<String>>,
    max_retries: usize,
}

impl AuthClient {
    pub fn new(
        inner: BlockEngineRelayerClient<InterceptedService<Channel, AuthInterceptor>>,
        max_retries: usize,
    ) -> Self {
        let token = Rc::new(RefCell::new(String::default()));
        Self {
            inner,
            token,
            max_retries,
        }
    }

    pub async fn subscribe_accounts_of_interest(
        &mut self,
        req: AccountsOfInterestRequest,
    ) -> Result<Response<Streaming<AccountsOfInterestUpdate>>, Status> {
        let mut n_retries = 0;
        loop {
            return match self.inner.subscribe_accounts_of_interest(req.clone()).await {
                Ok(resp) => Ok(resp),
                Err(status) => {
                    if AuthInterceptor::should_retry(
                        &status,
                        self.token.clone(),
                        self.max_retries,
                        n_retries,
                    ) {
                        n_retries += 1;
                        continue;
                    }
                    Err(status)
                }
            };
        }
    }

    pub async fn start_expiring_packet_stream(
        &mut self,
        buffer: usize,
    ) -> Result<
        (
            Sender<PacketBatchUpdate>,
            Response<SubscribeExpiringPacketsResponse>,
        ),
        Status,
    > {
        let mut n_retries = 0;
        loop {
            let (tx, rx) = channel::<PacketBatchUpdate>(buffer);
            let receiver_stream = ReceiverStream::new(rx);
            return match self
                .inner
                .start_expiring_packet_stream(receiver_stream)
                .await
            {
                Ok(resp) => Ok((tx, resp)),
                Err(status) => {
                    if AuthInterceptor::should_retry(
                        &status,
                        self.token.clone(),
                        self.max_retries,
                        n_retries,
                    ) {
                        n_retries += 1;
                        continue;
                    }
                    Err(status)
                }
            };
        }
    }
}
