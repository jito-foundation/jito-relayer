use std::sync::Arc;

use ed25519_dalek::{PublicKey, Signature, Verifier};
use solana_sdk::pubkey::Pubkey;
use tonic::{metadata::MetadataMap, service::Interceptor, Request, Status};

use crate::schedule_cache::LeaderScheduleCache;

#[derive(Clone)]
pub struct AuthenticationInterceptor {
    pub cache: Arc<LeaderScheduleCache>,
}

impl AuthenticationInterceptor {
    pub fn auth(
        req: &mut tonic::Request<()>,
        cache: &Arc<LeaderScheduleCache>,
    ) -> Result<(), Status> {
        let meta = req.metadata();
        let pubkey = extract_signer_pubkey(meta)?;
        let msg = Self::extract_msg(meta)?;
        let signature = Self::extract_signature(meta)?;
        pubkey
            .verify(msg.as_slice(), &signature)
            .map_err(|_| Status::invalid_argument("bad message and/or signature"))?;

        let validator_pubkey = Pubkey::new(&pubkey.to_bytes());

        // TODO: is this called in async runtime?
        if !cache.is_validator_scheduled(validator_pubkey) {
            return Err(Status::permission_denied(
                "not a validator scheduled for this epoch",
            ));
        }

        req.extensions_mut().insert(validator_pubkey);

        Ok(())
    }

    fn extract_msg(meta: &MetadataMap) -> Result<Vec<u8>, Status> {
        let msg = meta
            .get_bin("message-bin")
            .ok_or_else(|| Status::invalid_argument("message missing"))?;

        Ok(msg
            .to_bytes()
            .map_err(|_| Status::invalid_argument("bad message"))?
            .to_vec())
    }

    fn extract_signature(meta: &MetadataMap) -> Result<Signature, Status> {
        let sig_bytes = meta
            .get_bin("signature-bin")
            .ok_or_else(|| Status::invalid_argument("missing signature"))?
            .to_bytes()
            .map_err(|_| Status::invalid_argument("invalid signature format"))?;
        let sig_bytes = sig_bytes.to_vec();
        let sig_bytes = &<[u8; 64]>::try_from(sig_bytes.as_slice())
            .map_err(|_| Status::invalid_argument("invalid signature format"))?;

        Signature::from_bytes(sig_bytes)
            .map_err(|_| Status::invalid_argument("invalid signature format"))
    }
}

impl Interceptor for AuthenticationInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<Request<()>, Status> {
        Self::auth(&mut request, &self.cache)?;
        Ok(request)
    }
}

pub fn extract_signer_pubkey(meta: &MetadataMap) -> Result<PublicKey, Status> {
    let pubkey_bytes = meta
        .get_bin("public-key-bin")
        .ok_or_else(|| Status::invalid_argument("missing public key"))?
        .to_bytes()
        .map_err(|_| Status::invalid_argument("incorrectly formatted public key"))?;
    let pk = PublicKey::from_bytes(pubkey_bytes.as_ref())
        .map_err(|_| Status::invalid_argument("incorrectly formatted public key"))?;
    Ok(pk)
}

pub fn extract_pubkey(meta: &MetadataMap) -> Result<Pubkey, Status> {
    Ok(Pubkey::new(&extract_signer_pubkey(meta)?.to_bytes()))
}
