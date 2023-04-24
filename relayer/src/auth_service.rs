use std::{
    cmp::Reverse,
    net::IpAddr,
    ops::Add,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration as StdDuration,
};

use chrono::{Duration, Utc};
use ed25519_dalek::{PublicKey, Signature, Verifier};
use jito_protos::auth::{
    auth_service_server::AuthService, GenerateAuthChallengeRequest, GenerateAuthChallengeResponse,
    GenerateAuthTokensRequest, GenerateAuthTokensResponse, RefreshAccessTokenRequest,
    RefreshAccessTokenResponse, Role, Token as PbToken,
};
use jwt::{AlgorithmType, Header, PKeyWithDigest, SignWithKey, Token, VerifyWithKey};
use log::*;
use openssl::pkey::{Private, Public};
use prost_types::Timestamp;
use rand::{distributions::Alphanumeric, Rng};
use solana_sdk::pubkey::Pubkey;
use tokio::{task::JoinHandle, time::interval};
use tonic::{Request, Response, Status};

use crate::{
    auth_challenges::{AuthChallenge, AuthChallenges},
    auth_interceptor::{Claims, DeSerClaims},
    health_manager::HealthState,
};

pub trait ValidatorAuther: Send + Sync + 'static {
    fn is_authorized(&self, pubkey: &Pubkey) -> bool;
}

pub struct AuthServiceImpl<V: ValidatorAuther> {
    validator_auther: V,

    _t_hdl: JoinHandle<()>,

    /// Keeps track of generated challenges. Generating a challenge requires no authentication which
    /// opens up a DOS vector. In order to mitigate we'll allow one challenge per IP, so that an
    /// attacker would be required to rent many IPs to overload the system. Using a PQ where items
    /// priority is based on age. This makes expiring items more efficient since we don't need to
    /// iterate over the entire collection.
    ///
    /// NOTE: The order is reversed so that older (lesser) timestamps are prioritized.
    auth_challenges: AuthChallenges,

    /// The key used to sign JWT access & refresh tokens.
    signing_key: PKeyWithDigest<Private>,
    /// The key used to verify tokens. This same key must be shared with all services that
    /// perform token based auth.
    verifying_key: Arc<PKeyWithDigest<Public>>,

    /// Each token's respective TTLs.
    access_token_ttl: Duration,
    refresh_token_ttl: Duration,

    /// How long challenges are valid for DOS mitigation purposes.
    challenge_ttl: Duration,

    health_state: Arc<RwLock<HealthState>>,
}

// The capacity of the auth_challenges map.
const AUTH_CHALLENGES_CAPACITY: usize = 100_000;

impl<V: ValidatorAuther> AuthServiceImpl<V> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        validator_auther: V,
        signing_key: PKeyWithDigest<Private>,
        verifying_key: Arc<PKeyWithDigest<Public>>,
        access_token_ttl: StdDuration,
        refresh_token_ttl: StdDuration,
        challenge_ttl: StdDuration,
        challenge_expiration_sleep_interval: StdDuration,
        exit: &Arc<AtomicBool>,
        health_state: Arc<RwLock<HealthState>>,
    ) -> Self {
        let auth_challenges = AuthChallenges::default();
        let _t_hdl = Self::start_challenge_expiration_task(
            auth_challenges.clone(),
            challenge_expiration_sleep_interval,
            exit,
        );

        Self {
            auth_challenges,
            validator_auther,
            signing_key,
            verifying_key,
            _t_hdl,
            access_token_ttl: Duration::from_std(access_token_ttl).unwrap(),
            refresh_token_ttl: Duration::from_std(refresh_token_ttl).unwrap(),
            challenge_ttl: Duration::from_std(challenge_ttl).unwrap(),
            health_state,
        }
    }

    fn start_challenge_expiration_task(
        auth_challenges: AuthChallenges,
        sleep_interval: StdDuration,
        exit: &Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        tokio::task::spawn(async move {
            let mut interval = interval(sleep_interval);
            while !exit.load(Ordering::Relaxed) {
                let _ = interval.tick().await;
                auth_challenges.remove_all_expired().await;
            }
        })
    }

    // NOTE: if this is behind a proxy, the remote_addr will be the proxy, which may mess with the
    // authentication scheme
    fn client_ip<T>(req: &Request<T>) -> Result<IpAddr, Status> {
        Ok(req
            .remote_addr()
            .ok_or_else(|| Status::internal("request is missing IP address"))?
            .ip())
    }

    fn generate_challenge_token() -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(9)
            .map(char::from)
            .collect()
    }

    /// Prevent validators from authenticating if the relayer is unhealthy
    fn check_health(health_state: &Arc<RwLock<HealthState>>) -> Result<(), Status> {
        if *health_state.read().unwrap() != HealthState::Healthy {
            Err(Status::internal("relayer is unhealthy"))
        } else {
            Ok(())
        }
    }
}

#[tonic::async_trait]
impl<V: ValidatorAuther> AuthService for AuthServiceImpl<V> {
    async fn generate_auth_challenge(
        &self,
        req: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        Self::check_health(&self.health_state)?;
        let auth_challenges = &self.auth_challenges;

        if auth_challenges.len().await >= AUTH_CHALLENGES_CAPACITY {
            return Err(Status::resource_exhausted("System overloaded."));
        }

        let client_ip = Self::client_ip(&req)?;
        if let Some(auth_challenge) = auth_challenges.get_priority(&client_ip).await {
            if !auth_challenge.0.is_expired() {
                return Ok(Response::new(GenerateAuthChallengeResponse {
                    challenge: auth_challenge.0.challenge,
                }));
            }
        }

        let inner_req = req.into_inner();

        if inner_req.role != Role::Validator as i32 {
            return Err(Status::invalid_argument("Role must be validator."));
        }

        if inner_req.pubkey.len() != solana_sdk::pubkey::PUBKEY_BYTES {
            return Err(Status::invalid_argument(
                "Pubkey must be 32 bytes in length",
            ));
        }

        let pubkey = Pubkey::try_from(inner_req.pubkey)
            .map_err(|_| Status::invalid_argument("Invalid pubkey supplied."))?;

        if !self.validator_auther.is_authorized(&pubkey) {
            return Err(Status::permission_denied(
                "The supplied pubkey is not authorized to generate a challenge.",
            ));
        }

        let challenge = Self::generate_challenge_token();
        auth_challenges
            .push(
                client_ip,
                Reverse(AuthChallenge {
                    challenge: challenge.clone(),
                    access_claims: Claims {
                        client_ip,
                        client_pubkey: pubkey,
                        expires_at_utc: Utc::now().add(self.access_token_ttl).naive_utc(),
                    },
                    refresh_claims: Claims {
                        client_ip,
                        client_pubkey: pubkey,
                        expires_at_utc: Utc::now().add(self.refresh_token_ttl).naive_utc(),
                    },
                    expires_at_utc: Utc::now().add(self.challenge_ttl).naive_utc(),
                }),
            )
            .await;

        Ok(Response::new(GenerateAuthChallengeResponse { challenge }))
    }

    async fn generate_auth_tokens(
        &self,
        req: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        Self::check_health(&self.health_state)?;
        let auth_challenges = &self.auth_challenges;

        let client_ip = Self::client_ip(&req)?;
        let inner_req = req.into_inner();

        let client_pubkey = PublicKey::from_bytes(&inner_req.client_pubkey).map_err(|e| {
            warn!("Failed to create pubkey from string: {}", e);
            Status::invalid_argument("Invalid pubkey supplied.")
        })?;
        let solana_pubkey = Pubkey::try_from(client_pubkey.to_bytes())
            .map_err(|_| Status::invalid_argument("Invalid pubkey supplied."))?;

        let auth_challenge = if let Some(challenge) = auth_challenges.get_priority(&client_ip).await
        {
            Ok(challenge)
        } else {
            Err(Status::permission_denied(
                "Must invoke the GenerateAuthChallenge method before calling any method.",
            ))
        }?;

        // Prepended with the pubkey to invalidate any tx this server could maliciously send.
        let expected_challenge = format!("{}-{}", solana_pubkey, auth_challenge.0.challenge);
        if expected_challenge != inner_req.challenge {
            return Err(Status::invalid_argument(format!(
                "The provided challenge does not match the expected challenge: {expected_challenge}"
            )));
        }

        if inner_req.signed_challenge.len() != solana_sdk::signature::SIGNATURE_BYTES {
            return Err(Status::invalid_argument("Signature must be 64 bytes."));
        }
        let signed_challenge = {
            let sig_bytes =
                &<[u8; 64]>::try_from(&inner_req.signed_challenge[..]).map_err(|e| {
                    error!("Invalid signature 1: {}", e);
                    Status::invalid_argument("Invalid signature.")
                })?;

            Signature::from_bytes(sig_bytes).map_err(|e| {
                error!("Invalid signature 2: {}", e);
                Status::invalid_argument("Invalid signature.")
            })?
        };

        client_pubkey
            .verify(inner_req.challenge.as_bytes(), &signed_challenge)
            .map_err(|e| {
                warn!("Challenge verification failed: {}", e);
                Status::invalid_argument("Failed challenge verification. Did you sign with the supplied pubkey's corresponding private key?")
            })?;

        let access_token = {
            let header = Header {
                algorithm: AlgorithmType::Rs256,
                ..Default::default()
            };
            let claims: DeSerClaims = auth_challenge.0.access_claims.into();
            Token::new(header, claims)
                .sign_with_key(&self.signing_key)
                .map_err(|e| {
                    error!("Error signing access_token claims: {}", e);
                    Status::internal("Error signing access_token.")
                })
        }?
        .as_str()
        .to_string();

        let refresh_token = {
            let header = Header {
                algorithm: AlgorithmType::Rs256,
                ..Default::default()
            };
            let claims: DeSerClaims = auth_challenge.0.refresh_claims.into();
            Token::new(header, claims)
                .sign_with_key(&self.signing_key)
                .map_err(|e| {
                    error!("Error signing refresh_token claims: {}", e);
                    Status::internal("Error signing refresh_token.")
                })
        }?
        .as_str()
        .to_string();

        let access_expiry = auth_challenge.0.access_claims.expires_at_utc;
        let refresh_expiry = auth_challenge.0.refresh_claims.expires_at_utc;

        auth_challenges.remove(&client_ip).await;

        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(PbToken {
                value: access_token,
                expires_at_utc: Some(Timestamp {
                    seconds: access_expiry.timestamp(),
                    nanos: 0,
                }),
            }),
            refresh_token: Some(PbToken {
                value: refresh_token,
                expires_at_utc: Some(Timestamp {
                    seconds: refresh_expiry.timestamp(),
                    nanos: 0,
                }),
            }),
        }))
    }

    async fn refresh_access_token(
        &self,
        req: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        Self::check_health(&self.health_state)?;

        let inner_req = req.into_inner();

        let refresh_token: &str = inner_req.refresh_token.as_str();
        let refresh_claims: DeSerClaims = refresh_token
            .verify_with_key(self.verifying_key.as_ref())
            .map_err(|e| {
                warn!("refresh_token failed to verify: {}", e);
                Status::permission_denied("Invalid refresh_token supplied")
            })?;
        let refresh_claims: Claims = (&refresh_claims).into();

        if refresh_claims.is_expired() {
            return Err(Status::permission_denied("Client refresh_token has expired, please generate a new auth challenge to obtain a set of new access tokens."));
        }

        let expires_at_utc = Utc::now().add(self.access_token_ttl).naive_utc();
        let access_claims: DeSerClaims = Claims {
            client_ip: refresh_claims.client_ip,
            client_pubkey: refresh_claims.client_pubkey,
            expires_at_utc,
        }
        .into();
        let access_token = {
            let header = Header {
                algorithm: AlgorithmType::Rs256,
                ..Default::default()
            };
            Token::new(header, access_claims)
                .sign_with_key(&self.signing_key)
                .map_err(|e| {
                    error!("Error signing access_token claims: {}", e);
                    Status::internal("Error signing access_token.")
                })
        }?
        .as_str()
        .to_string();

        Ok(Response::new(RefreshAccessTokenResponse {
            access_token: Some(PbToken {
                value: access_token,
                expires_at_utc: Some(Timestamp {
                    seconds: expires_at_utc.timestamp(),
                    nanos: 0,
                }),
            }),
        }))
    }
}
