use std::{net::IpAddr, str::FromStr, sync::Arc};

use chrono::{DateTime, NaiveDateTime, Utc};
use jwt::{AlgorithmType, Header, PKeyWithDigest, Token, Verified, VerifyWithKey};
use log::*;
use openssl::pkey::Public;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tonic::{metadata::MetadataMap, service::Interceptor, Request, Status};

/// What the JWT token will be encoded with.
#[derive(Copy, Clone)]
pub struct Claims {
    /// Expected IP of the client.
    pub client_ip: IpAddr,

    /// Expected public key of the client.
    pub client_pubkey: Pubkey,

    /// This token's expirations.
    pub expires_at_utc: NaiveDateTime,
}

impl Claims {
    pub fn is_expired(&self) -> bool {
        self.expires_at_utc.le(&Utc::now().naive_utc())
    }
}

// TODO: Handle these unwraps better just in case we deploy buggy code that issues seemingly valid tokens.
impl From<&DeSerClaims> for Claims {
    fn from(de_ser_claims: &DeSerClaims) -> Self {
        Self {
            client_ip: IpAddr::from_str(&de_ser_claims.client_ip).unwrap(),
            client_pubkey: Pubkey::from_str(&de_ser_claims.client_pubkey).unwrap(),
            expires_at_utc: DateTime::from_timestamp(de_ser_claims.expires_at_unix_ts, 0)
                .unwrap()
                .naive_utc(),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct DeSerClaims {
    pub client_ip: String,
    pub client_pubkey: String,
    pub expires_at_unix_ts: i64,
}

impl From<Claims> for DeSerClaims {
    fn from(claims: Claims) -> Self {
        Self {
            client_ip: claims.client_ip.to_string(),
            client_pubkey: claims.client_pubkey.to_string(),
            expires_at_unix_ts: claims.expires_at_utc.and_utc().timestamp(),
        }
    }
}

#[derive(Clone)]
pub struct AuthInterceptor {
    /// The key used to verify tokens.
    verifying_key: Arc<PKeyWithDigest<Public>>,

    /// The tokens' expected signing algo.
    expected_signing_algo: AlgorithmType,
}

impl AuthInterceptor {
    pub fn new(
        verifying_key: Arc<PKeyWithDigest<Public>>,
        expected_signing_algo: AlgorithmType,
    ) -> Self {
        Self {
            verifying_key,
            expected_signing_algo,
        }
    }

    fn jwt_from_header(
        &self,
        meta: &MetadataMap,
    ) -> Result<Token<Header, DeSerClaims, Verified>, Status> {
        if let Some(auth_header) = meta.get(AUTHORIZATION_HEADER) {
            let auth_header = auth_header.to_str().map_err(|e| {
                warn!("error parsing authorization header {}", e);
                Status::invalid_argument("Failed to parse authorization header.")
            })?;

            if !auth_header.starts_with(BEARER) {
                return Err(Status::permission_denied(
                    "Invalid authorization header format. Must conform to `Authorization: Bearer ${token}`.",
                ));
            }

            let split: Vec<&str> = auth_header.split(BEARER).collect();
            if split.len() != 2 {
                return Err(Status::permission_denied("Missing jwt token."));
            }

            let jwt_token: Token<Header, DeSerClaims, Verified> =
                VerifyWithKey::verify_with_key(split[1], self.verifying_key.as_ref()).map_err(
                    |e| {
                        warn!("error verifying token: {}", e);
                        Status::permission_denied("Token failed verification.")
                    },
                )?;

            // This shouldn't fail since the token passed verification.
            assert_eq!(jwt_token.header().algorithm, self.expected_signing_algo);

            Ok(jwt_token)
        } else {
            Err(Status::permission_denied(
                "The authorization header is missing.",
            ))
        }
    }
}

const AUTHORIZATION_HEADER: &str = "authorization";
const BEARER: &str = "Bearer ";

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let jwt_token = self.jwt_from_header(req.metadata())?;
        let claims: Claims = jwt_token.claims().into();
        req.extensions_mut().insert(claims.client_pubkey);

        Ok(req)
    }
}
