use std::{cmp::Reverse, net::IpAddr, sync::Arc};

use chrono::{NaiveDateTime, Utc};
use keyed_priority_queue::KeyedPriorityQueue;
use tokio::sync::Mutex;

use crate::auth_interceptor::Claims;

#[derive(Clone)]
pub(crate) struct AuthChallenge {
    /// The randomly generated clients are expected to sign.
    pub(crate) challenge: String,

    /// What the access token will be encoded with upon issuance.
    pub(crate) access_claims: Claims,

    /// What the refresh token will be encoded with upon issuance.
    pub(crate) refresh_claims: Claims,

    /// Challenges must have expirations.
    pub(crate) expires_at_utc: NaiveDateTime,
}

impl AuthChallenge {
    pub(crate) fn is_expired(&self) -> bool {
        self.expires_at_utc.le(&Utc::now().naive_utc())
    }
}

impl Eq for AuthChallenge {}

impl PartialEq<Self> for AuthChallenge {
    fn eq(&self, other: &Self) -> bool {
        self.expires_at_utc.eq(&other.expires_at_utc)
    }
}

impl PartialOrd<Self> for AuthChallenge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.expires_at_utc.cmp(&other.expires_at_utc))
    }
}

impl Ord for AuthChallenge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expires_at_utc.cmp(&other.expires_at_utc)
    }
}

#[derive(Clone, Default)]
pub(crate) struct AuthChallenges(Arc<Mutex<KeyedPriorityQueue<IpAddr, Reverse<AuthChallenge>>>>);

impl AuthChallenges {
    pub(crate) async fn remove_all_expired(&self) {
        let mut inner = self.0.lock().await;
        while let Some((_ip_addr, auth_challenge)) = inner.peek() {
            if auth_challenge.0.is_expired() {
                inner.pop();
            } else {
                break;
            }
        }
    }

    pub(crate) async fn push(&self, ip: IpAddr, challenge: Reverse<AuthChallenge>) {
        let mut inner = self.0.lock().await;
        inner.push(ip, challenge);
    }

    pub(crate) async fn len(&self) -> usize {
        let inner = self.0.lock().await;
        inner.len()
    }

    pub(crate) async fn get_priority(&self, ip: &IpAddr) -> Option<Reverse<AuthChallenge>> {
        let inner = self.0.lock().await;
        inner.get_priority(ip).cloned()
    }

    pub(crate) async fn remove(&self, ip: &IpAddr) {
        let mut inner = self.0.lock().await;
        let _ = inner.remove(ip);
    }
}
