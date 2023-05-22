use std::{
    collections::HashSet,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use jito_geyser_protos::solana::geyser::{
    geyser_client::GeyserClient, SubscribeAccountUpdatesRequest, TimestampedAccountUpdate,
};
use jito_rpc::load_balancer::LoadBalancer;
use log::{debug, error, info, warn};
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_metrics::{datapoint_error, datapoint_info};
use solana_program::{address_lookup_table_account::AddressLookupTableAccount, pubkey::Pubkey};
use tokio::time::{interval, sleep};
use tokio_stream::StreamExt;
use tonic::{
    codegen::InterceptedService,
    service::Interceptor,
    transport::{Channel, ClientTlsConfig, Endpoint, Error},
    Response, Status, Streaming,
};

#[derive(Clone)]
pub struct GrpcInterceptor {
    pub access_token: String,
}

impl Interceptor for GrpcInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        request
            .metadata_mut()
            .insert("access-token", self.access_token.parse().unwrap());
        Ok(request)
    }
}

async fn get_geyser_client(
    geyser_grpc_addr: &str,
    maybe_geyser_access_token: &Option<String>,
) -> Result<GeyserClient<InterceptedService<Channel, GrpcInterceptor>>, Error> {
    let geyser_grpc_channel = if geyser_grpc_addr.contains("https") {
        Endpoint::from_str(geyser_grpc_addr)
            .unwrap()
            .tls_config(ClientTlsConfig::new())
            .unwrap()
            .connect()
            .await?
    } else {
        Endpoint::from_str(geyser_grpc_addr)
            .unwrap()
            .connect()
            .await?
    };
    let interceptor = GrpcInterceptor {
        access_token: maybe_geyser_access_token.clone().unwrap_or_default(),
    };

    Ok(GeyserClient::with_interceptor(
        geyser_grpc_channel,
        interceptor,
    ))
}

async fn update_lookup_table_from_stream(
    stream: Response<Streaming<TimestampedAccountUpdate>>,
    lookup_table_cache: &Arc<DashMap<Pubkey, AddressLookupTableAccount>>,
) -> Result<(), String> {
    let mut s = stream.into_inner();
    while let Some(next_message) = s.next().await {
        let timestamped_account_update = next_message.map_err(|e| e.to_string())?;
        let account_update = timestamped_account_update
            .account_update
            .ok_or_else(|| "missing account in update".to_string())?;
        let pubkey = Pubkey::try_from(account_update.pubkey)
            .map_err(|_| "bad public key in account update".to_string())?;
        let lookup_table = AddressLookupTable::deserialize(&account_update.data)
            .map_err(|_| "bad address lookup table written".to_string())?;

        info!(
            "new lookup table update: {:?} num addresses: {:?}",
            pubkey,
            lookup_table.addresses.len()
        );

        lookup_table_cache.insert(
            pubkey,
            AddressLookupTableAccount {
                key: pubkey,
                addresses: lookup_table.addresses.to_vec(),
            },
        );
    }
    Ok(())
}

async fn geyser_lookup_table_updater(
    geyser_url: String,
    geyser_access_token: Option<String>,
    lookup_table_cache: Arc<DashMap<Pubkey, AddressLookupTableAccount>>,
) {
    let address_lookup_table =
        Pubkey::from_str("AddressLookupTab1e1111111111111111111111111").unwrap();
    loop {
        match get_geyser_client(&geyser_url, &geyser_access_token).await {
            Ok(mut geyser_client) => {
                match geyser_client
                    .subscribe_account_updates(SubscribeAccountUpdatesRequest {
                        accounts: vec![address_lookup_table.to_bytes().to_vec()],
                    })
                    .await
                {
                    Ok(stream) => {
                        if let Err(e) =
                            update_lookup_table_from_stream(stream, &lookup_table_cache).await
                        {
                            error!("error streaming from lookup table: {:?}", e);
                        }
                    }
                    Err(e) => {
                        warn!("error connecting to geyser: {:?}", e);
                        sleep(Duration::from_secs(2)).await;
                    }
                }
            }
            Err(e) => {
                error!("error connecting to geyser: {:?}", e);
            }
        }
    }
}

pub async fn start_lookup_table_refresher(
    rpc_load_balancer: Arc<LoadBalancer>,
    lookup_table: Arc<DashMap<Pubkey, AddressLookupTableAccount>>,
    refresh_duration: Duration,
    exit: Arc<AtomicBool>,
    geyser_url: Option<String>,
    geyser_access_token: Option<String>,
) {
    let rpc_load_balancer = rpc_load_balancer.clone();
    let exit = exit.clone();
    let lookup_table = lookup_table.clone();
    // seed lookup table
    if let Err(e) = refresh_address_lookup_table(&rpc_load_balancer, &lookup_table).await {
        error!("error refreshing address lookup table: {e:?}");
    }

    if geyser_url.is_some() {
        tokio::spawn(geyser_lookup_table_updater(
            geyser_url.unwrap(),
            geyser_access_token,
            lookup_table.clone(),
        ));
    }

    let mut tick = interval(Duration::from_secs(1));
    let mut last_refresh = Instant::now();

    while !exit.load(Ordering::Relaxed) {
        let _ = tick.tick().await;
        if last_refresh.elapsed() < refresh_duration {
            continue;
        }

        let now = Instant::now();
        let refresh_result = refresh_address_lookup_table(&rpc_load_balancer, &lookup_table).await;
        let updated_elapsed = now.elapsed().as_micros();
        match refresh_result {
            Ok(_) => {
                datapoint_info!(
                    "lookup_table_refresher-ok",
                    ("count", 1, i64),
                    ("lookup_table_size", lookup_table.len(), i64),
                    ("updated_elapsed_us", updated_elapsed, i64),
                );
            }
            Err(e) => {
                datapoint_error!(
                    "lookup_table_refresher-error",
                    ("count", 1, i64),
                    ("lookup_table_size", lookup_table.len(), i64),
                    ("updated_elapsed_us", updated_elapsed, i64),
                    ("error", e.to_string(), String),
                );
            }
        }
        last_refresh = Instant::now();
    }
}

pub async fn refresh_address_lookup_table(
    rpc_load_balancer: &Arc<LoadBalancer>,
    lookup_table: &DashMap<Pubkey, AddressLookupTableAccount>,
) -> solana_client::client_error::Result<()> {
    let rpc_client = rpc_load_balancer.non_blocking_rpc_client();

    let address_lookup_table =
        Pubkey::from_str("AddressLookupTab1e1111111111111111111111111").unwrap();
    let start = Instant::now();
    let accounts = rpc_client
        .get_program_accounts(&address_lookup_table)
        .await?;
    info!(
        "Fetched {} lookup tables from RPC in {:?}",
        accounts.len(),
        start.elapsed()
    );

    let mut new_pubkeys = HashSet::new();
    for (pubkey, account_data) in accounts {
        match AddressLookupTable::deserialize(&account_data.data) {
            Err(e) => {
                error!("error deserializing AddressLookupTable pubkey: {pubkey}, error: {e}");
            }
            Ok(table) => {
                debug!("lookup table loaded pubkey: {pubkey:?}, table: {table:?}");
                new_pubkeys.insert(pubkey);
                lookup_table.insert(
                    pubkey,
                    AddressLookupTableAccount {
                        key: pubkey,
                        addresses: table.addresses.to_vec(),
                    },
                );
            }
        }
    }

    // remove all the closed lookup tables
    lookup_table.retain(|pubkey, _| new_pubkeys.contains(pubkey));

    Ok(())
}
