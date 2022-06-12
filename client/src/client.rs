use std::time::Duration;

use crossbeam_channel::{unbounded, Receiver, Sender};
use jito_protos::relayer::{
    relayer_service_client::RelayerServiceClient, HeartbeatResponse, HeartbeatSubscriptionRequest,
    PacketSubscriptionRequest, PacketSubscriptionResponse,
};
use log::{error, warn};
use tokio::{runtime::Runtime, time::sleep};
use tonic::{Response, Streaming};

pub struct Client {}

impl Client {
    /// Subscribes to packets from the relayer across `max_connections` connections.
    /// `max_throughput_mbps` contains the max throughput of packets the client wants
    /// sent over the channel.
    ///
    /// Does best effort to keep the number of connections at `max_connections`
    pub fn subscribe_packets(
        rt: Runtime,
        url: &str,
        max_throughput_mbps: u64,
        max_connections: usize,
    ) -> Receiver<PacketSubscriptionResponse> {
        let (sender, receiver) = unbounded();

        let tasks: Vec<_> = (0..max_connections)
            .map(|i| {
                let url = url.to_string();
                let sender = sender.clone();
                rt.spawn(async move {
                    loop {
                        match RelayerServiceClient::connect(url.to_string()).await {
                            Ok(mut client) => {
                                match client
                                    .subscribe_packets(PacketSubscriptionRequest {
                                        max_throughput_mbps,
                                    })
                                    .await
                                {
                                    Ok(response) => {
                                        Self::stream_packets(response, &sender).await;
                                    }
                                    Err(e) => {
                                        error!("error subscribing to packets: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("error connecting to client: {:?}", e);
                            }
                        }
                        sleep(Duration::from_secs(1)).await;
                    }
                })
            })
            .collect();
        receiver
    }

    /// Subscribes to heartbeat from the relayer. Attempts to keep a connection
    pub fn subscribe_heartbeat(rt: Runtime, url: &str) -> Receiver<HeartbeatResponse> {
        let (sender, receiver) = unbounded();

        let url = url.to_string();
        let sender = sender.clone();
        rt.spawn(async move {
            loop {
                match RelayerServiceClient::connect(url.to_string()).await {
                    Ok(mut client) => {
                        match client
                            .subscribe_heartbeat(HeartbeatSubscriptionRequest {})
                            .await
                        {
                            Ok(response) => {
                                Self::stream_packets(response, &sender).await;
                            }
                            Err(e) => {
                                error!("error subscribing to packets: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("error connecting to client: {:?}", e);
                    }
                }
                sleep(Duration::from_secs(1)).await;
            }
        });

        receiver
    }

    pub async fn stream_packets<T>(response: Response<Streaming<T>>, sender: &Sender<T>) {
        let mut stream = response.into_inner();
        loop {
            match stream.message().await {
                Ok(Some(msg)) => {
                    if let Err(e) = sender.send(msg) {
                        error!("error sending over channel: {}", e);
                        break;
                    }
                }
                Ok(None) => {
                    warn!("got none on channel?");
                    break;
                }
                Err(e) => {
                    error!("error reading channel: {}", e);
                    break;
                }
            }
        }
    }
}
