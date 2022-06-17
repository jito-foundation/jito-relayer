use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crossbeam_channel::Receiver;
use jito_protos::validator_interface_service::{
    validator_interface_server::ValidatorInterface, GetTpuConfigsRequest, GetTpuConfigsResponse,
    SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest,
    SubscribePacketsResponse,
};
use log::error;
use solana_core::banking_stage::BankingPacketBatch;
use solana_sdk::clock::Slot;
use tokio::{sync::mpsc::channel, time::sleep};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::router::Router;

pub struct Relayer {
    router: Router,
}

impl Relayer {
    pub fn new(
        slot_receiver: Receiver<Slot>,
        packet_receiver: Receiver<BankingPacketBatch>,
    ) -> Relayer {
        let router = Router::new(slot_receiver, packet_receiver);
        Relayer { router }
    }
}

pub struct ValidatorSubscriberStream<T> {
    inner: ReceiverStream<Result<T, Status>>,
}

impl<T> Stream for ValidatorSubscriberStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T> Drop for ValidatorSubscriberStream<T> {
    fn drop(&mut self) {
        // ToDo: Need anything here?
    }
}

#[tonic::async_trait]
impl ValidatorInterface for Relayer {
    type SubscribePacketsStream = ValidatorSubscriberStream<SubscribePacketsResponse>;

    async fn get_tpu_configs(
        &self,
        _: Request<GetTpuConfigsRequest>,
    ) -> Result<Response<GetTpuConfigsResponse>, Status> {
        unimplemented!();
    }

    type SubscribeBundlesStream = ValidatorSubscriberStream<SubscribeBundlesResponse>;

    async fn subscribe_bundles(
        &self,
        _: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        unimplemented!();
    }

    async fn subscribe_packets(
        &self,
        _request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let (sender, receiver) = channel(100);

        tokio::spawn(async move {
            if let Err(e) = sender.send(Ok(SubscribePacketsResponse::default())).await {
                error!("subscribe_packets error sending response: {:?}", e);
            }
            sleep(Duration::from_millis(500)).await;
        });

        Ok(Response::new(ValidatorSubscriberStream {
            inner: ReceiverStream::new(receiver),
        }))
    }
}
