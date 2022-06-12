# syntax=docker/dockerfile:1.4.0
FROM rust:1.59.0-slim-buster as builder

RUN apt-get update && apt-get install -y libudev-dev clang pkg-config libssl-dev build-essential cmake
RUN rustup component add rustfmt
RUN update-ca-certificates

ENV HOME=/home/root
WORKDIR $HOME/app
COPY . .

RUN --mount=type=cache,mode=0777,target=/home/root/app/target \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry \
    cargo build --release && cp target/release/jito-* ./

FROM debian:buster-slim as jito-transaction-relayer

#EXPOSE 8899/tcp
## RPC pubsub
#EXPOSE 8900/tcp
## entrypoint
#EXPOSE 8001/tcp
## (future) bank service
#EXPOSE 8901/tcp
## bank service
#EXPOSE 8902/tcp
## faucet
#EXPOSE 9900/tcp
## tvu
#EXPOSE 8000/udp
## gossip
#EXPOSE 8001/udp
## tvu_forwards
#EXPOSE 8002/udp
## tpu
#EXPOSE 8003/udp
## tpu_forwards
#EXPOSE 8004/udp
## retransmit
#EXPOSE 8005/udp
## repair
#EXPOSE 8006/udp
## serve_repair
#EXPOSE 8007/udp
## broadcast
#EXPOSE 8008/udp
## tpu_vote
#EXPOSE 8009/udp

WORKDIR /app
COPY --from=builder /home/root/app/jito-transaction-relayer ./
ENTRYPOINT ./jito-transaction-relayer
