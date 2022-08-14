# syntax=docker/dockerfile:1.4.0
FROM rust:1.62.1-slim-buster as builder

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

# needed for HTTPS
RUN \
  apt-get update && \
  apt-get -y install ca-certificates && \
  apt-get clean

WORKDIR /app
COPY --from=builder /home/root/app/jito-transaction-relayer ./
ENTRYPOINT ./jito-transaction-relayer
