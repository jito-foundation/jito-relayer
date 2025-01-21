# syntax=docker/dockerfile:1.4.0
FROM rust:1.64-slim-bullseye as builder

# Add Google Protocol Buffers for Libra's metrics library.
ENV PROTOC_VERSION 3.8.0
ENV PROTOC_ZIP protoc-$PROTOC_VERSION-linux-x86_64.zip

RUN set -x \
 && apt update \
 && apt install -y \
      clang \
      cmake \
      libudev-dev \
      make \
      unzip \
      libssl-dev \
      pkg-config \
      zlib1g-dev \
      curl \
 && rustup component add rustfmt \
 && rustup component add clippy \
 && rustc --version \
 && cargo --version \
 && curl -OL https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP \
 && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
 && unzip -o $PROTOC_ZIP -d /usr/local include/* \
 && rm -f $PROTOC_ZIP

ENV HOME=/home/root
WORKDIR $HOME/app
COPY . .

# cache these directories for reuse
# see: https://docs.docker.com/build/cache/#use-the-dedicated-run-cache
RUN --mount=type=cache,mode=0777,target=/home/root/app/target \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/git \
    RUSTFLAGS="-C target-cpu=x86-64-v3" ./cargo stable build --release && cp target/release/jito-* ./

FROM debian:bullseye-slim as jito-transaction-relayer
RUN apt-get -qq update && apt-get -qq -y install ca-certificates libssl1.1 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /home/root/app/jito-transaction-relayer ./
# COPY --from=builder /home/root/app/jito-packet-blaster ./
ENTRYPOINT ./jito-transaction-relayer
