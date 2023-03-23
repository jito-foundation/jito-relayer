#!/usr/bin/env bash
set -euxo pipefail

TAG=$(git describe --match=NeVeRmAtCh --always --abbrev=8 --dirty)
ORG="jitolabs"

export DOCKER_LOCALHOST=172.17.0.1
if [[ $(uname) == "Darwin" ]]; then
  DOCKER_LOCALHOST=docker.for.mac.localhost
fi

# Build and run
DOCKER_BUILDKIT=1 \
  BUILDKIT_PROGRESS=plain \
  TAG="${TAG}" \
  ORG="${ORG}" \
  RPC_SERVERS=http://${DOCKER_LOCALHOST}:8899 \
  WEBSOCKET_SERVERS=ws://${DOCKER_LOCALHOST}:8900 \
  BLOCK_ENGINE_AUTH_SERVICE_URL=http://${DOCKER_LOCALHOST}:1005 \
  BLOCK_ENGINE_URL=http://${DOCKER_LOCALHOST}:1000 \
  CLUSTER=devnet \
  REGION=local \
  docker compose up --build --pull=always --remove-orphans --renew-anon-volumes
