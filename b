#!/usr/bin/env sh
# Build the relayer container
set -e

TAG=$(git describe --match=NeVeRmAtCh --always --abbrev=8 --dirty)
ORG="jitolabs"

COMPOSE_DOCKER_CLI_BUILD=1 \
  DOCKER_BUILDKIT=1 \
  TAG="${TAG}" \
  ORG="${ORG}" \
  docker-compose build --progress=plain
