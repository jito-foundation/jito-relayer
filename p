#!/usr/bin/env sh
set -e

# Some container vars
TAG=${USER}-dev
ORG="jitolabs"


COMPOSE_DOCKER_CLI_BUILD=1 \
  DOCKER_BUILDKIT=1 \
  TAG="${TAG}" \
  ORG="${ORG}" \
  docker compose build --progress=plain

docker push ${ORG}/jito-transaction-relayer:${TAG}
