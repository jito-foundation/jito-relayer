#!/usr/bin/env bash
# Builds relayer in a docker container.
# Useful for running on machines that might not have cargo installed but can run docker (Flatcar Linux).
# run `./f ` to compile

set -eux -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

GIT_SHA="$(git describe --always --dirty)"

echo "Git hash: $GIT_SHA"

DEBUG_FLAGS=${1-false}

DOCKER_BUILDKIT=1 docker build \
                    --build-arg debug=$DEBUG_FLAGS \
                    --build-arg ci_commit=$GIT_SHA \
                    -t jitolabs/jito-transaction-relayer . --progress=plain

# Creates a temporary container, copies binaries built inside container and removes the temporary container.
docker rm temp || true
docker container create --name temp jitolabs/jito-transaction-relayer
mkdir -p "$SCRIPT_DIR"/docker-output
# Outputs the binaries
docker container cp temp:/app/jito-transaction-relayer "$SCRIPT_DIR"/docker-output
# docker container cp temp:/app/jito-packet-blaster "$SCRIPT_DIR"/docker-output
docker rm temp
