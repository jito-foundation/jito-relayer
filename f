#!/usr/bin/env bash
# Builds jito-solana in a docker container.
# Useful for running on machines that might not have cargo installed but can run docker (Flatcar Linux).
# run `./f true` to compile with debug flags

set -eux -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

GIT_SHA="$(git describe --always --dirty)"

echo "Git hash: $GIT_SHA"

DOCKER_BUILDKIT=1 docker build -t jitolabs/jito-transaction-relayer . --progress=plain

# Creates a temporary container, copies solana-validator built inside container there and
# removes the temporary container.
docker rm temp || true
docker container create --name temp jitolabs/jito-transaction-relayer
mkdir -p "$SCRIPT_DIR"/docker-output
# Outputs the binaries
docker container cp temp:/app "$SCRIPT_DIR"
docker rm temp
