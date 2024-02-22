# Jito Relayer
Jito Relayer acts as a transaction processing unit (TPU) proxy for Solana validators.

# Building
```shell
# pull submodules to get protobuffers required to connect to Block Engine and validator
$ git submodule update -i -r
# build from source
$ cargo b --release
```

# Releases

## Making a release

We opt to use cargo workspaces for making releases.
First, install cargo workspaces by running: `cargo install cargo-workspaces`.
Next, check out the master branch of the jito-relayer repo and 
ensure you're on the latest commit.
In the master branch, run the following command and follow the instructions:
```shell
$ ./release
```
This will bump all the versions of the packages in your repo, 
push to master and tag a new commit.

## Running a release
There are two options for running the relayer from releases:
- Download the most recent release on the [releases](https://github.com/jito-foundation/jito-relayer/releases) page.
- (Not recommended for production): One can download and run Docker containers from the Docker [registry](https://hub.docker.com/r/jitolabs/jito-transaction-relayer).

# Running a Relayer
See https://jito-foundation.gitbook.io/mev/jito-relayer/running-a-relayer for setup and usage instructions.
