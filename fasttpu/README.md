# Fast TPU server

fasttpu is scalable rewrite of the TPU/QUIC server inspired by
Firedancer's fd_quic.

Notable differences from Agave's solana_streamer server are that fasttpu
does not use a "magic" global scheduler like Tokio.
