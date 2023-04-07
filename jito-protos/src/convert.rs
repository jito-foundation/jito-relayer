use std::cmp::min;

use solana_perf::packet::{Packet, PACKET_DATA_SIZE};
use solana_sdk::transaction::VersionedTransaction;

use crate::packet::{Meta as ProtoMeta, Packet as ProtoPacket, PacketFlags as ProtoPacketFlags};

pub fn packet_to_proto_packet(p: &Packet) -> Option<ProtoPacket> {
    Some(ProtoPacket {
        data: p.data(..)?.to_vec(),
        meta: Some(ProtoMeta {
            size: p.meta.size as u64,
            addr: p.meta.addr.to_string(),
            port: p.meta.port as u32,
            flags: Some(ProtoPacketFlags {
                discard: p.meta.discard(),
                forwarded: p.meta.forwarded(),
                repair: p.meta.repair(),
                simple_vote_tx: p.meta.is_simple_vote_tx(),
                tracer_packet: p.meta.is_tracer_packet(),
            }),
            sender_stake: p.meta.sender_stake,
        }),
    })
}

/// Converts a protobuf packet to a VersionedTransaction
pub fn versioned_tx_from_packet(p: &ProtoPacket) -> Option<VersionedTransaction> {
    let mut data = [0; PACKET_DATA_SIZE];
    let copy_len = min(data.len(), p.data.len());
    data[..copy_len].copy_from_slice(&p.data[..copy_len]);
    let mut packet = Packet::new(data, Default::default());
    if let Some(meta) = &p.meta {
        packet.meta.size = meta.size as usize;
    }
    packet.deserialize_slice(..).ok()
}
