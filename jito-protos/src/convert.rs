use solana_perf::packet::Packet;

use crate::packet::{Meta as ProtoMeta, Packet as ProtoPacket, PacketFlags as ProtoPacketFlags};

pub fn packet_to_proto_packet(p: &Packet) -> Option<ProtoPacket> {
    Some(ProtoPacket {
        data: p.data(0..p.meta.size)?.to_vec(),
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
