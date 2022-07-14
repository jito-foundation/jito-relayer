// Returns the batches as a wrapped ExpiringPacketBatches protobuf with an expiration
// attached to them.
// pub fn packet_batches_to_expiring_packet_batches(
//     batches: Vec<PacketBatch>,
//     packet_delay_ms: u32,
// ) -> ExpiringPacketBatches {
//     let now = SystemTime::now();
//
//     ExpiringPacketBatches {
//         header: Some(Header {
//             ts: Some(prost_types::Timestamp::from(now)),
//         }),
//         batch_list: batches
//             .into_iter()
//             .map(|batch| PbPacketBatch {
//                 packets: batch
//                     .iter()
//                     .filter(|p| !p.meta.discard())
//                     .filter_map(|p| {
//                         Some(PbPacket {
//                             data: p.data(0..p.meta.size)?.to_vec(),
//                             meta: Some(PbMeta {
//                                 size: p.meta.size as u64,
//                                 addr: p.meta.addr.to_string(),
//                                 port: p.meta.port as u32,
//                                 flags: Some(PbPacketFlags {
//                                     discard: p.meta.discard(),
//                                     forwarded: p.meta.forwarded(),
//                                     repair: p.meta.repair(),
//                                     simple_vote_tx: p.meta.is_simple_vote_tx(),
//                                     tracer_packet: p.meta.is_tracer_packet(),
//                                 }),
//                                 sender_stake: p.meta.sender_stake,
//                             }),
//                         })
//                     })
//                     .collect(),
//             })
//             .collect(),
//         expiry_ms: packet_delay_ms,
//     }
// }
