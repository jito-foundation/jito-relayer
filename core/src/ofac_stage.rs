use std::{
    collections::HashSet,
    sync::Arc,
    thread,
    thread::{Builder, JoinHandle},
};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use log::warn;
use solana_core::banking_stage::BankingPacketBatch;
use solana_perf::packet::PacketBatch;
use solana_sdk::{
    address_lookup_table_account::AddressLookupTableAccount, pubkey::Pubkey,
    transaction::VersionedTransaction,
};

pub struct OfacStage {
    ofac_thread: JoinHandle<()>,
}

impl OfacStage {
    pub fn new(
        verified_receiver: Receiver<BankingPacketBatch>,
        ofac_sender: Sender<BankingPacketBatch>,
        ofac_addresses: &HashSet<Pubkey>,
        address_lookup_table_cache: &Arc<DashMap<Pubkey, AddressLookupTableAccount>>,
    ) -> OfacStage {
        let ofac_thread = if ofac_addresses.is_empty() {
            Self::spawn_passthrough(verified_receiver, ofac_sender)
        } else {
            Self::spawn_ofac_thread(
                verified_receiver,
                ofac_sender,
                ofac_addresses,
                address_lookup_table_cache,
            )
        };
        OfacStage { ofac_thread }
    }

    pub fn join(self) -> thread::Result<()> {
        self.ofac_thread.join()
    }

    fn spawn_passthrough(
        verified_receiver: Receiver<BankingPacketBatch>,
        ofac_sender: Sender<BankingPacketBatch>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("ofac_stage".into())
            .spawn(move || {
                while let Ok(packets) = verified_receiver.recv() {
                    if ofac_sender.send(packets).is_err() {
                        warn!("ofac_sender send error, returning early");
                        return;
                    }
                }
                warn!("verified_receiver receive error, returning early");
            })
            .unwrap()
    }

    fn spawn_ofac_thread(
        verified_receiver: Receiver<BankingPacketBatch>,
        ofac_sender: Sender<BankingPacketBatch>,
        ofac_addresses: &HashSet<Pubkey>,
        address_lookup_table_cache: &Arc<DashMap<Pubkey, AddressLookupTableAccount>>,
    ) -> JoinHandle<()> {
        let address_lookup_table_cache = address_lookup_table_cache.clone();
        let ofac_addresses = ofac_addresses.clone();
        Builder::new()
            .name("ofac_stage".into())
            .spawn(move || {
                while let Ok(mut packets) = verified_receiver.recv() {
                    packets.0.iter_mut().for_each(|packet_batch| {
                        discard_ofac_packets(
                            packet_batch,
                            &ofac_addresses,
                            &address_lookup_table_cache,
                        );
                    });

                    if ofac_sender.send(packets).is_err() {
                        warn!("ofac_sender send error, returning early");
                        return;
                    }
                }
            })
            .unwrap()
    }
}

/// Discards packets that mention any OFAC related transactions
fn discard_ofac_packets(
    packet_batch: &mut PacketBatch,
    ofac_addresses: &HashSet<Pubkey>,
    address_lookup_table_cache: &DashMap<Pubkey, AddressLookupTableAccount>,
) {
    for p in packet_batch.iter_mut().filter(|p| !p.meta.discard()) {
        let tx: bincode::Result<VersionedTransaction> = p.deserialize_slice(..);
        if let Ok(tx) = tx {
            if is_tx_ofac_related(&tx, ofac_addresses, address_lookup_table_cache) {
                p.meta.set_discard(true);
            }
        }
    }
}

/// Returns true if transaction is ofac-related, false if not
fn is_tx_ofac_related(
    tx: &VersionedTransaction,
    ofac_addresses: &HashSet<Pubkey>,
    address_lookup_table_cache: &DashMap<Pubkey, AddressLookupTableAccount>,
) -> bool {
    is_ofac_address_in_static_keys(tx, ofac_addresses)
        || is_ofac_address_in_lookup_table(tx, ofac_addresses, address_lookup_table_cache)
}

/// Returns true if an ofac address is in the static keys for an account
fn is_ofac_address_in_static_keys(
    tx: &VersionedTransaction,
    ofac_addresses: &HashSet<Pubkey>,
) -> bool {
    tx.message
        .static_account_keys()
        .iter()
        .any(|acc| ofac_addresses.contains(acc))
}

/// Returns true if an ofac address is in the dynamic keys (lookup table) for an account
fn is_ofac_address_in_lookup_table(
    tx: &VersionedTransaction,
    ofac_addresses: &HashSet<Pubkey>,
    address_lookup_table_cache: &DashMap<Pubkey, AddressLookupTableAccount>,
) -> bool {
    if let Some(lookup_tables) = tx.message.address_table_lookups() {
        for table in lookup_tables {
            if let Some(lookup_info) = address_lookup_table_cache.get(&table.account_key) {
                for idx in table
                    .writable_indexes
                    .iter()
                    .chain(table.readonly_indexes.iter())
                {
                    if let Some(account) = lookup_info.addresses.get(*idx as usize) {
                        if ofac_addresses.contains(account) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use crossbeam_channel::unbounded;
    use dashmap::DashMap;
    use solana_perf::packet::PacketBatch;
    use solana_sdk::{
        address_lookup_table_account::AddressLookupTableAccount,
        hash::Hash,
        instruction::{AccountMeta, CompiledInstruction, Instruction},
        message::{v0, v0::MessageAddressTableLookup, MessageHeader, VersionedMessage},
        packet::Packet,
        pubkey::Pubkey,
        signature::Signer,
        signer::keypair::Keypair,
        transaction::{Transaction, VersionedTransaction},
    };

    use crate::ofac_stage::{
        discard_ofac_packets, is_ofac_address_in_lookup_table, is_ofac_address_in_static_keys,
        OfacStage,
    };

    #[test]
    fn test_is_ofac_address_in_static_keys() {
        let ofac_signer = Keypair::new();
        let ofac_pubkey = ofac_signer.pubkey();
        let ofac_addresses: HashSet<Pubkey> = HashSet::from_iter([ofac_pubkey]);

        let payer = Keypair::new();

        // random address passes
        let tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: Pubkey::new_unique(),
                    is_signer: false,
                    is_writable: false,
                }],
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let tx = VersionedTransaction::from(tx);
        assert!(!is_ofac_address_in_static_keys(&tx, &ofac_addresses));

        // transaction with ofac account as writable
        let tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: ofac_pubkey,
                    is_signer: false,
                    is_writable: true,
                }],
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let tx = VersionedTransaction::from(tx);
        assert!(is_ofac_address_in_static_keys(&tx, &ofac_addresses));

        // transaction with ofac account as readonly
        let tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: ofac_pubkey,
                    is_signer: false,
                    is_writable: false,
                }],
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let tx = VersionedTransaction::from(tx);

        assert!(is_ofac_address_in_static_keys(&tx, &ofac_addresses));

        // transaction with ofac account as signer
        let tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: Pubkey::new_unique(),
                    is_signer: false,
                    is_writable: true,
                }],
            )],
            Some(&ofac_signer.pubkey()),
            &[&ofac_signer],
            Hash::default(),
        );
        let tx = VersionedTransaction::from(tx);
        assert!(is_ofac_address_in_static_keys(&tx, &ofac_addresses));
    }

    #[test]
    fn test_is_ofac_address_in_lookup_table() {
        let ofac_pubkey = Pubkey::new_unique();
        let ofac_addresses: HashSet<Pubkey> = HashSet::from_iter([ofac_pubkey]);

        let payer = Keypair::new();

        let lookup_table_pubkey = Pubkey::new_unique();
        let lookup_table = AddressLookupTableAccount {
            key: lookup_table_pubkey.clone(),
            addresses: vec![ofac_pubkey, Pubkey::new_unique()],
        };

        let address_lookup_table_cache = DashMap::from_iter([(lookup_table_pubkey, lookup_table)]);

        // test read-only ofac address
        let message = VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            recent_blockhash: Hash::new_unique(),
            account_keys: vec![payer.pubkey(), Pubkey::new_unique()],
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: lookup_table_pubkey,
                writable_indexes: vec![],
                readonly_indexes: vec![0],
            }],
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![],
            }],
        });
        let tx = VersionedTransaction::try_new(message, &[&payer]).expect("valid tx");

        assert!(is_ofac_address_in_lookup_table(
            &tx,
            &ofac_addresses,
            &address_lookup_table_cache
        ));

        // test writeable ofac
        let message = VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            recent_blockhash: Hash::new_unique(),
            account_keys: vec![payer.pubkey(), Pubkey::new_unique()],
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: lookup_table_pubkey,
                writable_indexes: vec![0],
                readonly_indexes: vec![],
            }],
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![],
            }],
        });
        let tx = VersionedTransaction::try_new(message, &[&payer]).expect("valid tx");
        assert!(is_ofac_address_in_lookup_table(
            &tx,
            &ofac_addresses,
            &address_lookup_table_cache
        ));

        // test proximate ofac (in same lookup table, but not referenced)
        let message = VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            recent_blockhash: Hash::new_unique(),
            account_keys: vec![payer.pubkey(), Pubkey::new_unique()],
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: lookup_table_pubkey,
                writable_indexes: vec![1],
                readonly_indexes: vec![],
            }],
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![1],
                data: vec![],
            }],
        });
        let tx = VersionedTransaction::try_new(message, &[&payer]).expect("valid tx");
        assert!(!is_ofac_address_in_lookup_table(
            &tx,
            &ofac_addresses,
            &address_lookup_table_cache
        ));
    }

    #[test]
    fn test_discard_ofac_packets() {
        let ofac_pubkey = Pubkey::new_unique();
        let ofac_addresses: HashSet<Pubkey> = HashSet::from_iter([ofac_pubkey]);

        let address_lookup_table_cache = DashMap::new();

        let payer = Keypair::new();

        // random address packet
        let random_tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: Pubkey::new_unique(),
                    is_signer: false,
                    is_writable: false,
                }],
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let random_tx = VersionedTransaction::from(random_tx);
        let random_packet = Packet::from_data(None, &random_tx).expect("can create packet");

        let ofac_tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: ofac_pubkey,
                    is_signer: false,
                    is_writable: true,
                }],
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let ofac_tx = VersionedTransaction::from(ofac_tx);
        let ofac_packet = Packet::from_data(None, &ofac_tx).expect("can create packet");

        let mut packet_batch = PacketBatch::new(vec![random_packet, ofac_packet]);
        discard_ofac_packets(
            &mut packet_batch,
            &ofac_addresses,
            &address_lookup_table_cache,
        );

        assert_eq!(packet_batch.len(), 2);
        assert!(!packet_batch[0].meta.discard());
        assert!(packet_batch[1].meta.discard());
    }

    #[test]
    fn test_ofac_stage_ofac_tx() {
        let ofac_pubkey = Pubkey::new_unique();
        let ofac_addresses: HashSet<Pubkey> = HashSet::from_iter([ofac_pubkey]);

        let address_lookup_table_cache = Arc::new(DashMap::new());

        let payer = Keypair::new();

        let random_tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: ofac_pubkey,
                    is_signer: false,
                    is_writable: false,
                }],
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let random_tx = VersionedTransaction::from(random_tx);
        let random_packet = Packet::from_data(None, &random_tx).expect("can create packet");

        let (tx_sender, tx_receiver) = unbounded();
        let (ofac_sender, ofac_receiver) = unbounded();

        let ofac_stage = OfacStage::new(
            tx_receiver,
            ofac_sender,
            &ofac_addresses,
            &address_lookup_table_cache,
        );

        tx_sender
            .send((vec![PacketBatch::new(vec![random_packet])], None))
            .unwrap();

        let packets = ofac_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(packets.0.len(), 1);
        assert_eq!(packets.0[0].len(), 1);
        assert!(packets.0[0][0].meta.discard());

        drop(tx_sender);
        ofac_stage.join().unwrap();
    }

    #[test]
    fn test_ofac_stage_non_ofac_tx() {
        let ofac_pubkey = Pubkey::new_unique();
        let ofac_addresses: HashSet<Pubkey> = HashSet::from_iter([ofac_pubkey]);

        let address_lookup_table_cache = Arc::new(DashMap::new());

        let payer = Keypair::new();

        let random_tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: Pubkey::new_unique(),
                    is_signer: false,
                    is_writable: false,
                }],
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let random_tx = VersionedTransaction::from(random_tx);
        let random_packet = Packet::from_data(None, &random_tx).expect("can create packet");

        let (tx_sender, tx_receiver) = unbounded();
        let (ofac_sender, ofac_receiver) = unbounded();

        let ofac_stage = OfacStage::new(
            tx_receiver,
            ofac_sender,
            &ofac_addresses,
            &address_lookup_table_cache,
        );

        tx_sender
            .send((vec![PacketBatch::new(vec![random_packet])], None))
            .unwrap();

        let packets = ofac_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(packets.0.len(), 1);
        assert_eq!(packets.0[0].len(), 1);
        assert!(!packets.0[0][0].meta.discard());

        drop(tx_sender);
        ofac_stage.join().unwrap();
    }

    #[test]
    fn test_ofac_stage_passthrough() {
        let ofac_addresses: HashSet<Pubkey> = HashSet::new();
        let address_lookup_table_cache = Arc::new(DashMap::new());

        let payer = Keypair::new();

        let random_tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[0],
                vec![AccountMeta {
                    pubkey: Pubkey::new_unique(),
                    is_signer: false,
                    is_writable: false,
                }],
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let random_tx = VersionedTransaction::from(random_tx);
        let random_packet = Packet::from_data(None, &random_tx).expect("can create packet");

        let (tx_sender, tx_receiver) = unbounded();
        let (ofac_sender, ofac_receiver) = unbounded();

        let ofac_stage = OfacStage::new(
            tx_receiver,
            ofac_sender,
            &ofac_addresses,
            &address_lookup_table_cache,
        );

        tx_sender
            .send((vec![PacketBatch::new(vec![random_packet])], None))
            .unwrap();

        let packets = ofac_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(packets.0.len(), 1);
        assert_eq!(packets.0[0].len(), 1);
        assert!(!packets.0[0][0].meta.discard());

        drop(tx_sender);
        ofac_stage.join().unwrap();
    }
}
