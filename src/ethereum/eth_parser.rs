use ethers::{prelude::*};
use chrono::{Date, DateTime, TimeZone, Utc};
use ethers::types::transaction::eip2930::AccessList;
use serde::{Serialize, Deserialize};
use ethers::utils::hex;

use crate::ethereum::{EthereumBlockRawData, EthereumLogs, EthereumReceipts, EthereumTransactions, FinalBlock, FinalLog, FinalReceipt, FinalTransaction};
use crate::ethereum::encode::{bloom_to_hex, bytes_to_hex, h160_to_hex, h256_to_hex, h64_to_hex, u256_to_hex};


pub async fn process_raw_block(raw_block: EthereumBlockRawData) -> (FinalBlock, EthereumTransactions, EthereumReceipts, EthereumLogs)
{
    println!("Latest block number: {:?}", raw_block.block.number);
    // println!("Got block: {:?}", raw_block.block);
    // println!("Txn 0: {:?}", raw_block.block.transactions[0]);
    // println!("Receipt 0: {:?}", raw_block.receipts[0]);
    let block = raw_block.block.clone();
    let final_block = FinalBlock{
        number: block.number.unwrap_or_else(U64::zero).as_u64(),
        hash: h256_to_hex(&block.hash.unwrap_or_else(H256::zero)),
        parent_hash: h256_to_hex(&block.parent_hash),
        nonce: h64_to_hex(&block.nonce.unwrap_or_else(H64::zero)),
        sha3_uncles: h256_to_hex(&block.uncles_hash),
        miner: h160_to_hex(&block.author.unwrap_or_else(Address::zero)),
        logs_bloom: bloom_to_hex(&block.logs_bloom.unwrap_or_else(Bloom::zero)),
        transactions_root: h256_to_hex(&block.transactions_root),
        state_root: h256_to_hex(&block.state_root),
        receipts_root: h256_to_hex(&block.receipts_root),
        difficulty: u256_to_hex(&block.difficulty),
        total_difficulty: u256_to_hex(&block.total_difficulty.unwrap_or_else(U256::zero)),
        size: block.size.unwrap_or_else(U256::zero).as_u64(),
        extra_data: bytes_to_hex(&block.extra_data),
        gas_limit: block.gas_limit.as_u64(),
        gas_used: block.gas_used.as_u64(),
        timestamp: block.timestamp.as_u64(),
        transaction_count: block.transactions.len() as u64,
        base_fee_per_gas: block.base_fee_per_gas.unwrap_or_else(U256::zero).as_u64(),
        date: Utc.timestamp(i64::try_from(block.timestamp.as_u64()).unwrap(), 0).naive_utc().date(),
        //date: Utc.timestamp(block.timestamp.to_string().into(), 0).date().to_string()
    };

    let transactions = block.transactions;
    let status : Vec<U64> = raw_block.receipts.iter().map(|receipt| receipt.status.unwrap_or_else(U64::zero)).collect();
    let mut final_transactions = Vec::with_capacity(transactions.len());

    let mut txn_idx = 0;
    for tx in &transactions {
        let input = bytes_to_hex(&tx.input);
        let final_txn = FinalTransaction {
            hash: h256_to_hex(&tx.hash),
            nonce: u256_to_hex(&tx.nonce),
            transaction_index: tx.transaction_index.unwrap_or_else(U64::zero).as_u64(),
            from_address: h160_to_hex(&tx.from),
            to_address: h160_to_hex(&tx.to.unwrap_or_else(Address::zero)),
            value: u256_to_hex(&tx.value),
            gas: tx.gas.as_u64(),
            gas_price: tx.gas_price.unwrap_or_else(U256::zero).as_u64(),
            method_id: if input.len() > 9 { input[..10].to_string() } else { input.to_string() },
            input: bytes_to_hex(&tx.input),
            max_fee_per_gas: tx.max_fee_per_gas.unwrap_or_else(U256::zero).as_u64(),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.unwrap_or_else(U256::zero).as_u64(),
            transaction_type: tx.transaction_type.unwrap_or_else(U64::zero).as_u64(),
            access_list: format!("{:?}", tx.access_list.clone().unwrap_or_else(AccessList::default)),
            block_hash: h256_to_hex(&block.hash.unwrap_or_else(H256::zero)),
            block_number: block.number.unwrap_or_else(U64::zero).as_u64(),
            block_timestamp: block.timestamp.as_u64(),
            status: status[txn_idx].as_u64(),
            date: Utc.timestamp(i64::try_from(block.timestamp.as_u64()).unwrap(), 0).naive_utc().date()
        };
        final_transactions.push(final_txn);
        txn_idx += 1;
    }

    let receipts = raw_block.receipts;
    let mut final_receipts = Vec::with_capacity(receipts.len());
    let mut final_logs = vec![];

    for receipt in &receipts {
        let final_receipt = FinalReceipt {
            transaction_hash: h256_to_hex(&receipt.transaction_hash),
            transaction_index: receipt.transaction_index.as_u64(),
            block_hash: h256_to_hex(&receipt.block_hash.unwrap_or_else(H256::zero)),
            block_number: receipt.block_number.unwrap_or_else(U64::zero).as_u64(),
            from_address: h160_to_hex(&receipt.from),
            to_address: h160_to_hex(&receipt.to.unwrap_or_else(Address::zero)),
            cumulative_gas_used: receipt.cumulative_gas_used.as_u64(),
            gas_used: receipt.gas_used.unwrap_or_else(U256::zero).as_u64(),
            contract_address: h160_to_hex(&receipt.contract_address.unwrap_or_else(Address::zero)),
            root: h256_to_hex(&receipt.root.unwrap_or_else(H256::zero)),
            status: receipt.status.unwrap_or_else(U64::zero).as_u64(),
            logs_bloom: bloom_to_hex(&receipt.logs_bloom),
            transaction_type: receipt.transaction_type.unwrap_or_else(U64::zero).as_u64(),
            effective_gas_price: receipt.effective_gas_price.unwrap_or_else(U256::zero).as_u64(),
            block_timestamp: block.timestamp.as_u64(),
            date: Utc.timestamp(i64::try_from(block.timestamp.as_u64()).unwrap(), 0).naive_utc().date(),
        };
        final_receipts.push(final_receipt);

        for log in &receipt.logs {
            let flat_topics = log.topics.clone().into_iter().flat_map(|t| t.as_ref().to_vec()).collect::<Vec<u8>>();
            let num_of_topics = log.topics.len();
            let final_log = FinalLog {
                contract_address: h160_to_hex(&log.address),
                num_of_topics: U64::from(num_of_topics).as_u64(),
                topics: format!("0x{}", hex::encode(flat_topics)),
                topic0: if num_of_topics >= 1 { h256_to_hex(&log.topics[0]) } else { "".to_string() },
                topic1: if num_of_topics >= 2 { h256_to_hex(&log.topics[1]) } else { "".to_string() },
                topic2: if num_of_topics >= 3 { h256_to_hex(&log.topics[2]) } else { "".to_string() },
                topic3: if num_of_topics >= 4 { h256_to_hex(&log.topics[3]) } else { "".to_string() },
                data: bytes_to_hex(&log.data),
                block_hash: h256_to_hex(&log.block_hash.unwrap_or_else(H256::zero)),
                block_number: log.block_number.unwrap_or_else(U64::zero).as_u64(),
                transaction_hash: h256_to_hex(&log.transaction_hash.unwrap_or_else(H256::zero)),
                transaction_index: log.transaction_index.unwrap_or_else(U64::zero).as_u64(),
                log_index: log.log_index.unwrap_or_else(U256::zero).as_u64(),
                transaction_log_index: log.transaction_log_index.unwrap_or_else(U256::zero).as_u64(),
                log_type: log.log_type.clone().unwrap_or("".to_string()),
                removed: log.removed.unwrap_or(false),
                block_timestamp: block.timestamp.as_u64(),
                date: Utc.timestamp(i64::try_from(block.timestamp.as_u64()).unwrap(), 0).naive_utc().date(),
            };
            final_logs.push(final_log);
        }
    }

    let eth_transactions = EthereumTransactions{block_num: block.number.unwrap_or_else(U64::zero).as_u64(), transactions: final_transactions};
    let eth_receipts = EthereumReceipts {block_num: block.number.unwrap_or_else(U64::zero).as_u64(), receipts: final_receipts};
    let eth_logs = EthereumLogs {block_num: block.number.unwrap_or_else(U64::zero).as_u64(), logs: final_logs};

    (final_block, eth_transactions, eth_receipts, eth_logs)
}