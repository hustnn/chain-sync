//! Implementation of ethereum protocol parser

pub mod eth_parser;
pub mod encode;

use std::collections::HashMap;
use chrono::{Date, NaiveDate, Utc};
use deltalake::{Schema, SchemaDataType, SchemaField};
use serde::{Serialize, Deserialize};

use ethers::{prelude::*};
use ethers::types::transaction::eip2930::AccessList;

pub struct EthereumBlockRawData {
    pub block: Block<Transaction>,
    pub receipts: Vec<TransactionReceipt>,
}

pub struct EthereumBlock {
    pub block: Block<TxHash>,
}

pub struct EthereumTransactions {
    pub block_num: u64,
    pub transactions: Vec<FinalTransaction>,
}

pub struct EthereumReceipts {
    pub block_num: u64,
    pub receipts: Vec<FinalReceipt>,
}

pub struct EthereumLogs {
    pub block_num: u64,
    pub logs: Vec<FinalLog>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalBlock {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub nonce: String,
    pub sha3_uncles: String,
    pub miner: String,
    pub logs_bloom: String,
    pub transactions_root: String,
    pub state_root: String,
    pub receipts_root: String,
    pub difficulty: String,
    pub total_difficulty: String,
    pub size: u64,
    pub extra_data: String,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub transaction_count: u64,
    pub base_fee_per_gas: u64,
    pub date: NaiveDate,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalTransaction {
    pub hash: String,
    pub nonce: String,
    pub transaction_index: u64,
    pub from_address: String,
    pub to_address: String,
    pub value: String,
    pub gas: u64,
    pub gas_price: u64,
    pub method_id: String,
    pub input: String,
    pub max_fee_per_gas: u64,
    pub max_priority_fee_per_gas: u64,
    pub transaction_type: u64,
    pub access_list: String,
    pub block_hash: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub status: u64,
    pub date: NaiveDate,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalReceipt {
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub block_hash: String,
    pub block_number: u64,
    pub from_address: String,
    pub to_address: String,
    pub cumulative_gas_used: u64,
    pub gas_used: u64,
    pub contract_address: String,
    pub root: String,
    pub status: u64,
    pub logs_bloom: String,
    pub transaction_type: u64,
    pub effective_gas_price: u64,
    pub block_timestamp: u64,
    pub date: NaiveDate,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalLog {
    pub contract_address: String,
    pub num_of_topics: u64,
    pub topics: String,
    pub topic0: String,
    pub topic1: String,
    pub topic2: String,
    pub topic3: String,
    pub data: String,
    pub block_hash: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub transaction_log_index: u64,
    pub log_type: String,
    pub removed: bool,
    pub block_timestamp: u64,
    pub date: NaiveDate,
}

pub fn getEthBlockSchema() -> Schema {
    Schema::new(vec![
        SchemaField::new(
            "number".to_string(),
            SchemaDataType::primitive("long".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "hash".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "parent_hash".to_string(),
            SchemaDataType::primitive("string".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "nonce".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "sha3_uncles".to_string(),
            SchemaDataType::primitive("string".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "logs_bloom".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "transactions_root".to_string(),
            SchemaDataType::primitive("string".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "state_root".to_string(),
            SchemaDataType::primitive("string".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "receipts_root".to_string(),
            SchemaDataType::primitive("string".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "miner".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "difficulty".to_string(),
            SchemaDataType::primitive("string".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "total_difficulty".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "size".to_string(),
            SchemaDataType::primitive("long".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "extra_data".to_string(),
            SchemaDataType::primitive("string".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "gas_limit".to_string(),
            SchemaDataType::primitive("long".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "gas_used".to_string(),
            SchemaDataType::primitive("long".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "timestamp".to_string(),
            SchemaDataType::primitive("long".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "transaction_count".to_string(),
            SchemaDataType::primitive("long".to_string()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "base_fee_per_gas".to_string(),
            SchemaDataType::primitive("long".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "date".to_string(),
            SchemaDataType::primitive("date".to_string()),
            false,
            HashMap::new(),
        ),
    ])
}
