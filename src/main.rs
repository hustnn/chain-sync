use std::{thread, time};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::sync::Arc;
use std::env;

use dashmap::DashMap;
use log::{debug, error, info, warn};

use ethers::{prelude::*};
use crossbeam_channel::{Receiver, Sender, unbounded};
use eyre::Result;
use serde_json::json;

use chain_sync::{DataTypePartition, get_latest_state, IngestOptions, PartitionAssignment, Message, DataTypeOffset, start_ingest, create_block_table, IngestProcess};
use chain_sync::ethereum::eth_parser::process_raw_block;
use chain_sync::ethereum::{EthereumBlock, EthereumBlockRawData, EthereumReceipts, EthereumTransactions, FinalBlock};
use ethers::prelude::ProviderError;

fn get_latest_block(partition_assignment: &PartitionAssignment, max_block_num: &mut i64, need_offset: &mut bool, current_block_num: i64)
{
    for (p, offset) in partition_assignment.assignment.iter() {
        println!("Partition is {:?}, and offset is {:?}", p, offset);
        match offset {
            Some(o)  => {
                if *o < *max_block_num {
                    *max_block_num = *o;
                }
                if *need_offset {
                    *need_offset = false;
                    println!("no need_offset anymore");
                }
            }
            None => {
                if *max_block_num > current_block_num  {
                    *max_block_num = current_block_num - 1;
                }
            }
            _ => {
                println!("why here???");
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Start sync ethereum!");

    let args: Vec<String> = env::args().collect();
    let full_node_provider = &args[1];
    let current_block = args[2].parse::<i64>().unwrap();

    let storage_options = HashMap::from([
        ("AWS_ACCESS_KEY_ID".to_string(), "xxx".to_string()),
        ("AWS_SECRET_ACCESS_KEY".to_string(), "xxx".to_string()),
    ]);

    let app_id = "eth_delta_ingest";
    let block_table_uri = "s3://querystal-chain-data/ethereum/ods/blocks/";
    let transaction_table_uri = "s3://querystal-chain-data/ethereum/ods/transactions/";
    let receipt_table_uri = "s3://querystal-chain-data/ethereum/ods/receipts/";
    let log_table_uri = "s3://querystal-chain-data/ethereum/ods/logs";

    let partition: DataTypePartition = 1;
    let mut next_block = i64::MAX;
    let mut max_block_num_for_blocks = next_block;
    let mut max_block_num_for_txns = next_block;
    let mut max_block_num_for_receipts = next_block;
    let mut max_block_num_for_logs = next_block;

    /// create table for the first time.
    /// create_block_table(block_table_uri, storage_options.clone()).await;
    let mut need_offset_for_block = true;
    let mut need_offset_for_txn = true;
    let mut need_offset_for_receipt = true;
    let mut need_offset_for_log = true;

    let mut block_partition_assignment  = PartitionAssignment::default();
    let mut transaction_partition_assignment = PartitionAssignment::default();
    let mut receipt_partition_assignment = PartitionAssignment::default();
    let mut log_partition_assignment = PartitionAssignment::default();;

    get_latest_state(block_table_uri.to_string(), app_id.to_string(), storage_options.clone(), vec![partition], &mut block_partition_assignment).await;
    get_latest_state(transaction_table_uri.to_string(), app_id.to_string(), storage_options.clone(), vec![partition], &mut transaction_partition_assignment).await;
    get_latest_state(receipt_table_uri.to_string(), app_id.to_string(), storage_options.clone(), vec![partition], &mut receipt_partition_assignment).await;
    get_latest_state(log_table_uri.to_string(), app_id.to_string(), storage_options.clone(), vec![partition], &mut log_partition_assignment).await;

    get_latest_block(&block_partition_assignment, &mut max_block_num_for_blocks, &mut need_offset_for_block, current_block);
    get_latest_block(&transaction_partition_assignment, &mut max_block_num_for_txns, &mut need_offset_for_txn, current_block);
    get_latest_block(&receipt_partition_assignment, &mut max_block_num_for_receipts, &mut need_offset_for_receipt, current_block);
    get_latest_block(&log_partition_assignment, &mut max_block_num_for_logs, &mut need_offset_for_log, current_block);

    next_block = *vec![max_block_num_for_blocks, max_block_num_for_txns, max_block_num_for_receipts, max_block_num_for_logs].iter().min().unwrap();

    let mut block_ingest_opts = IngestOptions::default();
    if need_offset_for_block {
        block_ingest_opts.seek_offsets = Option::Some(vec![(partition, next_block)]);
    }

    let mut txn_ingest_opts = IngestOptions::default();
    if need_offset_for_txn {
        txn_ingest_opts.seek_offsets = Option::Some(vec![(partition, next_block)]);
    }

    let mut receipt_ingest_opts = IngestOptions::default();
    if need_offset_for_receipt {
        receipt_ingest_opts.seek_offsets = Option::Some(vec![(partition, next_block)]);
    }

    let mut log_ingest_opts = IngestOptions::default();
    if need_offset_for_log {
        log_ingest_opts.seek_offsets = Option::Some(vec![(partition, next_block)]);
    }

    next_block += 1;
    println!("Start to ingestion from block: {:?}.", next_block);

    let provider = Arc::new(Provider::<Http>::try_from(full_node_provider).expect("could not instantiate HTTP Provider"));

    let mut ingest_block = IngestProcess::new(block_table_uri, storage_options.clone(), block_ingest_opts).await?;
    let mut ingest_txn = IngestProcess::new(transaction_table_uri, storage_options.clone(), txn_ingest_opts).await?;
    let mut ingest_receipt = IngestProcess::new(receipt_table_uri, storage_options.clone(), receipt_ingest_opts).await?;
    let mut ingest_log = IngestProcess::new(log_table_uri, storage_options.clone(), log_ingest_opts).await?;
    ingest_block.reset_state()?;
    ingest_txn.reset_state()?;
    ingest_receipt.reset_state()?;
    ingest_log.reset_state()?;

    let batch_size = U64::from(10000);

    let mut cur_block = U64::from(next_block);
    let max_block_num_for_blocks = U64::from(max_block_num_for_blocks);
    let max_block_num_for_txns = U64::from(max_block_num_for_txns);
    let max_block_num_for_receipts = U64::from(max_block_num_for_receipts);
    let max_block_num_for_logs = U64::from(max_block_num_for_logs);

    loop {
        let latest_block = provider.get_block_number().await.unwrap();
        println!("Get the latest block number: {:?}", latest_block);

        while cur_block <= latest_block {
            let cur_batch_size = min(batch_size, latest_block - cur_block + 1).as_u64();
            for i in 0..cur_batch_size {
                let block_num = cur_block + U64::from(i);
                println!("start to request block: {:?} and latest block is {:?}", block_num, latest_block);
                let block = provider.get_block_with_txs(block_num).await.unwrap().unwrap();
                println!("get block: {:?} and latest block is {:?}", block_num, latest_block);
                let receipts = provider.get_block_receipts(block_num).await.unwrap();
                println!("get receipt: {:?} and latest block is {:?}", block_num, latest_block);
                let raw_block = EthereumBlockRawData {block, receipts};
                let (block, eth_transactions, eth_receipts, eth_logs) = process_raw_block(raw_block).await;
                let offset = block_num.to_string().parse::<i64>().unwrap();
                let block_msg = Message{partition, offset, values: vec![serde_json::to_value(block).unwrap()]};
                let txn_msg = Message{partition, offset, values: eth_transactions.transactions.iter().map(|txn| serde_json::to_value(txn).unwrap()).collect()};
                let receipt_msg = Message{partition, offset, values: eth_receipts.receipts.iter().map(|receipt| serde_json::to_value(receipt).unwrap()).collect()};
                let log_msg = Message{partition, offset, values: eth_logs.logs.iter().map(|log| serde_json::to_value(log).unwrap()).collect()};

                if block_num > max_block_num_for_blocks {
                    ingest_block.ingest_data(block_msg).await?;
                }

                if block_num > max_block_num_for_txns {
                    ingest_txn.ingest_data(txn_msg).await?;
                }

                if block_num > max_block_num_for_receipts {
                    ingest_receipt.ingest_data(receipt_msg).await?;
                }

                if block_num > max_block_num_for_logs {
                    ingest_log.ingest_data(log_msg).await?;
                }
            }
            cur_block += cur_batch_size.into();
        }

        thread::sleep(time::Duration::from_secs(10));
    }
}

/*
#[tokio::main]
async fn main() -> Result<()> {
    println!("Start sync ethereum!");

    let full_node_provider = "http://xxx:8545";

    // let mut block_raw_data_cache = vec![];
    // block_raw_data_cache.drain(0..20);

    let storage_options = HashMap::from([
        ("AWS_ACCESS_KEY_ID".to_string(), "xxx".to_string()),
        ("AWS_SECRET_ACCESS_KEY".to_string(), "xxx".to_string()),
    ]);

    /*let mut delta_block_table = utils::load_table("s3://eth-etl-delta/example/blocks/", storage_options.clone()).await.unwrap();
    println!("Delta block table: {:?}", delta_block_table.schema());

    let mut block_value_buffers = ValueBuffer::new();
    let mut delta_writer = JsonWriter::for_table(&delta_block_table, storage_options.clone()).unwrap();

    let app_id = "chain-sync";
    let mut delta_offset: DataTypeOffset;
    let mut latency_timer = Instant::now();*/

    let app_id = "Eth_delta_ingest";
    // let block_table_uri = "s3://eth-etl-delta/example/blocks_test/";
    let block_table_uri = "s3://ethereum-chain-demo/blocks/";
    let transaction_table_uri = "s3://ethereum-chain-demo/transactions/";
    let partition: DataTypePartition = 1;
    let mut current_block = 10000000 as i64;
    let mut max_block = i64::MAX;

    /// create table for the first time.
    /// create_block_table(block_table_uri, storage_options.clone()).await;

    let mut block_partition_assignment  = PartitionAssignment::default();
    let mut need_offset_for_block = true;
    let mut need_offset_for_txn = true;
    let mut transaction_partition_assignment = PartitionAssignment::default();
    get_latest_state(block_table_uri.to_string(), app_id.to_string(), storage_options.clone(), vec![1], &mut block_partition_assignment).await;
    get_latest_state(transaction_table_uri.to_string(), app_id.to_string(), storage_options.clone(), vec![1], &mut transaction_partition_assignment).await;

    for (p, offset) in block_partition_assignment.assignment.iter() {
        println!("Partition is {:?}, and offset is {:?}", p, offset);
        match offset {
            Some(o)  => {
                if *o < max_block {
                    max_block = *o;
                }
                if need_offset_for_block {
                    need_offset_for_block = false;
                    println!("no need_offset_for_block");
                }
            }
            None => {
                if max_block > current_block  {
                    max_block = current_block;
                }
            }
            _ => {
                println!("why block");
            }
        }
    }

    for (p, offset) in transaction_partition_assignment.assignment.iter() {
        println!("Partition is {:?}, and offset is {:?}", p, offset);
        match offset {
            Some(o) => {
                if *o < max_block {
                    max_block = *o;
                }
                if need_offset_for_txn {
                    need_offset_for_txn = false;
                    println!("no need_offset_for_txn");
                }
            }
            None => {
                if max_block > current_block {
                    max_block = current_block;
                }
            }
            _ => {
                println!("why txn");
            }
        }
    }

    println!("Start to ingestion from block: {:?}.", max_block);

    let (tx, rx): (Sender<EthereumBlockRawData>, Receiver<EthereumBlockRawData>) = unbounded();
    /*let (block_tx, block_rx): (Sender<FinalBlock>, Receiver<FinalBlock>) = unbounded();
    let (transactions_tx, transaction_rx): (Sender<EthereumTransactions>, Receiver<EthereumTransactions>) = unbounded();
    let (receipts_tx, receipts_rx): (Sender<EthereumReceipts>, Receiver<EthereumReceipts>) = unbounded();*/

    let (block_msg_tx, block_msg_rx): (Sender<Message>, Receiver<Message>) = unbounded();
    let (transaction_msg_tx, transaction_msg_rx): (Sender<Message>, Receiver<Message>) = unbounded();


    /// Block write to delta table thread
    let block_storage_options = storage_options.clone();
    let block_write_handler = tokio::spawn(async move {
        let mut block_ingest_opts = IngestOptions::default();
        if need_offset_for_block {
            block_ingest_opts.seek_offsets = Option::Some(vec![(partition, max_block - 1)]);
        }
        let run = start_ingest(
            block_table_uri.to_string(),
            block_storage_options,
            block_ingest_opts,
            block_msg_rx
        ).await;
        match &run {
            Ok(_) => info!("Ingest block service exited gracefully"),
            Err(e) => panic!("Ingest block service exited with error {:?}", e),
        }
        run
    });

    /// Transaction write to delta table thread
    let txn_storage_options = storage_options.clone();
    let txn_write_handler = tokio::spawn(async move {
        let mut txn_ingest_opts = IngestOptions::default();
        if need_offset_for_txn {
            txn_ingest_opts.seek_offsets = Option::Some(vec![(partition, max_block - 1)]);
        }
        let run = start_ingest(
            transaction_table_uri.to_string(),
            txn_storage_options,
            txn_ingest_opts,
            transaction_msg_rx
        ).await;
        match &run {
            Ok(_) => info!("Ingest txn service exited gracefully"),
            Err(e) => {
                println!("Ingest error: {:?}", e);
                panic!("Ingest txn service exited with error {:?}", e)
            },
        }
        run
    });

    /// Block sequencing thread
    /*let block_seq_handler = tokio::spawn(async move {
        let mut block_map: HashMap<u64, FinalBlock> = HashMap::new();
        let mut next_block: u64 = max_block as u64;
        for block in block_rx.iter() {
            let block_num = block.number;
            println!("Get processed block: {:?} and next block to write is {:?}.", block_num, next_block);
            block_map.insert(block_num, block);
            if next_block == block_num {
                loop {
                    match block_map.get(&next_block) {
                        Some(final_block) => {
                            // write to writing queue.
                            let offset = final_block.number.to_string().parse::<i64>().unwrap();
                            let block_msg = Message{partition, offset, values: vec![serde_json::to_value(final_block).unwrap()]};
                            println!("Send block {:?} message to block write queue.", final_block.number);
                            block_msg_tx.send(block_msg).expect("send block msg panic");
                            block_map.remove(&next_block);
                            next_block += 1;
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }
        }
    });

    /// Transaction sequencing thread
    let txn_seq_handler = tokio::spawn(async move {
        let mut block_map: HashMap<u64, EthereumTransactions> = HashMap::new();
        let mut next_block: u64 = max_block as u64;
        for transactions in transaction_rx.iter() {
            let block_num = transactions.block_num;
            println!("Get processed transactions for block {:?}.", block_num);
            block_map.insert(block_num, transactions);
            if next_block == block_num {
                loop {
                    match block_map.get(&next_block) {
                        Some(block_transactions) => {
                            // write to writing queue.
                            let offset = block_transactions.block_num.to_string().parse::<i64>().unwrap();
                            let txn_msg = Message{partition, offset, values: block_transactions.transactions.iter().map(|txn| serde_json::to_value(txn).unwrap()).collect()};
                            println!("Send block {:?} message to txn write queue.", block_transactions.block_num);
                            transaction_msg_tx.send(txn_msg).expect("send txn msg panic");
                            block_map.remove(&next_block);
                            next_block += 1;
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }
        }
    });*/

    /// Receipt sequencing thread
    /*tokio::spawn(async move {
        for receipts in receipts_rx.iter() {
            println!("Get processed receipts for block: {:?}.", receipts.block.number);
            // start to write to delta table
        }
    });*/

    /// Raw data processing thread
    let raw_data_handler = tokio::spawn(async move {
        for raw_block in rx.iter() {
            let (block, eth_transactions, eth_receipts) = process_raw_block(raw_block).await;
            let blk_num = block.number;
            println!("Finished processed for raw block: {:?}", blk_num);
            let offset = blk_num.to_string().parse::<i64>().unwrap();
            let block_msg = Message{partition, offset, values: vec![serde_json::to_value(block).unwrap()]};
            let txn_msg = Message{partition, offset, values: eth_transactions.transactions.iter().map(|txn| serde_json::to_value(txn).unwrap()).collect()};
            block_msg_tx.send(block_msg).expect("send block msg panic");
            transaction_msg_tx.send(txn_msg).expect("send txn msg panic");
            println!("Finished sending block message to block and txn queue: {:?}", blk_num);
            /*
            let block_tx = block_tx.clone();
            let transactions_tx = transactions_tx.clone();
            let receipt_tx = receipts_tx.clone();
            tokio::spawn(async move {
                let (block, eth_transactions, eth_receipts) = process_raw_block(raw_block).await;
                println!("Finished processed for raw block: {:?}", block.number);
                block_tx.send(block).expect("send block to block seq queue panic");
                transactions_tx.send(eth_transactions).expect("send txn to txn seq queue panic");
                // receipts_tx.send(eth_receipts);
            });*/
        }
    });

    let block_fetch_handler = tokio::spawn(async move {
        println!("start with last_block: {}", max_block);
        let mut cur_block = U64::from(max_block);
        let batch_size = 10;

        let provider = Arc::new(Provider::<Http>::try_from(full_node_provider).expect("could not instantiate HTTP Provider"));

        loop {
            let latest_block = provider.get_block_number().await.unwrap();
            println!("Get the latest block number: {:?}", latest_block);

            while cur_block < latest_block {
                println!("start to request block: {:?} and latest block is {:?}", cur_block, latest_block);
                let block = provider.get_block_with_txs(cur_block).await.unwrap().unwrap();
                println!("get block: {:?} and latest block is {:?}", cur_block, latest_block);
                let receipts = provider.get_block_receipts(cur_block).await.unwrap();
                println!("get receipt: {:?} and latest block is {:?}", cur_block, latest_block);
                let raw_block = EthereumBlockRawData {block, receipts};
                tx.send(raw_block).expect("send raw data panic");
                println!("send block {:?} to ras processing queue", cur_block);
                cur_block += 1.into();
            }

            thread::sleep(time::Duration::from_secs(10));
        }
    });

    block_fetch_handler.await?;
    raw_data_handler.await?;
    block_write_handler.await?.expect("write block error");
    txn_write_handler.await?.expect("write txn error.");

    println!("Finish the sync.");

    Ok(())
}*/
