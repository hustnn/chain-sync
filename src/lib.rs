pub mod writer;
pub mod ethereum;
pub mod delta_helpers;
pub mod value_buffers;
pub mod coercions;

use crate::{writer::json::JsonWriter};
use crate::value_buffers::{ConsumedBuffers, ValueBuffers};

use tokio::time::Instant;

use deltalake::{action, DeltaDataTypeVersion, DeltaTable, DeltaTableBuilder, DeltaTableConfig, DeltaTableError, DeltaTableMetaData, PartitionFilter, storage};
use delta_helpers::*;

use crate::offsets::WriteOffsetsError;
use coercions::CoercionTree;

use writer::offsets;
use crate::writer::{DeltaWriter, DeltaWriterError};

use ethereum::FinalBlock;

use std::collections::HashMap;
use std::error::Error;
use ethers::types::Res;
use log::{debug, error, info, warn};
use serde_json::Value;
use chrono::prelude::*;
use crossbeam_channel::Receiver;
use deltalake::optimize::create_merge_plan;
use crate::ethereum::getEthBlockSchema;


/// Type alias for Kafka partition
pub type DataTypePartition = i32;
/// Type alias for message offset
pub type DataTypeOffset = i64;

/// The default number of times to retry a delta commit when optimistic concurrency fails.
pub(crate) const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS: u32 = 10_000_000;

/// Errors returned by [`start_ingest`] function.
#[derive(thiserror::Error, Debug)]
pub enum IngestError {
    /// Error from [`deltalake::DeltaTable`]
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// Wrapped [`deltalake::DeltaTableError`]
        #[from]
        source: DeltaTableError,
    },

    /// Error from [`writer::DataWriter`]
    #[error("Writer error: {source}")]
    Writer {
        /// Wrapped [`DeltaWriterError`]
        #[from]
        source: DeltaWriterError,
    },

    /// Error from [`WriteOffsetsError`]
    #[error("WriteOffsets error: {source}")]
    WriteOffsets {
        /// Wrapped [`WriteOffsetsError`]
        #[from]
        source: WriteOffsetsError,
    },

    /// Error from [`serde_json`]
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        /// Wrapped [`serde_json::Error`]
        #[from]
        source: serde_json::Error,
    },

    /// Error from [`std::io`]
    #[error("IO Error: {source}")]
    IoError {
        /// Wrapped [`std::io::Error`]
        #[from]
        source: std::io::Error,
    },

    /// Error returned when a delta write fails.
    /// Ending Kafka offsets and counts for each partition are included to help identify the Kafka buffer that caused the write to fail.
    #[error(
    "Delta write failed: ending_offsets: {ending_offsets}, partition_counts: {partition_counts}, source: {source}"
    )]
    DeltaWriteFailed {
        /// Ending offsets for each partition that failed to be written to delta.
        ending_offsets: String,
        /// Message counts for each partition that failed to be written to delta.
        partition_counts: String,
        /// The underlying DeltaWriterError.
        source: DeltaWriterError,
    },

    /// Error returned when a message is received from Kafka that has already been processed.
    #[error(
    "Partition offset has already been processed - partition: {partition}, offset: {offset}"
    )]
    AlreadyProcessedPartitionOffset {
        /// The Kafka partition the message was received from
        partition: DataTypePartition,
        /// The Kafka offset of the message
        offset: DataTypeOffset,
    },

    /// Error returned when delta table is in an inconsistent state with the partition offsets being written.
    #[error("Delta table is in an inconsistent state: {0}")]
    InconsistentState(String),

    /// Error returned when a rebalance signal interrupts the run loop. This is handled by the runloop by resetting state, seeking the consumer and skipping the message.
    #[error("A rebalance signal exists while processing message")]
    RebalanceInterrupt,

    /// Error returned when the the offsets in delta log txn actions for assigned partitions have changed.
    #[error("Delta transaction log contains conflicting offsets for assigned partitions.")]
    ConflictingOffsets,

    /// Error returned when the delta schema has changed since the version used to write messages to the parquet buffer.
    #[error("Delta schema has changed and must be updated.")]
    DeltaSchemaChanged,

    /// Error returned if the committed Delta table version does not match the version specified by the commit attempt.
    #[error("Committed delta version {actual_version} does not match the version specified in the commit attempt {expected_version}")]
    UnexpectedVersionMismatch {
        /// The version specified in the commit attempt
        expected_version: DeltaDataTypeVersion,
        /// The version returned after the commit
        actual_version: DeltaDataTypeVersion,
    },
}

/// The enum to represent 'auto.offset.reset' options.
pub enum AutoOffsetReset {
    /// The "earliest" option. Messages will be ingested from the beginning of a partition on reset.
    Earliest,
    /// The "latest" option. Messages will be ingested from the end of a partition on reset.
    Latest,
}

impl AutoOffsetReset {
    /// The librdkafka config key used to specify an `auto.offset.reset` policy.
    pub const CONFIG_KEY: &'static str = "auto.offset.reset";
}

pub struct Message {
    pub partition: DataTypePartition,
    pub offset: DataTypeOffset,
    pub values: Vec<Value>,
}

/// Options for configuring the behavior of the run loop executed by the [`start_ingest`] function.
pub struct IngestOptions {
    /// Unique per topic per environment. **Must** be the same for all processes that are part of a single job.
    /// It's used as a prefix for the `txn` actions to track messages offsets between partition/writers.
    pub app_id: String,
    /// Offsets to seek to before the ingestion. Creates new delta log version with `txn` actions
    /// to store the offsets for each partition in delta table.
    /// Note that `seek_offsets` is not the starting offsets, as such, then first ingested message
    /// will be `seek_offset + 1` or the next successive message in a partition.
    /// This configuration is only applied when offsets are not already stored in delta table.
    /// Note that if offsets are already exists in delta table but they're lower than provided
    /// then the error will be returned as this could break the data integrity. If one would want to skip
    /// the data and write from the later offsets then supplying new `app_id` is a safer approach.
    pub seek_offsets: Option<Vec<(DataTypePartition, DataTypeOffset)>>,
    /// Max desired latency from when a message is received to when it is written and
    /// committed to the target delta table (in seconds)
    pub allowed_latency: u64,
    /// Number of messages to buffer before writing a record batch.
    pub max_messages_per_batch: usize,
    /// Desired minimum number of compressed parquet bytes to buffer in memory
    /// before writing to storage and committing a transaction.
    pub min_bytes_per_file: usize,
}

impl Default for IngestOptions {
    fn default() -> Self {
        IngestOptions {
            app_id: "eth_delta_ingest".to_string(),
            seek_offsets: None,
            allowed_latency: 300,
            max_messages_per_batch: 1000,
            min_bytes_per_file: 134217728,
        }
    }
}

pub async fn get_latest_state(
    table_uri: String,
    app_id: String,
    options: HashMap<String, String>,
    partitions: Vec<DataTypePartition>,
    partition_assignment: &mut PartitionAssignment) {
    let table = delta_helpers::load_table(&table_uri, options.clone()).await.unwrap();
    println!("Partition for table {:?} is {:?}", table_uri, partitions);
    println!("table schema : {:?}, version: {:?}", table.schema(), table.version());
    println!("metadata : {:?}.", table.get_metadata());
    for partition in partitions.iter() {
        let txn_app_id = txn_app_id_for_partition(app_id.as_str(), *partition);
        let version = last_txn_version(&table, &txn_app_id);
        partition_assignment.assignment.insert(*partition, version);
    }
}

pub async fn create_block_table(path: &str, options: HashMap<String, String>) -> DeltaTable {
    let schema = getEthBlockSchema();
    let storage = DeltaTableBuilder::from_uri(&path)
        .with_storage_options(options)
        .build_storage().unwrap();
    let mut table = DeltaTable::new(storage, DeltaTableConfig::default());

    let metadata = DeltaTableMetaData::new(
        Some("Ethereum Block Table".to_string()),
        Some("Block delta table".to_string()),
        None,
        schema,
        vec!["date".to_string()],
        HashMap::new(),
    );

    let protocol = action::Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
    };

    let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );

    table.create(metadata,  protocol.clone(), Some(commit_info), None).await;

    table
}

/// Executes a run loop to consume from a Kafka topic and write to a Delta table.
pub async fn start_ingest(
    table_uri: String,
    options: HashMap<String, String>,
    opts: IngestOptions,
    receiver: Receiver<Message>
) -> Result<(), IngestError> {

    println!("Start ingest..");
    // Initialize partition assignment tracking
    let mut partition_assignment = PartitionAssignment::default();

    partition_assignment.reset_with(vec![1].as_slice());

    let mut ingest_processor = IngestProcessor::new(
        table_uri.as_str(),
        options,
        opts,
    ).await?;

    // Write seek_offsets if it's supplied and has not been written yet
    ingest_processor.write_offsets_to_delta_if_any().await?;

    // reset status for the first time
    ingest_processor.reset_state(&mut partition_assignment).expect("reset state failed");

    for received_mess in receiver.iter() {
        println!("Start ingest table {} for block: {:?}", &ingest_processor.table.table_uri(), received_mess.offset);
        // println!("Json: {:?}.", received_mess.values);

        if let Err(e) = ingest_processor.process_message(received_mess).await {
            match e {
                IngestError::AlreadyProcessedPartitionOffset { partition, offset } => {
                    println!("Skipping message for table {} with partition {}, offset {} because it was already processed", &ingest_processor.table.table_uri(), partition, offset);
                    continue;
                }
                _ => {
                    println!("Error during process_message: {:?}", e);
                    return Err(e)
                },
            }
        }

        // Complete the record batch if we should
        if ingest_processor.should_complete_record_batch() {
            println!("Start complete record batch.");
            ingest_processor.complete_record_batch(&mut partition_assignment).await?;
            println!("Finish complete record batch.");
        }

        // Complete the file if we should.
        if ingest_processor.should_complete_file() {
            let timer = Instant::now();
            println!("Start complete file.");
            match ingest_processor.complete_file(&partition_assignment).await {
                Err(IngestError::ConflictingOffsets) | Err(IngestError::DeltaSchemaChanged) => {
                    println!("Conflict offset or schema change for table {}", ingest_processor.table.table_uri());
                    ingest_processor.reset_state(&mut partition_assignment)?;
                    continue;
                }
                Err(e) => {
                    println!("Error happens during write table {}.", &ingest_processor.table.table_uri());
                    return Err(e);
                }
                Ok(v) => {
                    println!("Delta version {} completed in {} milliseconds.", v, timer.elapsed().as_millis());
                    /// generate presto manifest file
                    ingest_processor.delta_writer.generate_manifests(&ingest_processor.table).await?;
                }
            }
            println!("Finish complete file for table {}.", &ingest_processor.table.table_uri());
            /*/// perform optimize after writting. Can do it at lower frequency.
            ingest_processor.optimize_file().await;*/
        }
        println!("ingest next message for table {}.", &ingest_processor.table.table_uri());
    }

    println!("Finish ingestion.");
    Ok(())
}

/// Holds state and encapsulates functionality required to process messages and write to delta.
struct IngestProcessor {
    coercion_tree: CoercionTree,
    table: DeltaTable,
    delta_writer: JsonWriter,
    value_buffers: ValueBuffers,
    delta_partition_offsets: HashMap<DataTypePartition, Option<DataTypeOffset>>,
    latency_timer: Instant,
    opts: IngestOptions,
}

impl IngestProcessor {
    /// Creates a new ingest [`IngestProcessor`].
    async fn new(
        table_uri: &str,
        options: HashMap<String, String>,
        opts: IngestOptions,
    ) -> Result<IngestProcessor, IngestError> {
        let table = delta_helpers::load_table(table_uri, options.clone()).await?;
        let delta_writer = JsonWriter::for_table(&table)?;
        let coercion_tree = coercions::create_coercion_tree(&table.get_metadata()?.schema);

        Ok(IngestProcessor {
            coercion_tree,
            table,
            delta_writer,
            value_buffers: ValueBuffers::default(),
            latency_timer: Instant::now(),
            delta_partition_offsets: HashMap::new(),
            opts,
        })
    }

    /// If `opts.seek_offsets` is set then it calls the `offsets::write_offsets_to_delta` function.
    async fn write_offsets_to_delta_if_any(&mut self) -> Result<(), IngestError> {
        if let Some(ref offsets) = self.opts.seek_offsets {
            println!("Table {:?}, app id {:?}, offset {:?}", self.table, self.opts.app_id, offsets);
            offsets::write_offsets_to_delta(&mut self.table, &self.opts.app_id, offsets).await?;
        }
        Ok(())
    }

    /// Processes a single message received from full node.
    async fn process_message(&mut self, message: Message) -> Result<(), IngestError> {
        let partition = message.partition;
        let offset = message.offset;

        if !self.should_process_offset(partition, offset) {
            return Err(IngestError::AlreadyProcessedPartitionOffset {partition, offset});
        }

        // let mut value = message.value;
        // Coerce data types
        // coercions::coerce(&mut value, &self.coercion_tree);
        // Buffer
        self.value_buffers.add_values(partition, offset, message.values)?;

        Ok(())
    }

    /// Writes the transformed messages currently held in buffer to parquet byte buffers.
    async fn complete_record_batch(
        &mut self,
        partition_assignment: &mut PartitionAssignment,
    ) -> Result<(), IngestError> {
        let ConsumedBuffers {
            values,
            partition_offsets,
            partition_counts,
        } = self.value_buffers.consume();
        partition_assignment.update_offsets(&partition_offsets);

        if values.is_empty() {
            return Ok(());
        }

        match self.delta_writer.write(values).await {
            Err(e) => {
                return Err(IngestError::DeltaWriteFailed {
                    ending_offsets: serde_json::to_string(&partition_offsets).unwrap(),
                    partition_counts: serde_json::to_string(&partition_counts).unwrap(),
                    source: e,
                });
            }
            _ => { /* ok - noop */ }
        };

        Ok(())
    }

    /// Writes parquet buffers to a file in the destination delta table.
    async fn complete_file(
        &mut self,
        partition_assignment: &PartitionAssignment,
    ) -> Result<i64, IngestError> {
        // Reset the latency timer to track allowed latency for the next file
        self.latency_timer = Instant::now();
        let partition_offsets = partition_assignment.nonempty_partition_offsets();
        // Upload pending parquet file to delta store
        // TODO: remove it if we got conflict error? or it'll be considered as tombstone

        println!("start to flush for table {}", self.table.table_uri());
        let add = self.delta_writer.flush().await?;
        // Record file sizes
        /*for a in add.iter() {
            self.ingest_metrics.delta_file_size(a.size);
        }*/
        // Try to commit

        println!("start to pre commit for table {}", self.table.table_uri());
        let mut attempt_number: u32 = 0;
        let prepared_commit = {
            let mut tx = self.table.create_transaction(None);
            tx.add_actions(build_actions(
                &partition_offsets,
                self.opts.app_id.as_str(),
                add,
            ));
            tx.prepare_commit(None, None).await?
        };

        loop {
            self.table.update().await?;
            if !self.are_partition_offsets_match() {
                return Err(IngestError::ConflictingOffsets);
            }
            if self
                .delta_writer
                .update_schema(self.table.get_metadata()?)?
            {
                println!("Table schema has been updated");
                // Update the coercion tree to reflect the new schema
                let coercion_tree =
                    coercions::create_coercion_tree(&self.table.get_metadata()?.schema);
                let _ = std::mem::replace(&mut self.coercion_tree, coercion_tree);

                return Err(IngestError::DeltaSchemaChanged);
            }
            println!("start to commit for table {}", self.table.table_uri());
            let version = self.table.version() + 1;
            let commit_result = self
                .table
                .try_commit_transaction(&prepared_commit, version)
                .await;
            match commit_result {
                Ok(v) => {
                    if v != version {
                        println!("UnexpectedVersionMismatch");
                        return Err(IngestError::UnexpectedVersionMismatch {
                            expected_version: version,
                            actual_version: v,
                        });
                    }
                    assert_eq!(v, version);
                    for (p, o) in &partition_offsets {
                        self.delta_partition_offsets.insert(*p, Some(*o));
                    }

                    /*if self.opts.write_checkpoints {
                        try_create_checkpoint(&mut self.table, version).await?;
                    }*/

                    return Ok(version);
                }
                Err(e) => match e {
                    DeltaTableError::VersionAlreadyExists(_)
                    if attempt_number > DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS + 1 =>
                        {
                            println!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing", DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS);
                            return Err(e.into());
                        }
                    DeltaTableError::VersionAlreadyExists(_) => {
                        attempt_number += 1;
                        println!("Transaction attempt failed. Incrementing attempt number to {} and retrying", attempt_number);
                    }
                    /*DeltaTableError::StorageError {
                        source:
                        StorageError::DynamoDb {
                            source: DynamoError::NonAcquirableLock,
                        },
                    } => {
                        error!("Delta write failed.. DeltaTableError: {}", e);
                        return Err(IngestError::InconsistentState(
                            "The remote dynamodb lock is non-acquirable!".to_string(),
                        ));
                    }*/
                    _ => {
                        println!("Error for table {}", self.table.table_uri());
                        return Err(e.into());
                    }
                },
            }
        }
    }

    async fn optimize_file(
        &mut self,
    ) -> Result<(), Box<dyn Error>> {
        self.table.update().await?;
        let mut dt = &mut self.table;
        println!("Version before merge {:?}.", dt.version());
        let filter = vec![PartitionFilter::try_from(("date", "=", "19171"))?];
        let plan = create_merge_plan(dt, &filter, None)?;
        let metrics = plan.execute(&mut dt).await?;
        println!("Number of files added {:?}.", metrics.num_files_added);
        println!("Number of files deleted {:?}.", metrics.num_files_removed);
        println!("Version after merge {:?}.", dt.version());
        Ok(())
    }

    /// Resets all current state to the correct starting points represented by the current partition assignment.
    fn reset_state(
        &mut self,
        partition_assignment: &mut PartitionAssignment,
    ) -> Result<(), IngestError> {
        // Reset all stored state
        self.delta_writer.reset();
        self.value_buffers.reset();
        self.delta_partition_offsets.clear();
        let partitions: Vec<DataTypePartition> = partition_assignment.assigned_partitions();
        // Update offsets stored in PartitionAssignment to the latest from the delta log
        for partition in partitions.iter() {
            let txn_app_id = txn_app_id_for_partition(self.opts.app_id.as_str(), *partition);
            let version = last_txn_version(&self.table, &txn_app_id);
            partition_assignment.assignment.insert(*partition, version);
            self.delta_partition_offsets.insert(*partition, version);
        }
        // Seek the consumer to the correct offset for each partition
        // self.seek_consumer(partition_assignment)?;
        Ok(())
    }

    /// Returns a boolean indicating whether a message with `partition` and `offset` should be processed given current state.
    fn should_process_offset(&self, partition: DataTypePartition, offset: DataTypeOffset) -> bool {
        if let Some(Some(written_offset)) = self.delta_partition_offsets.get(&partition) {
            if offset <= *written_offset {
                /*debug!(
                    "Message with partition {} offset {} on topic {} is already in delta log so skipping.",
                    partition, offset, self.topic
                );*/
                return false;
            }
        }

        true
    }

    /// Returns a boolean indicating whether a record batch should be written based on current state.
    fn should_complete_record_batch(&self) -> bool {
        let elapsed_millis = self.latency_timer.elapsed().as_millis();

        let should = self.value_buffers.len() > 0
            && (self.value_buffers.len() == self.opts.max_messages_per_batch
            || elapsed_millis >= (self.opts.allowed_latency * 1000) as u128);

        debug!(
            "Should complete record batch - latency test: {} >= {}",
            elapsed_millis,
            (self.opts.allowed_latency * 1000) as u128
        );
        debug!(
            "Should complete record batch - buffer length test: {} >= {}",
            self.value_buffers.len(),
            self.opts.max_messages_per_batch
        );

        should
    }

    /// Returns a boolean indicating whether a delta file should be completed based on current state.
    fn should_complete_file(&self) -> bool {
        let elapsed_secs = self.latency_timer.elapsed().as_secs();

        let should = self.delta_writer.buffer_len() > 0
            && (self.delta_writer.buffer_len() >= self.opts.min_bytes_per_file
            || elapsed_secs >= self.opts.allowed_latency);

        debug!(
            "Should complete file - latency test: {} >= {}",
            elapsed_secs, self.opts.allowed_latency
        );
        debug!(
            "Should complete file - num bytes test: {} >= {}",
            self.delta_writer.buffer_len(),
            self.opts.min_bytes_per_file
        );

        should
    }

    /// Returns a boolean indicating whether the partition offsets currently held in memory match those stored in the delta log.
    fn are_partition_offsets_match(&self) -> bool {
        let mut result = true;
        for (partition, offset) in &self.delta_partition_offsets {
            let version = last_txn_version(
                &self.table,
                &txn_app_id_for_partition(self.opts.app_id.as_str(), *partition),
            );

            if let Some(version) = version {
                match offset {
                    Some(offset) if *offset == version => (),
                    _ => {
                        println!(
                            "Conflicting offset for partition {}: offset={:?}, delta={}",
                            partition, offset, version
                        );
                        result = false;
                    }
                }
            }
        }
        result
    }

    fn buffered_record_batch_count(&self) -> usize {
        self.delta_writer.buffered_record_batch_count()
    }
}

/// Contains the partition to offset map for all partitions assigned to the consumer.
#[derive(Default)]
pub struct PartitionAssignment {
    pub assignment: HashMap<DataTypePartition, Option<DataTypeOffset>>,
}

impl PartitionAssignment {
    /// Resets the [`PartitionAssignment`] with a new list of partitions.
    /// Offsets are set as [`None`] for all partitions.
    fn reset_with(&mut self, partitions: &[DataTypePartition]) {
        self.assignment.clear();
        for p in partitions {
            self.assignment.insert(*p, None);
        }
    }

    /// Updates the offsets for each partition stored in the [`PartitionAssignment`].
    fn update_offsets(&mut self, updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>) {
        for (k, v) in updated_offsets {
            if let Some(entry) = self.assignment.get_mut(k) {
                *entry = Some(*v);
            }
        }
    }

    /// Returns the full list of assigned partitions as a [`Vec`] whether offsets are recorded for them in-memory or not.
    fn assigned_partitions(&self) -> Vec<DataTypePartition> {
        self.assignment.keys().copied().collect()
    }

    /// Returns a copy of the current partition offsets as a [`HashMap`] for all partitions that have an offset stored in memory.
    /// Partitions that do not have an offset stored in memory (offset is [`None`]) are **not** included in the returned HashMap.
    fn nonempty_partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
        let partition_offsets = self
            .assignment
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|o| (*k, *o)))
            .collect();

        partition_offsets
    }
}

pub struct IngestProcess {
    partition_assignment: PartitionAssignment,
    ingest_processor: IngestProcessor,
}

impl IngestProcess {
    pub async fn new(
        table_uri: &str,
        options: HashMap<String, String>,
        opts: IngestOptions
    ) -> Result<IngestProcess, IngestError> {
        let mut ingest_processor = IngestProcessor::new(
            table_uri,
            options,
            opts,
        ).await?;

        let mut partition_assignment = PartitionAssignment::default();
        partition_assignment.reset_with(vec![1].as_slice());

        ingest_processor.write_offsets_to_delta_if_any().await?;

        Ok(IngestProcess {
            partition_assignment,
            ingest_processor
        })
    }

    pub fn reset_state(&mut self) -> Result<(), IngestError> {
        self.ingest_processor.reset_state(&mut self.partition_assignment)
    }

    pub async fn ingest_data(&mut self, received_mess: Message) -> Result<(), IngestError> {
        println!("Start ingest table {} for block: {:?}", &self.ingest_processor.table.table_uri(), received_mess.offset);
        // println!("Json: {:?}.", received_mess.values);

        if let Err(e) = self.ingest_processor.process_message(received_mess).await {
            match e {
                IngestError::AlreadyProcessedPartitionOffset { partition, offset } => {
                    println!("Skipping message for table {} with partition {}, offset {} because it was already processed", &self.ingest_processor.table.table_uri(), partition, offset);
                    return Err(e)
                }
                _ => {
                    println!("Error during process_message: {:?}", e);
                    return Err(e)
                },
            }
        }

        // Complete the record batch if we should
        if self.ingest_processor.should_complete_record_batch() {
            println!("Start complete record batch.");
            self.ingest_processor.complete_record_batch(&mut self.partition_assignment).await?;
            println!("Finish complete record batch.");
        }

        // Complete the file if we should.
        if self.ingest_processor.should_complete_file() {
            let timer = Instant::now();
            println!("Start complete file.");
            match self.ingest_processor.complete_file(&self.partition_assignment).await {
                Err(IngestError::ConflictingOffsets) | Err(IngestError::DeltaSchemaChanged) => {
                    println!("Conflict offset or schema change for table {}", self.ingest_processor.table.table_uri());
                    self.ingest_processor.reset_state(&mut self.partition_assignment)?;
                }
                Err(e) => {
                    println!("Error happens during write table {}.", &self.ingest_processor.table.table_uri());
                    return Err(e);
                }
                Ok(v) => {
                    println!("Delta version {} completed in {} milliseconds.", v, timer.elapsed().as_millis());
                    /// generate presto manifest file
                    self.ingest_processor.delta_writer.generate_manifests(&self.ingest_processor.table).await?;
                }
            }
            println!("Finish complete file for table {}.", &self.ingest_processor.table.table_uri());
        }
        println!("ingest next message for table {}.", &self.ingest_processor.table.table_uri());
        Ok(())
    }
}