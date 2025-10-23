use {
    crate::{
        clickhouse::{
            buffer::{BatchBuffer, BatchConvertible},
            pool::{ConnectionPool, TBL_BLOCK},
            ClickHouseError,
        },
        config::{ArrowCfg, BatchCfg, ConnectionCfg},
        metrics::Metrics,
        workers::consts::{
            CONNECTION_ACQUIRE_TIMEOUT, FLUSH_TICK_INTERVAL, HEALTH_CHECK_INTERVAL,
            MAX_CONSECUTIVE_ERRORS, MAX_FLUSH_ERRORS, STATS_INTERVAL, WORKER_SHUTDOWN_TIMEOUT,
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaBlockInfoV3, ReplicaBlockInfoV4,
    },
    chrono::{DateTime, Utc},
    clickhouse_arrow::{
        arrow::arrow::{
            array::{
                FixedSizeBinaryBuilder, Int16Builder, Int64Builder, ListBuilder, RecordBatch,
                StringBuilder, TimestampMillisecondBuilder, UInt64Builder, UInt8Builder,
            },
            datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        },
        tracing::debug,
    },
    futures_util::StreamExt,
    kanal::{Receiver, Sender},
    log::{error, info, warn},
    solana_transaction_status::Reward,
    std::{
        mem,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    tokio::{select, task::JoinHandle, time::timeout},
};

#[derive(Debug, Clone)]
pub struct Block {
    pub slot: u64,
    pub blockhash: String,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub parent_slot: u64,
    pub updated_at: DateTime<Utc>,
    pub rewards_pubkey: Vec<[u8; 32]>,
    pub rewards_lamports: Vec<i64>,
    pub rewards_post_balance: Vec<i64>,
    pub rewards_reward_type: Vec<u8>,
    pub rewards_commission: Vec<Option<i16>>,
}

impl BatchConvertible for Block {
    fn to_record_batch(items: &[Self]) -> Result<RecordBatch, ClickHouseError> {
        if items.is_empty() {
            return Err(ClickHouseError::BufferOverflow(
                "cannot create RecordBatch from empty items".into(),
            ));
        }

        let len = items.len();
        let mut slot_builder = UInt64Builder::with_capacity(len);
        let mut blockhash_builder = StringBuilder::with_capacity(len, len * 44);
        let mut block_time_builder = TimestampMillisecondBuilder::with_capacity(len);
        let mut block_height_builder = UInt64Builder::with_capacity(len);
        let mut parent_slot_builder = UInt64Builder::with_capacity(len);
        let mut updated_at_builder = TimestampMillisecondBuilder::with_capacity(len);

        let total_rewards: usize = items.iter().map(|b| b.rewards_pubkey.len()).sum();
        let mut rewards_pubkey_builder =
            ListBuilder::new(FixedSizeBinaryBuilder::with_capacity(total_rewards, 32));
        let mut rewards_lamports_builder =
            ListBuilder::new(Int64Builder::with_capacity(total_rewards));
        let mut rewards_post_balance_builder =
            ListBuilder::new(Int64Builder::with_capacity(total_rewards));
        let mut rewards_reward_type_builder =
            ListBuilder::new(UInt8Builder::with_capacity(total_rewards));
        let mut rewards_commission_builder =
            ListBuilder::new(Int16Builder::with_capacity(total_rewards));

        for item in items {
            slot_builder.append_value(item.slot);
            blockhash_builder.append_value(&item.blockhash);

            match item.block_time {
                Some(ms) => block_time_builder.append_value(ms),
                None => block_time_builder.append_null(),
            }

            match item.block_height {
                Some(h) => block_height_builder.append_value(h),
                None => block_height_builder.append_null(),
            }

            parent_slot_builder.append_value(item.parent_slot);
            updated_at_builder.append_value(item.updated_at.timestamp_millis());

            let pubkey_values = rewards_pubkey_builder.values();
            for pubkey in &item.rewards_pubkey {
                pubkey_values.append_value(pubkey).map_err(|e| {
                    ClickHouseError::Connection(format!("append rewards_pubkey failed: {e}"))
                })?;
            }

            rewards_pubkey_builder.append(true);

            let lamports_values = rewards_lamports_builder.values();
            for lamport in &item.rewards_lamports {
                lamports_values.append_value(*lamport);
            }
            rewards_lamports_builder.append(true);

            let post_balance_values = rewards_post_balance_builder.values();
            for balance in &item.rewards_post_balance {
                post_balance_values.append_value(*balance);
            }
            rewards_post_balance_builder.append(true);

            let reward_type_values = rewards_reward_type_builder.values();
            for reward_type in &item.rewards_reward_type {
                reward_type_values.append_value(*reward_type);
            }
            rewards_reward_type_builder.append(true);

            let commission_values = rewards_commission_builder.values();
            for commission in &item.rewards_commission {
                match commission {
                    Some(c) => commission_values.append_value(*c),
                    None => commission_values.append_null(),
                }
            }
            rewards_commission_builder.append(true);
        }

        let schema = Self::schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(slot_builder.finish()),
                Arc::new(blockhash_builder.finish()),
                Arc::new(block_time_builder.finish()),
                Arc::new(block_height_builder.finish()),
                Arc::new(parent_slot_builder.finish()),
                Arc::new(updated_at_builder.finish()),
                Arc::new(rewards_pubkey_builder.finish()),
                Arc::new(rewards_lamports_builder.finish()),
                Arc::new(rewards_post_balance_builder.finish()),
                Arc::new(rewards_reward_type_builder.finish()),
                Arc::new(rewards_commission_builder.finish()),
            ],
        )
        .map_err(|e| ClickHouseError::Connection(format!("arrow RecordBatch creation failed: {e}")))
    }

    fn memory_size(&self) -> usize {
        mem::size_of::<u64>() * 2
            + self.blockhash.len()
            + mem::size_of::<Option<i64>>()
            + mem::size_of::<Option<u64>>()
            + mem::size_of::<DateTime<Utc>>()
            + self.rewards_pubkey.len() * 32
            + self.rewards_lamports.len() * mem::size_of::<i64>()
            + self.rewards_post_balance.len() * mem::size_of::<i64>()
            + self.rewards_reward_type.len() * mem::size_of::<u8>()
            + self.rewards_commission.len() * mem::size_of::<Option<i16>>()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("slot", DataType::UInt64, false),
            Field::new("blockhash", DataType::Utf8, false),
            Field::new(
                "block_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("block_height", DataType::UInt64, true),
            Field::new("parent_slot", DataType::UInt64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "rewards_pubkey",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeBinary(32),
                    true,
                ))),
                false,
            ),
            Field::new(
                "rewards_lamports",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
            Field::new(
                "rewards_post_balance",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
            Field::new(
                "rewards_reward_type",
                DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                false,
            ),
            Field::new(
                "rewards_commission",
                DataType::List(Arc::new(Field::new("item", DataType::Int16, true))),
                false,
            ),
        ]))
    }
}

pub struct BlockWorker {
    id: usize,
    pool: Arc<ConnectionPool>,
    buffer: BatchBuffer<Block>,
    metrics: Metrics,
    last_health_check: Instant,
}

impl BlockWorker {
    pub fn new(
        id: usize,
        pool: Arc<ConnectionPool>,
        arrow_cfg: &ArrowCfg,
        max_rows: u64,
        max_bytes: u64,
        flush_ms: u64,
    ) -> Result<Self, ClickHouseError> {
        let buffer = BatchBuffer::new(
            max_rows as usize,
            max_bytes as usize,
            Duration::from_millis(flush_ms),
            arrow_cfg,
        );
        Ok(Self {
            id,
            pool,
            buffer,
            metrics: Metrics::new(id, "block"),
            last_health_check: Instant::now(),
        })
    }

    async fn process_block(&mut self, block: Block) -> Result<(), ClickHouseError> {
        self.metrics.total_processed.fetch_add(1, Ordering::Relaxed);

        if let Err(block) = self.buffer.push(block) {
            self.metrics
                .memory_pressure_events
                .fetch_add(1, Ordering::Relaxed);

            if let Err(e) = self.flush().await {
                self.metrics.record_error();
                return Err(e);
            }

            if let Err(block) = self.buffer.push(block) {
                self.buffer.push_oversized(block).map_err(|e| {
                    self.metrics.record_error();
                    e
                })?;
            }
        }

        let current_memory = self.buffer.memory_usage();
        self.metrics.max_memory_used.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |max| if current_memory > max { Some(current_memory) } else { None }
        ).ok();

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ClickHouseError> {
        let flush_start = Instant::now();
        let batch_result = self.buffer.flush_to_arrow();
        let record_batch = match batch_result {
            Ok(Some(batch)) => batch,
            Ok(None) => return Ok(()),
            Err(e) => {
                self.metrics.record_error();
                return Err(e);
            }
        };
        let rows = record_batch.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let conn = match timeout(CONNECTION_ACQUIRE_TIMEOUT, self.pool.get_connection()).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                self.metrics.record_error();
                return Err(e);
            }
            Err(_) => {
                self.metrics.record_connection_timeout();
                return Err(ClickHouseError::Timeout(
                    "connection acquire timeout in flush".into(),
                ));
            }
        };
        let insert_query = format!("INSERT INTO {} FORMAT ArrowStream", TBL_BLOCK);
        let mut insert_stream = conn
            .insert(insert_query, record_batch, None)
            .await
            .map_err(|e| {
                self.metrics.record_error();
                ClickHouseError::Connection(format!("failed to create insert stream: {e}"))
            })?;
        while let Some(result) = insert_stream.next().await {
            result.map_err(|e| {
                self.metrics.record_error();
                ClickHouseError::Connection(format!("insert stream error: {e}"))
            })?;
        }
        let flush_duration = flush_start.elapsed();
        self.metrics
            .total_flushed
            .fetch_add(rows as u64, Ordering::Relaxed);
        self.metrics.flush_count.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_flush_duration
            .store(flush_duration.as_micros() as u64, Ordering::Relaxed);
        self.metrics.reset_error_count();
        debug!(
            "worker {} flushed {} block records via Arrow in {:?}",
            self.id, rows, flush_duration
        );
        Ok(())
    }

    async fn health_check(&mut self) -> Result<(), ClickHouseError> {
        if self.last_health_check.elapsed() < HEALTH_CHECK_INTERVAL {
            return Ok(());
        }
        match self.pool.health_check().await {
            Ok(_) => {
                self.last_health_check = Instant::now();
                Ok(())
            }
            Err(e) => {
                self.metrics.record_error();
                Err(e)
            }
        }
    }

    async fn run_impl(
        &mut self,
        receiver: Receiver<Block>,
        exit_flag: Arc<AtomicBool>,
        startup_done_flag: Arc<AtomicBool>,
        startup_done_count: Arc<AtomicUsize>,
    ) -> Result<(), ClickHouseError> {
        let mut is_startup_done = false;
        let mut flush_ticker = tokio::time::interval(FLUSH_TICK_INTERVAL);
        let mut stats_ticker = tokio::time::interval(STATS_INTERVAL);
        let mut health_ticker = tokio::time::interval(HEALTH_CHECK_INTERVAL);
        info!("block worker {} started", self.id);
        loop {
            select! {
                block_result = receiver.as_async().recv() => {
                    match block_result {
                        Ok(block) => {
                            if let Err(e) = self.process_block(block).await {
                                let consecutive_errors = self.metrics.consecutive_error_count();
                                if consecutive_errors > MAX_CONSECUTIVE_ERRORS {
                                    error!(
                                        "block worker {} has {} consecutive errors, shutting down: {}",
                                        self.id, consecutive_errors, e
                                    );
                                    return Err(e);
                                }
                                warn!("block worker {} error (consecutive: {}): {}", self.id, consecutive_errors, e);
                            }
                        },
                        Err(_) => {
                            debug!("block worker {} Kanal channel closed", self.id);
                            break;
                        }
                    }
                },
                _ = flush_ticker.tick() => {
                    let (should_flush, reason) = self.buffer.should_flush();
                    if should_flush {
                        if let Err(e) = self.flush().await {
                            let consecutive_errors = self.metrics.consecutive_error_count();
                            if consecutive_errors > MAX_FLUSH_ERRORS {
                                error!(
                                    "block worker {} has {} errors. Exceeded threshold, shutting down: {}",
                                    self.id, consecutive_errors, e
                                );
                                return Err(e);
                            }
                            warn!("block worker {} flush error: {} (reason: {:?})", self.id, e, reason);
                        }
                    }
                },
                _ = stats_ticker.tick() => {
                    self.metrics.log_stats();
                },
                _ = health_ticker.tick() => {
                    if let Err(e) = self.health_check().await {
                        debug!("block worker {} health check failed (non-fatal): {}", self.id, e);
                    }
                },
            }
            if exit_flag.load(Ordering::Relaxed) {
                info!("block worker {} received exit signal", self.id);
                break;
            }
            if !is_startup_done && startup_done_flag.load(Ordering::Relaxed) {
                if let Err(e) = self.flush().await {
                    warn!("block worker {} startup flush failed: {}", self.id, e);
                }
                is_startup_done = true;
                startup_done_count.fetch_add(1, Ordering::Relaxed);
                info!("block worker {} startup complete", self.id);
            }
        }
        self.flush().await?;
        info!("block worker {} shutdown", self.id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::workers::ManagedWorker for BlockWorker {
    type Item = Block;

    fn entity_name() -> &'static str {
        "Block"
    }

    fn create(
        id: usize,
        pool: Arc<ConnectionPool>,
        arrow_cfg: &ArrowCfg,
        max_rows: u64,
        max_bytes: u64,
        flush_ms: u64,
    ) -> Result<Self, ClickHouseError> {
        BlockWorker::new(id, pool, arrow_cfg, max_rows, max_bytes, flush_ms)
    }

    async fn run(
        mut self,
        receiver: Receiver<Block>,
        exit_flag: Arc<AtomicBool>,
        startup_done_flag: Arc<AtomicBool>,
        startup_done_count: Arc<AtomicUsize>,
    ) -> Result<(), ClickHouseError> {
        self.run_impl(receiver, exit_flag, startup_done_flag, startup_done_count)
            .await
    }
}

pub struct BlockManager {
    pool: Arc<ConnectionPool>,
    sender: Sender<Block>,
    workers: Vec<JoinHandle<Result<(), ClickHouseError>>>,
    exit_flag: Arc<AtomicBool>,
    worker_count: usize,
    channel_capacity: usize,
}

impl std::fmt::Debug for BlockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockManager")
            .field("worker_count", &self.worker_count)
            .field("channel_capacity", &self.channel_capacity)
            .finish_non_exhaustive()
    }
}

impl BlockManager {
    pub async fn new(
        conn_config: &ConnectionCfg,
        batch_cfg: &BatchCfg,
        arrow_cfg: &ArrowCfg,
        channel_size: usize,
    ) -> Result<Self, ClickHouseError> {
        let components = crate::workers::build_manager::<BlockWorker>(
            conn_config,
            &batch_cfg.blocks,
            arrow_cfg,
            channel_size,
        )
        .await?;

        Ok(Self {
            pool: components.pool,
            sender: components.sender,
            workers: components.workers,
            exit_flag: components.exit_flag,
            worker_count: components.worker_count,
            channel_capacity: components.channel_capacity,
        })
    }

    pub fn try_send_block(&self, block: Block) -> Result<(), ClickHouseError> {
        match self.sender.try_send(block) {
            Ok(true) => Ok(()),
            Ok(false) => Err(ClickHouseError::BufferOverflow("block channel full".into())),
            Err(_) => Err(ClickHouseError::ChannelClosed(
                "block channel closed".into(),
            )),
        }
    }

    pub async fn shutdown(self) -> Result<(), ClickHouseError> {
        info!(
            "shutting down BlockManager with {} workers",
            self.worker_count
        );
        self.exit_flag.store(true, Ordering::Relaxed);
        drop(self.sender);
        let mut shutdown_errors = Vec::new();
        for (i, handle) in self.workers.into_iter().enumerate() {
            match timeout(WORKER_SHUTDOWN_TIMEOUT, handle).await {
                Ok(Ok(Ok(()))) => {
                    debug!("worker {} shutdown cleanly", i);
                }
                Ok(Ok(Err(e))) => {
                    error!("worker {} shutdown with error: {}", i, e);
                    shutdown_errors.push(e);
                }
                Ok(Err(e)) => {
                    error!("worker {} panicked: {}", i, e);
                    shutdown_errors
                        .push(ClickHouseError::Connection(format!("worker panic: {}", e)));
                }
                Err(_) => {
                    error!("worker {} shutdown timeout", i);
                    shutdown_errors.push(ClickHouseError::Timeout(format!(
                        "worker {} shutdown timeout",
                        i
                    )));
                }
            }
        }
        if shutdown_errors.is_empty() {
            info!("BlockManager shutdown complete");
            Ok(())
        } else {
            let error_msg = format!("shutdown completed with {} errors", shutdown_errors.len());
            error!("{}", error_msg);
            Err(ClickHouseError::Connection(error_msg))
        }
    }

    pub fn pool(&self) -> Arc<ConnectionPool> {
        self.pool.clone()
    }

    pub fn channel_capacity(&self) -> usize {
        self.channel_capacity
    }

    pub fn worker_count(&self) -> usize {
        self.worker_count
    }
}

fn collect_rewards<'a, I>(iter: I) -> (Vec<[u8; 32]>, Vec<i64>, Vec<i64>, Vec<u8>, Vec<Option<i16>>)
where
    I: IntoIterator<Item = &'a Reward>,
{
    let iter = iter.into_iter();
    let (lower, _) = iter.size_hint();
    let mut rewards_pubkey = Vec::with_capacity(lower);
    let mut rewards_lamports = Vec::with_capacity(lower);
    let mut rewards_post_balance = Vec::with_capacity(lower);
    let mut rewards_reward_type = Vec::with_capacity(lower);
    let mut rewards_commission = Vec::with_capacity(lower);

    for r in iter {
        match bs58::decode(&r.pubkey).into_vec() {
            Ok(bytes) if bytes.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                rewards_pubkey.push(arr);
            }
            _ => {
                warn!(
                    "skipping invalid pubkey (not 32 bytes after base58): {}",
                    r.pubkey
                );
                continue;
            }
        }

        rewards_lamports.push(r.lamports);
        rewards_post_balance.push(r.post_balance as i64);
        rewards_reward_type.push(match r.reward_type {
            Some(solana_transaction_status::RewardType::Fee) => 1,
            Some(solana_transaction_status::RewardType::Rent) => 2,
            Some(solana_transaction_status::RewardType::Staking) => 3,
            Some(solana_transaction_status::RewardType::Voting) => 4,
            None => 0,
        });
        rewards_commission.push(r.commission.map(|c| c as i16));
    }

    (
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_reward_type,
        rewards_commission,
    )
}

pub fn build_block_v3(block_info: &ReplicaBlockInfoV3) -> (u64, Block) {
    let (
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_reward_type,
        rewards_commission,
    ) = collect_rewards(block_info.rewards.iter());

    let block_time_ms = block_info
        .block_time
        .and_then(|t| (t >= 0).then(|| t * 1000));

    let block = Block {
        slot: block_info.slot,
        blockhash: block_info.blockhash.to_string(),
        block_time: block_time_ms,
        block_height: block_info.block_height,
        parent_slot: block_info.parent_slot,
        updated_at: Utc::now(),
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_reward_type,
        rewards_commission,
    };
    (block_info.slot, block)
}

pub fn build_block_v4(block_info: &ReplicaBlockInfoV4) -> (u64, Block) {
    let (
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_reward_type,
        rewards_commission,
    ) = collect_rewards(block_info.rewards.rewards.iter());

    let block_time_ms = block_info
        .block_time
        .and_then(|t| (t >= 0).then(|| t * 1000));

    let block = Block {
        slot: block_info.slot,
        blockhash: block_info.blockhash.to_string(),
        block_time: block_time_ms,
        block_height: block_info.block_height,
        parent_slot: block_info.parent_slot,
        updated_at: Utc::now(),
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_reward_type,
        rewards_commission,
    };
    (block_info.slot, block)
}
