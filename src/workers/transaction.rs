use {
    crate::{
        clickhouse::{
            buffer::{BatchBuffer, BatchConvertible},
            pool::{ConnectionPool, TBL_TRANSACTION},
            ClickHouseError,
        },
        config::{ArrowCfg, BatchCfg, ConnectionCfg},
        metrics::Metrics,
        workers::consts::{
            CONNECTION_ACQUIRE_TIMEOUT, FLUSH_TICK_INTERVAL, HEALTH_CHECK_INTERVAL,
            MAX_CONSECUTIVE_ERRORS, MAX_FLUSH_ERRORS, STARTUP_TIMEOUT, STATS_INTERVAL,
            WORKER_SHUTDOWN_TIMEOUT,
        },
    },
    chrono::{DateTime, Utc},
    clickhouse_arrow::arrow::arrow::{
        array::{
            BinaryBuilder, BooleanBuilder, ListBuilder, RecordBatch, TimestampMillisecondBuilder,
            UInt64Builder, UInt8Builder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    futures_util::StreamExt,
    kanal::{bounded, Receiver, Sender},
    std::{
        mem,
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    tokio::{
        select,
        task::JoinHandle,
        time::{sleep, timeout},
    },
};

#[derive(Debug, Clone)]
pub struct BalanceChange {
    pub signature: [u8; 64],
    pub account: [u8; 32],
    pub account_index: u8,
    pub pre_balance: u64,
    pub post_balance: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub signature: [u8; 64],
    pub slot: u64,
    pub tx_index: usize,
    pub is_vote: bool,
    pub message_type: u8,
    pub success: bool,
    pub fee: u64,
    pub balance_changes: Vec<BalanceChange>,
    pub updated_at: DateTime<Utc>,
}

impl BatchConvertible for Transaction {
    fn to_record_batch(items: &[Self]) -> Result<RecordBatch, ClickHouseError> {
        if items.is_empty() {
            return RecordBatch::try_new(
                Self::schema(),
                vec![
                    Arc::new(BinaryBuilder::new().finish()),
                    Arc::new(UInt64Builder::new().finish()),
                    Arc::new(UInt64Builder::new().finish()),
                    Arc::new(BooleanBuilder::new().finish()),
                    Arc::new(UInt8Builder::new().finish()),
                    Arc::new(BooleanBuilder::new().finish()),
                    Arc::new(UInt64Builder::new().finish()),
                    Arc::new(TimestampMillisecondBuilder::new().finish()),
                    Arc::new(
                        ListBuilder::new(BinaryBuilder::new())
                            .with_field(Arc::new(Field::new("item", DataType::Binary, false)))
                            .finish(),
                    ),
                    Arc::new(
                        ListBuilder::new(UInt8Builder::new())
                            .with_field(Arc::new(Field::new("item", DataType::UInt8, false)))
                            .finish(),
                    ),
                    Arc::new(
                        ListBuilder::new(UInt64Builder::new())
                            .with_field(Arc::new(Field::new("item", DataType::UInt64, false)))
                            .finish(),
                    ),
                    Arc::new(
                        ListBuilder::new(UInt64Builder::new())
                            .with_field(Arc::new(Field::new("item", DataType::UInt64, false)))
                            .finish(),
                    ),
                    Arc::new(
                        ListBuilder::new(TimestampMillisecondBuilder::new())
                            .with_field(Arc::new(Field::new(
                                "item",
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            )))
                            .finish(),
                    ),
                ],
            )
            .map_err(|e| {
                ClickHouseError::Connection(format!("Empty RecordBatch creation failed: {e}"))
            });
        }

        let len = items.len();

        let mut signature_builder = BinaryBuilder::with_capacity(len, len * 64);
        let mut slot_builder = UInt64Builder::with_capacity(len);
        let mut tx_index_builder = UInt64Builder::with_capacity(len);
        let mut is_vote_builder = BooleanBuilder::with_capacity(len);
        let mut message_type_builder = UInt8Builder::with_capacity(len);
        let mut success_builder = BooleanBuilder::with_capacity(len);
        let mut fee_builder = UInt64Builder::with_capacity(len);
        let mut updated_at_builder = TimestampMillisecondBuilder::with_capacity(len);

        let mut balance_changes_account_builder = ListBuilder::new(BinaryBuilder::new())
            .with_field(Arc::new(Field::new("item", DataType::Binary, false)));
        let mut balance_changes_account_index_builder = ListBuilder::new(UInt8Builder::new())
            .with_field(Arc::new(Field::new("item", DataType::UInt8, false)));
        let mut balance_changes_pre_balance_builder = ListBuilder::new(UInt64Builder::new())
            .with_field(Arc::new(Field::new("item", DataType::UInt64, false)));
        let mut balance_changes_post_balance_builder = ListBuilder::new(UInt64Builder::new())
            .with_field(Arc::new(Field::new("item", DataType::UInt64, false)));
        let mut balance_changes_updated_at_builder =
            ListBuilder::new(TimestampMillisecondBuilder::new()).with_field(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )));

        for item in items {
            signature_builder.append_value(&item.signature);
            slot_builder.append_value(item.slot);
            tx_index_builder.append_value(item.tx_index as u64);
            is_vote_builder.append_value(item.is_vote);
            message_type_builder.append_value(item.message_type);
            success_builder.append_value(item.success);
            fee_builder.append_value(item.fee);
            updated_at_builder.append_value(item.updated_at.timestamp_millis());

            for bc in &item.balance_changes {
                balance_changes_account_builder
                    .values()
                    .append_value(&bc.account);
                balance_changes_account_index_builder
                    .values()
                    .append_value(bc.account_index);
                balance_changes_pre_balance_builder
                    .values()
                    .append_value(bc.pre_balance);
                balance_changes_post_balance_builder
                    .values()
                    .append_value(bc.post_balance);
                balance_changes_updated_at_builder
                    .values()
                    .append_value(bc.updated_at.timestamp_millis());
            }
            balance_changes_account_builder.append(true);
            balance_changes_account_index_builder.append(true);
            balance_changes_pre_balance_builder.append(true);
            balance_changes_post_balance_builder.append(true);
            balance_changes_updated_at_builder.append(true);
        }

        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(signature_builder.finish()),
                Arc::new(slot_builder.finish()),
                Arc::new(tx_index_builder.finish()),
                Arc::new(is_vote_builder.finish()),
                Arc::new(message_type_builder.finish()),
                Arc::new(success_builder.finish()),
                Arc::new(fee_builder.finish()),
                Arc::new(updated_at_builder.finish()),
                Arc::new(balance_changes_account_builder.finish()),
                Arc::new(balance_changes_account_index_builder.finish()),
                Arc::new(balance_changes_pre_balance_builder.finish()),
                Arc::new(balance_changes_post_balance_builder.finish()),
                Arc::new(balance_changes_updated_at_builder.finish()),
            ],
        )
        .map_err(|e| ClickHouseError::Connection(format!("RecordBatch creation failed: {e}")))
    }

    #[inline]
    fn memory_size(&self) -> usize {
        mem::size_of::<Self>() + (self.balance_changes.len() * mem::size_of::<BalanceChange>())
            - mem::size_of::<Vec<BalanceChange>>()
    }

    fn schema() -> SchemaRef {
        static SCHEMA: std::sync::OnceLock<SchemaRef> = std::sync::OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                Arc::new(Schema::new(vec![
                    Field::new("signature", DataType::Binary, false),
                    Field::new("slot", DataType::UInt64, false),
                    Field::new("tx_index", DataType::UInt64, false),
                    Field::new("is_vote", DataType::Boolean, false),
                    Field::new("message_type", DataType::UInt8, false),
                    Field::new("success", DataType::Boolean, false),
                    Field::new("fee", DataType::UInt64, false),
                    Field::new(
                        "updated_at",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new(
                        "balance_changes.account",
                        DataType::List(Arc::new(Field::new("item", DataType::Binary, false))),
                        false,
                    ),
                    Field::new(
                        "balance_changes.account_index",
                        DataType::List(Arc::new(Field::new("item", DataType::UInt8, false))),
                        false,
                    ),
                    Field::new(
                        "balance_changes.pre_balance",
                        DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                        false,
                    ),
                    Field::new(
                        "balance_changes.post_balance",
                        DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                        false,
                    ),
                    Field::new(
                        "balance_changes.updated_at",
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::Timestamp(TimeUnit::Millisecond, None),
                            false,
                        ))),
                        false,
                    ),
                ]))
            })
            .clone()
    }
}

pub struct TransactionWorker {
    id: usize,
    pool: Arc<ConnectionPool>,
    buffer: BatchBuffer<Transaction>,
    metrics: Metrics,
    last_health_check: Instant,
    total_balance_changes: Arc<AtomicU64>,
}

impl TransactionWorker {
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
            metrics: Metrics::new(id, "transaction"),
            last_health_check: Instant::now(),
            total_balance_changes: Arc::new(AtomicU64::new(0)),
        })
    }

    #[inline]
    async fn process_transaction(
        &mut self,
        transaction: Transaction,
    ) -> Result<(), ClickHouseError> {
        self.metrics.total_processed.fetch_add(1, Ordering::Relaxed);
        self.total_balance_changes
            .fetch_add(transaction.balance_changes.len() as u64, Ordering::Relaxed);

        if let Err(transaction) = self.buffer.push(transaction) {
            self.metrics
                .memory_pressure_events
                .fetch_add(1, Ordering::Relaxed);
            self.flush().await?;

            if let Err(transaction) = self.buffer.push(transaction) {
                self.buffer.push_oversized(transaction)?;
            }
        }

        let current_memory = self.buffer.memory_usage();
        let max_memory = self.metrics.max_memory_used.load(Ordering::Relaxed);
        if current_memory > max_memory {
            self.metrics
                .max_memory_used
                .store(current_memory, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ClickHouseError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let flush_start = Instant::now();
        let record_batch = self
            .buffer
            .flush_to_arrow()?
            .ok_or_else(|| ClickHouseError::BufferOverflow("flush returned empty batch".into()))?;

        let transaction_rows = record_batch.num_rows();
        if transaction_rows == 0 {
            return Ok(());
        }

        let conn = timeout(CONNECTION_ACQUIRE_TIMEOUT, self.pool.get_connection())
            .await
            .map_err(|_| {
                self.metrics.record_connection_timeout();
                ClickHouseError::Timeout("connection acquire timeout".into())
            })?
            .map_err(|e| {
                self.metrics.record_error();
                e
            })?;

        let insert_query = format!("INSERT INTO {} FORMAT ArrowStream", TBL_TRANSACTION);
        let mut insert_stream = conn
            .insert(insert_query, record_batch, None)
            .await
            .map_err(|e| {
                self.metrics.record_error();
                ClickHouseError::Connection(format!("insert stream creation failed: {e}"))
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
            .fetch_add(transaction_rows as u64, Ordering::Relaxed);
        self.metrics.flush_count.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_flush_duration
            .store(flush_duration.as_micros() as u64, Ordering::Relaxed);
        self.metrics.reset_error_count();

        log::debug!(
            "transaction worker {} flushed {} transactions ({} balance changes) in {:?}",
            self.id,
            transaction_rows,
            self.total_balance_changes.swap(0, Ordering::Relaxed),
            flush_duration
        );

        Ok(())
    }

    #[inline]
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

    pub async fn run(
        &mut self,
        receiver: Receiver<Transaction>,
        exit_flag: Arc<AtomicBool>,
        startup_done_flag: Arc<AtomicBool>,
        startup_done_count: Arc<AtomicUsize>,
    ) -> Result<(), ClickHouseError> {
        let mut is_startup_done = false;
        let mut flush_ticker = tokio::time::interval(FLUSH_TICK_INTERVAL);
        let mut stats_ticker = tokio::time::interval(STATS_INTERVAL);
        let mut health_ticker = tokio::time::interval(HEALTH_CHECK_INTERVAL);

        log::info!("transaction worker {} started", self.id);

        loop {
            select! {
                transaction_result = receiver.as_async().recv() => {
                    match transaction_result {
                        Ok(transaction) => {
                            if let Err(e) = self.process_transaction(transaction).await {
                                let consecutive_errors = self.metrics.consecutive_error_count();
                                if consecutive_errors > MAX_CONSECUTIVE_ERRORS {
                                    log::error!(
                                        "transaction worker {} exceeded error threshold ({} consecutive), shutting down: {e}",
                                        self.id, consecutive_errors
                                    );
                                    return Err(e);
                                }
                                log::warn!("transaction worker {} error (consecutive: {}): {}", self.id, consecutive_errors, e);
                            }
                        },
                        Err(_) => {
                            log::debug!("transaction worker {} channel closed", self.id);
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
                                log::error!(
                                    "transaction worker {} exceeded flush error threshold ({}), shutting down: {}",
                                    self.id, consecutive_errors, e
                                );
                                return Err(e);
                            }
                            log::warn!("transaction worker {} flush error (reason: {:?}): {}", self.id, reason, e);
                        }
                    }
                },

                _ = stats_ticker.tick() => {
                    self.metrics.log_stats();
                },

                _ = health_ticker.tick() => {
                    if let Err(e) = self.health_check().await {
                        log::debug!("transaction worker {} health check failed (non-fatal): {}", self.id, e);
                    }
                },
            }

            if exit_flag.load(Ordering::Relaxed) {
                log::info!("transaction worker {} received exit signal", self.id);
                break;
            }

            if !is_startup_done && startup_done_flag.load(Ordering::Relaxed) {
                if let Err(e) = self.flush().await {
                    log::warn!("transaction worker {} startup flush failed: {}", self.id, e);
                }
                is_startup_done = true;
                startup_done_count.fetch_add(1, Ordering::Relaxed);
                log::info!("transaction worker {} startup complete", self.id);
            }
        }

        self.flush().await?;
        log::info!("transaction worker {} shutdown", self.id);
        Ok(())
    }
}

pub struct TransactionManager {
    pool: Arc<ConnectionPool>,
    sender: Sender<Transaction>,
    workers: Vec<JoinHandle<Result<(), ClickHouseError>>>,
    exit_flag: Arc<AtomicBool>,
    worker_count: usize,
    channel_capacity: usize,
}

impl std::fmt::Debug for TransactionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionManager")
            .field("worker_count", &self.worker_count)
            .field("channel_capacity", &self.channel_capacity)
            .finish_non_exhaustive()
    }
}

impl TransactionManager {
    pub async fn new(
        conn_config: &ConnectionCfg,
        batch_cfg: &BatchCfg,
        arrow_cfg: &ArrowCfg,
        channel_size: usize,
    ) -> Result<Self, ClickHouseError> {
        let pool = ConnectionPool::new(&conn_config).await?;
        let worker_count = batch_cfg.transactions.workers.max(1) as usize;
        log::info!(
            "initializing TransactionManager with {} workers",
            worker_count
        );

        let (main_sender, main_receiver) = bounded::<Transaction>(channel_size);
        let exit_flag = Arc::new(AtomicBool::new(false));
        let startup_done_flag = Arc::new(AtomicBool::new(false));
        let startup_done_count = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(worker_count + 1);
        let mut worker_senders = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            let (worker_sender, worker_receiver) =
                bounded(batch_cfg.transactions.max_rows as usize);
            worker_senders.push(worker_sender);

            let mut worker = TransactionWorker::new(
                i,
                pool.clone(),
                arrow_cfg,
                batch_cfg.transactions.max_rows,
                batch_cfg.transactions.max_bytes,
                batch_cfg.transactions.flush_ms,
            )?;

            let exit_flag_clone = exit_flag.clone();
            let startup_done_flag_clone = startup_done_flag.clone();
            let startup_done_count_clone = startup_done_count.clone();

            let handle = tokio::spawn(async move {
                worker
                    .run(
                        worker_receiver,
                        exit_flag_clone,
                        startup_done_flag_clone,
                        startup_done_count_clone,
                    )
                    .await
            });
            workers.push(handle);
        }

        let router_exit_flag = exit_flag.clone();
        let router_handle = tokio::spawn(async move {
            let mut round_robin = 0usize;
            while let Ok(transaction) = main_receiver.as_async().recv().await {
                if router_exit_flag.load(Ordering::Relaxed) {
                    break;
                }
                let target_worker = round_robin % worker_senders.len();
                if worker_senders[target_worker]
                    .as_async()
                    .send(transaction)
                    .await
                    .is_err()
                {
                    log::error!(
                        "failed to route transaction to worker {} - channel closed",
                        target_worker
                    );
                    break;
                }
                round_robin = round_robin.wrapping_add(1);
            }
            drop(worker_senders);
            log::debug!("transaction router task completed");
            Ok(())
        });
        workers.push(router_handle);

        let start_time = Instant::now();
        startup_done_flag.store(true, Ordering::Relaxed);

        while startup_done_count.load(Ordering::Relaxed) < worker_count
            && start_time.elapsed() < STARTUP_TIMEOUT
        {
            sleep(Duration::from_millis(50)).await;
        }

        let completed_workers = startup_done_count.load(Ordering::Relaxed);
        if completed_workers < worker_count {
            return Err(ClickHouseError::Timeout(format!(
                "startup timeout: {}/{} workers completed",
                completed_workers, worker_count
            )));
        }

        log::info!(
            "TransactionManager initialization complete with {} workers",
            worker_count
        );

        Ok(Self {
            pool,
            sender: main_sender,
            workers,
            exit_flag,
            worker_count,
            channel_capacity: channel_size,
        })
    }

    #[inline]
    pub fn try_send_transaction(&self, transaction: Transaction) -> Result<(), ClickHouseError> {
        match self.sender.try_send(transaction) {
            Ok(true) => Ok(()),
            Ok(false) => Err(ClickHouseError::BufferOverflow(
                "transaction channel full".into(),
            )),
            Err(_) => Err(ClickHouseError::ChannelClosed(
                "transaction channel closed".into(),
            )),
        }
    }

    pub async fn shutdown(self) -> Result<(), ClickHouseError> {
        log::info!(
            "shutting down TransactionManager with {} workers",
            self.worker_count
        );
        self.exit_flag.store(true, Ordering::Relaxed);
        drop(self.sender);

        let mut shutdown_errors = Vec::new();
        for (i, handle) in self.workers.into_iter().enumerate() {
            match timeout(WORKER_SHUTDOWN_TIMEOUT, handle).await {
                Ok(Ok(Ok(()))) => {
                    log::debug!("worker {} shutdown cleanly", i);
                }
                Ok(Ok(Err(e))) => {
                    log::error!("worker {} shutdown with error: {}", i, e);
                    shutdown_errors.push(e);
                }
                Ok(Err(e)) => {
                    log::error!("worker {} panicked: {}", i, e);
                    shutdown_errors
                        .push(ClickHouseError::Connection(format!("worker panic: {}", e)));
                }
                Err(_) => {
                    log::error!("worker {} shutdown timeout", i);
                    shutdown_errors.push(ClickHouseError::Timeout(format!(
                        "worker {} shutdown timeout",
                        i
                    )));
                }
            }
        }

        if shutdown_errors.is_empty() {
            log::info!("TransactionManager shutdown complete");
            Ok(())
        } else {
            let error_msg = format!("shutdown completed with {} errors", shutdown_errors.len());
            log::error!("{}", error_msg);
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
