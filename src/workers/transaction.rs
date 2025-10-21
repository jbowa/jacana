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
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    tokio::{task::JoinHandle, time::timeout},
};

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

#[derive(Debug, Clone)]
pub struct BalanceChange {
    pub signature: [u8; 64],
    pub account: [u8; 32],
    pub account_index: u8,
    pub pre_balance: u64,
    pub post_balance: u64,
    pub updated_at: DateTime<Utc>,
}

impl BatchConvertible for Transaction {
    fn to_record_batch(items: &[Self]) -> Result<RecordBatch, ClickHouseError> {
        if items.is_empty() {
            return Err(ClickHouseError::BufferOverflow(
                "cannot create RecordBatch from empty items".into(),
            ));
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

        let total_balance_changes: usize = items.iter().map(|t| t.balance_changes.len()).sum();
        let mut balance_signatures_builder = ListBuilder::new(BinaryBuilder::with_capacity(
            total_balance_changes,
            total_balance_changes * 64,
        ));
        let mut balance_accounts_builder = ListBuilder::new(BinaryBuilder::with_capacity(
            total_balance_changes,
            total_balance_changes * 32,
        ));
        let mut balance_account_indices_builder =
            ListBuilder::new(UInt8Builder::with_capacity(total_balance_changes));
        let mut balance_pre_balances_builder =
            ListBuilder::new(UInt64Builder::with_capacity(total_balance_changes));
        let mut balance_post_balances_builder =
            ListBuilder::new(UInt64Builder::with_capacity(total_balance_changes));

        for item in items {
            signature_builder.append_value(&item.signature);
            slot_builder.append_value(item.slot);
            tx_index_builder.append_value(item.tx_index as u64);
            is_vote_builder.append_value(item.is_vote);
            message_type_builder.append_value(item.message_type);
            success_builder.append_value(item.success);
            fee_builder.append_value(item.fee);
            updated_at_builder.append_value(item.updated_at.timestamp_millis());

            // Process balance changes for this transaction.
            let balance_sig_values = balance_signatures_builder.values();
            for bc in &item.balance_changes {
                balance_sig_values.append_value(&bc.signature);
            }
            balance_signatures_builder.append(true);

            let balance_acc_values = balance_accounts_builder.values();
            for bc in &item.balance_changes {
                balance_acc_values.append_value(&bc.account);
            }
            balance_accounts_builder.append(true);

            let balance_idx_values = balance_account_indices_builder.values();
            for bc in &item.balance_changes {
                balance_idx_values.append_value(bc.account_index);
            }
            balance_account_indices_builder.append(true);

            let balance_pre_values = balance_pre_balances_builder.values();
            for bc in &item.balance_changes {
                balance_pre_values.append_value(bc.pre_balance);
            }
            balance_pre_balances_builder.append(true);

            let balance_post_values = balance_post_balances_builder.values();
            for bc in &item.balance_changes {
                balance_post_values.append_value(bc.post_balance);
            }
            balance_post_balances_builder.append(true);
        }

        let schema = Self::schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(signature_builder.finish()),
                Arc::new(slot_builder.finish()),
                Arc::new(tx_index_builder.finish()),
                Arc::new(is_vote_builder.finish()),
                Arc::new(message_type_builder.finish()),
                Arc::new(success_builder.finish()),
                Arc::new(fee_builder.finish()),
                Arc::new(updated_at_builder.finish()),
                Arc::new(balance_signatures_builder.finish()),
                Arc::new(balance_accounts_builder.finish()),
                Arc::new(balance_account_indices_builder.finish()),
                Arc::new(balance_pre_balances_builder.finish()),
                Arc::new(balance_post_balances_builder.finish()),
            ],
        )
        .map_err(|e| ClickHouseError::Connection(format!("arrow RecordBatch creation failed: {e}")))
    }

    fn memory_size(&self) -> usize {
        mem::size_of::<[u8; 64]>()
            + mem::size_of::<u64>() * 3
            + mem::size_of::<bool>() * 2
            + mem::size_of::<u8>()
            + mem::size_of::<DateTime<Utc>>()
            + mem::size_of::<Vec<BalanceChange>>()
            + self.balance_changes.len()
                * (mem::size_of::<[u8; 64]>()
                    + mem::size_of::<[u8; 32]>()
                    + mem::size_of::<u8>()
                    + mem::size_of::<u64>() * 2
                    + mem::size_of::<DateTime<Utc>>())
    }

    fn schema() -> SchemaRef {
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
                "balance_signatures",
                DataType::List(Arc::new(Field::new("item", DataType::Binary, false))),
                false,
            ),
            Field::new(
                "balance_accounts",
                DataType::List(Arc::new(Field::new("item", DataType::Binary, false))),
                false,
            ),
            Field::new(
                "balance_account_indices",
                DataType::List(Arc::new(Field::new("item", DataType::UInt8, false))),
                false,
            ),
            Field::new(
                "balance_pre_balances",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                false,
            ),
            Field::new(
                "balance_post_balances",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                false,
            ),
        ]))
    }
}

pub struct TransactionWorker {
    id: usize,
    pool: Arc<ConnectionPool>,
    buffer: BatchBuffer<Transaction>,
    metrics: Metrics,
    last_health_check: Instant,
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
            metrics: Metrics::new(id),
            last_health_check: Instant::now(),
        })
    }

    async fn process_transaction(
        &mut self,
        transaction: Transaction,
    ) -> Result<(), ClickHouseError> {
        self.metrics.total_processed.fetch_add(1, Ordering::Relaxed);

        if let Err(transaction) = self.buffer.push(transaction) {
            self.metrics
                .memory_pressure_events
                .fetch_add(1, Ordering::Relaxed);

            if let Err(e) = self.flush().await {
                self.metrics.record_error();
                return Err(e);
            }

            if let Err(transaction) = self.buffer.push(transaction) {
                self.buffer.push_oversized(transaction).map_err(|e| {
                    self.metrics.record_error();
                    e
                })?;
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

        let insert_query = format!("INSERT INTO {} FORMAT ArrowStream", TBL_TRANSACTION);
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

        log::debug!(
            "transaction worker {} flushed {} transaction records via Arrow in {:?}",
            self.id,
            rows,
            flush_duration
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
            tokio::select! {
                transaction_result = receiver.as_async().recv() => {
                    match transaction_result {
                        Ok(transaction) => {
                            if let Err(e) = self.process_transaction(transaction).await {
                                let consecutive_errors = self.metrics.consecutive_error_count();
                                if consecutive_errors > MAX_CONSECUTIVE_ERRORS {
                                    log::error!(
                                        "transaction worker {} has {} consecutive errors, shutting down: {}",
                                        self.id, consecutive_errors, e
                                    );
                                    return Err(e);
                                }
                                log::warn!("transaction worker {} error (consecutive: {}): {}", self.id, consecutive_errors, e);
                            }
                        },
                        Err(_) => {
                            log::debug!("transaction worker {} Kanal channel closed", self.id);
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
                                    "transaction worker {} has {} errors. Exceeded threshold, shutting down: {}",
                                    self.id, consecutive_errors, e
                                );
                                return Err(e);
                            }
                            log::warn!("transaction worker {} flush error: {} (reason: {:?})", self.id, e, reason);
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
            let mut round_robin = 0;
            while let Ok(transaction) = main_receiver.as_async().recv().await {
                if router_exit_flag.load(Ordering::Relaxed) {
                    break;
                }
                let target_worker = round_robin % worker_senders.len();
                match worker_senders[target_worker]
                    .as_async()
                    .send(transaction)
                    .await
                {
                    Ok(_) => {
                        round_robin = round_robin.wrapping_add(1);
                    }
                    Err(_) => {
                        log::error!(
                            "failed to route transaction to worker {} - channel closed",
                            target_worker
                        );
                        break;
                    }
                }
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
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let completed_workers = startup_done_count.load(Ordering::Relaxed);
        if completed_workers < worker_count {
            return Err(ClickHouseError::Timeout(format!(
                "timeout waiting for workers to start: {}/{} completed",
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
            info!("TransactionManager shutdown complete");
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
