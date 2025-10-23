use {
    crate::{
        clickhouse::{
            buffer::{BatchBuffer, BatchConvertible},
            pool::{ConnectionPool, TBL_ACCOUNT},
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
    clickhouse_arrow::{
        arrow::arrow::{
            array::{
                BinaryBuilder, BooleanBuilder, RecordBatch, TimestampMillisecondBuilder,
                UInt64Builder,
            },
            datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        },
        tracing::debug,
    },
    futures_util::StreamExt,
    kanal::{bounded, Receiver, Sender},
    log::{error, info, warn},
    std::{
        mem,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
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
pub struct Account {
    pub pubkey: [u8; 32],
    pub owner: [u8; 32],
    pub slot: u64,
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub updated_at: DateTime<Utc>,
}

impl BatchConvertible for Account {
    fn to_record_batch(items: &[Self]) -> Result<RecordBatch, ClickHouseError> {
        if items.is_empty() {
            return Err(ClickHouseError::BufferOverflow(
                "cannot create RecordBatch from empty items".into(),
            ));
        }

        let len = items.len();
        let mut pubkey_builder = BinaryBuilder::with_capacity(len, len * 32);
        let mut owner_builder = BinaryBuilder::with_capacity(len, len * 32);
        let mut slot_builder = UInt64Builder::with_capacity(len);
        let mut lamports_builder = UInt64Builder::with_capacity(len);
        let mut executable_builder = BooleanBuilder::with_capacity(len);
        let mut rent_epoch_builder = UInt64Builder::with_capacity(len);
        let mut write_version_builder = UInt64Builder::with_capacity(len);
        let mut updated_at_builder = TimestampMillisecondBuilder::with_capacity(len);

        let total_data_size: usize = items.iter().map(|a| a.data.len()).sum();
        let mut data_builder = BinaryBuilder::with_capacity(len, total_data_size);

        for item in items {
            pubkey_builder.append_value(&item.pubkey);
            owner_builder.append_value(&item.owner);
            slot_builder.append_value(item.slot);
            lamports_builder.append_value(item.lamports);
            executable_builder.append_value(item.executable);
            rent_epoch_builder.append_value(item.rent_epoch);
            data_builder.append_value(&item.data);
            write_version_builder.append_value(item.write_version);
            updated_at_builder.append_value(item.updated_at.timestamp_millis());
        }

        let schema = Self::schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(pubkey_builder.finish()),
                Arc::new(owner_builder.finish()),
                Arc::new(slot_builder.finish()),
                Arc::new(lamports_builder.finish()),
                Arc::new(executable_builder.finish()),
                Arc::new(rent_epoch_builder.finish()),
                Arc::new(data_builder.finish()),
                Arc::new(write_version_builder.finish()),
                Arc::new(updated_at_builder.finish()),
            ],
        )
        .map_err(|e| ClickHouseError::Connection(format!("arrow RecordBatch creation failed: {e}")))
    }

    fn memory_size(&self) -> usize {
        mem::size_of::<[u8; 32]>() * 2
            + mem::size_of::<u64>() * 4
            + mem::size_of::<bool>()
            + mem::size_of::<DateTime<Utc>>()
            + mem::size_of::<Vec<u8>>()
            + self.data.len()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pubkey", DataType::Binary, false),
            Field::new("owner", DataType::Binary, false),
            Field::new("slot", DataType::UInt64, false),
            Field::new("lamports", DataType::UInt64, false),
            Field::new("executable", DataType::Boolean, false),
            Field::new("rent_epoch", DataType::UInt64, false),
            Field::new("data", DataType::Binary, false),
            Field::new("write_version", DataType::UInt64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]))
    }
}

pub struct AccountWorker {
    id: usize,
    pool: Arc<ConnectionPool>,
    buffer: BatchBuffer<Account>,
    metrics: Metrics,
    last_health_check: Instant,
}

impl AccountWorker {
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
            metrics: Metrics::new(id, "account"),
            last_health_check: Instant::now(),
        })
    }

    async fn process_account(&mut self, account: Account) -> Result<(), ClickHouseError> {
        self.metrics.total_processed.fetch_add(1, Ordering::Relaxed);

        if let Err(account) = self.buffer.push(account) {
            self.metrics
                .memory_pressure_events
                .fetch_add(1, Ordering::Relaxed);

            if let Err(e) = self.flush().await {
                self.metrics.record_error();
                return Err(e);
            }

            if let Err(account) = self.buffer.push(account) {
                self.buffer.push_oversized(account).map_err(|e| {
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
        let insert_query = format!("INSERT INTO {} FORMAT ArrowStream", TBL_ACCOUNT);
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
            "worker {} flushed {} account records via Arrow in {:?}",
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

    pub async fn run(
        &mut self,
        receiver: Receiver<Account>,
        exit_flag: Arc<AtomicBool>,
        startup_done_flag: Arc<AtomicBool>,
        startup_done_count: Arc<AtomicUsize>,
    ) -> Result<(), ClickHouseError> {
        let mut is_startup_done = false;
        let mut flush_ticker = tokio::time::interval(FLUSH_TICK_INTERVAL);
        let mut stats_ticker = tokio::time::interval(STATS_INTERVAL);
        let mut health_ticker = tokio::time::interval(HEALTH_CHECK_INTERVAL);
        info!("account worker {} started", self.id);
        loop {
            select! {
                account_result = receiver.as_async().recv() => {
                    match account_result {
                        Ok(account) => {
                            if let Err(e) = self.process_account(account).await {
                                let consecutive_errors = self.metrics.consecutive_error_count();
                                if consecutive_errors > MAX_CONSECUTIVE_ERRORS {
                                    error!(
                                        "account worker {} has {} consecutive errors, shutting down: {}",
                                        self.id, consecutive_errors, e
                                    );
                                    return Err(e);
                                }
                                warn!("account worker {} error (consecutive: {}): {}", self.id, consecutive_errors, e);
                            }
                        },
                        Err(_) => {
                            debug!("account worker {} Kanal channel closed", self.id);
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
                                    "account worker {} has {} errors. Exceeded threshold, shutting down: {}",
                                    self.id, consecutive_errors, e
                                );
                                return Err(e);
                            }
                            warn!("account worker {} flush error: {} (reason: {:?})", self.id, e, reason);
                        }
                    }
                },
                _ = stats_ticker.tick() => {
                    self.metrics.log_stats();
                },
                _ = health_ticker.tick() => {
                    if let Err(e) = self.health_check().await {
                        debug!("account worker {} health check failed (non-fatal): {}", self.id, e);
                    }
                },
            }
            if exit_flag.load(Ordering::Relaxed) {
                info!("account worker {} received exit signal", self.id);
                break;
            }
            if !is_startup_done && startup_done_flag.load(Ordering::Relaxed) {
                if let Err(e) = self.flush().await {
                    warn!("account worker {} startup flush failed: {}", self.id, e);
                }
                is_startup_done = true;
                startup_done_count.fetch_add(1, Ordering::Relaxed);
                info!("account worker {} startup complete", self.id);
            }
        }
        self.flush().await?;
        info!("account worker {} shutdown", self.id);
        Ok(())
    }
}

pub struct AccountManager {
    pool: Arc<ConnectionPool>,
    sender: Sender<Account>,
    workers: Vec<JoinHandle<Result<(), ClickHouseError>>>,
    exit_flag: Arc<AtomicBool>,
    worker_count: usize,
    channel_capacity: usize,
}

impl std::fmt::Debug for AccountManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountManager")
            .field("worker_count", &self.worker_count)
            .field("channel_capacity", &self.channel_capacity)
            .finish_non_exhaustive()
    }
}

impl AccountManager {
    pub async fn new(
        conn_config: &ConnectionCfg,
        batch_cfg: &BatchCfg,
        arrow_cfg: &ArrowCfg,
        channel_size: usize,
    ) -> Result<Self, ClickHouseError> {
        let pool = ConnectionPool::new(&conn_config).await?;
        let worker_count = batch_cfg.accounts.workers.max(1) as usize;
        info!("initializing AccountManager with {} workers", worker_count);
        let (main_sender, main_receiver) = bounded::<Account>(channel_size);
        let exit_flag = Arc::new(AtomicBool::new(false));
        let startup_done_flag = Arc::new(AtomicBool::new(false));
        let startup_done_count = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(worker_count + 1);
        let mut worker_senders = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let (worker_sender, worker_receiver) = bounded(batch_cfg.accounts.max_rows as usize);
            worker_senders.push(worker_sender);
            let mut worker = AccountWorker::new(
                i,
                pool.clone(),
                arrow_cfg,
                batch_cfg.accounts.max_rows,
                batch_cfg.accounts.max_bytes,
                batch_cfg.accounts.flush_ms,
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
            while let Ok(account) = main_receiver.as_async().recv().await {
                if router_exit_flag.load(Ordering::Relaxed) {
                    break;
                }
                let target_worker = round_robin % worker_senders.len();
                match worker_senders[target_worker].as_async().send(account).await {
                    Ok(_) => {
                        round_robin = round_robin.wrapping_add(1);
                    }
                    Err(_) => {
                        error!(
                            "failed to route account to worker {} - channel closed",
                            target_worker
                        );
                        break;
                    }
                }
            }
            drop(worker_senders);
            debug!("account router task completed");
            Ok(())
        });
        workers.push(router_handle);

        let start_time = Instant::now();
        startup_done_flag.store(true, Ordering::Relaxed);
        while startup_done_count.load(Ordering::Relaxed) < worker_count
            && start_time.elapsed() < STARTUP_TIMEOUT
        {
            sleep(Duration::from_millis(100)).await;
        }

        let completed_workers = startup_done_count.load(Ordering::Relaxed);
        if completed_workers < worker_count {
            return Err(ClickHouseError::Timeout(format!(
                "timeout waiting for workers to start: {}/{} completed",
                completed_workers, worker_count
            )));
        }
        info!(
            "AccountManager initialization complete with {} workers",
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

    pub fn try_send_account(&self, account: Account) -> Result<(), ClickHouseError> {
        match self.sender.try_send(account) {
            Ok(true) => Ok(()),
            Ok(false) => Err(ClickHouseError::BufferOverflow(
                "account channel full".into(),
            )),
            Err(_) => Err(ClickHouseError::ChannelClosed(
                "account channel closed".into(),
            )),
        }
    }

    pub async fn shutdown(self) -> Result<(), ClickHouseError> {
        info!(
            "shutting down AccountManager with {} workers",
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
            info!("AccountManager shutdown complete");
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
