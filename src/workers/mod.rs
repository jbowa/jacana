use {
    crate::{
        clickhouse::{pool::ConnectionPool, ClickHouseError},
        config::{ArrowCfg, BatchCfg, BatchPolicy, ChannelMaxBufferCfg, ConnectionCfg},
        workers::{
            account::AccountManager, block::BlockManager, slot::SlotManager,
            transaction::TransactionManager,
        },
    },
    async_trait::async_trait,
    kanal::{bounded, Receiver, Sender},
    log::{debug, error, info},
    std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        time::Instant,
    },
    tokio::{task::JoinHandle, time::sleep},
};

pub mod account;
pub mod block;
pub mod consts;
pub mod slot;
pub mod transaction;

// Trait for workers that can be spawned in a managed pool.
// Provides factory method for creation and async run method for execution.
#[async_trait]
pub trait ManagedWorker: Sized + Send + 'static {
    type Item: Send + Sync + Clone + 'static;

    fn entity_name() -> &'static str;

    fn create(
        id: usize,
        pool: Arc<ConnectionPool>,
        arrow_cfg: &ArrowCfg,
        max_rows: u64,
        max_bytes: u64,
        flush_ms: u64,
    ) -> Result<Self, ClickHouseError>;

    async fn run(
        self,
        receiver: Receiver<Self::Item>,
        exit_flag: Arc<AtomicBool>,
        startup_done_flag: Arc<AtomicBool>,
        startup_done_count: Arc<AtomicUsize>,
    ) -> Result<(), ClickHouseError>;
}

pub struct ManagerComponents<T: Send + Sync + Clone + 'static> {
    pub pool: Arc<ConnectionPool>,
    pub sender: Sender<T>,
    pub workers: Vec<JoinHandle<Result<(), ClickHouseError>>>,
    pub exit_flag: Arc<AtomicBool>,
    pub worker_count: usize,
    pub channel_capacity: usize,
}

pub async fn build_manager<W>(
    conn_config: &ConnectionCfg,
    batch_policy: &BatchPolicy,
    arrow_cfg: &ArrowCfg,
    channel_size: usize,
) -> Result<ManagerComponents<W::Item>, ClickHouseError>
where
    W: ManagedWorker + Send,
    W::Item: Send,
{
    use consts::STARTUP_TIMEOUT;

    let pool = ConnectionPool::new(conn_config).await?;
    let worker_count = batch_policy.workers.max(1) as usize;
    let entity_name = W::entity_name();

    info!(
        "initializing {}Manager with {} workers",
        entity_name, worker_count
    );

    let (main_sender, main_receiver) = bounded::<W::Item>(channel_size);
    let exit_flag = Arc::new(AtomicBool::new(false));
    let startup_done_flag = Arc::new(AtomicBool::new(false));
    let startup_done_count = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::with_capacity(worker_count + 1);
    let mut worker_senders = Vec::with_capacity(worker_count);

    // Spawn worker tasks.
    for i in 0..worker_count {
        let (worker_sender, worker_receiver) = bounded(batch_policy.max_rows as usize);
        worker_senders.push(worker_sender);

        let worker = W::create(
            i,
            pool.clone(),
            arrow_cfg,
            batch_policy.max_rows,
            batch_policy.max_bytes,
            batch_policy.flush_ms,
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

    // Router task with round-robin distribution.
    let router_exit_flag = exit_flag.clone();
    let router_handle = tokio::spawn(async move {
        let mut round_robin = 0;
        while let Ok(item) = main_receiver.as_async().recv().await {
            if router_exit_flag.load(Ordering::Relaxed) {
                break;
            }
            let target_worker = round_robin % worker_senders.len();
            match worker_senders[target_worker].as_async().send(item).await {
                Ok(_) => {
                    round_robin = round_robin.wrapping_add(1);
                }
                Err(_) => {
                    error!(
                        "failed to route {} to worker {} - channel closed",
                        entity_name, target_worker
                    );
                    break;
                }
            }
        }
        drop(worker_senders);
        debug!("{} router task completed", entity_name);
        Ok(())
    });
    workers.push(router_handle);

    // Wait for worker startup coordination.
    let start_time = Instant::now();
    startup_done_flag.store(true, Ordering::Relaxed);
    while startup_done_count.load(Ordering::Relaxed) < worker_count
        && start_time.elapsed() < STARTUP_TIMEOUT
    {
        sleep(std::time::Duration::from_millis(100)).await;
    }

    let completed_workers = startup_done_count.load(Ordering::Relaxed);
    if completed_workers < worker_count {
        return Err(ClickHouseError::Timeout(format!(
            "timeout waiting for {} workers to start: {}/{} completed",
            entity_name, completed_workers, worker_count
        )));
    }

    info!(
        "{}Manager started successfully: {}/{} workers ready",
        entity_name, completed_workers, worker_count
    );

    Ok(ManagerComponents {
        pool,
        sender: main_sender,
        workers,
        exit_flag,
        worker_count,
        channel_capacity: channel_size,
    })
}

#[async_trait]
pub trait Worker: Send + Sync + 'static {
    type Item: Send + Sync + Clone + 'static;

    fn name(&self) -> &'static str;
    fn try_send(&self, item: Self::Item) -> Result<(), ClickHouseError>;
    async fn shutdown(self) -> Result<(), ClickHouseError>;
}

#[derive(Debug)]
pub struct WorkerManager {
    pub slots: SlotManager,
    pub blocks: BlockManager,
    pub accounts: AccountManager,
    pub transactions: TransactionManager,
}

impl WorkerManager {
    pub async fn new(
        conn_cfg: &ConnectionCfg,
        batch_cfg: &BatchCfg,
        arrow_cfg: &ArrowCfg,
        channel_cfg: &ChannelMaxBufferCfg,
    ) -> Result<Self, ClickHouseError> {
        let slots = SlotManager::new(conn_cfg, batch_cfg, arrow_cfg, channel_cfg.slots).await?;
        let blocks = BlockManager::new(conn_cfg, batch_cfg, arrow_cfg, channel_cfg.blocks).await?;
        let accounts =
            AccountManager::new(conn_cfg, batch_cfg, arrow_cfg, channel_cfg.accounts).await?;
        let transactions =
            TransactionManager::new(conn_cfg, batch_cfg, arrow_cfg, channel_cfg.transactions)
                .await?;
        Ok(Self {
            slots,
            blocks,
            accounts,
            transactions,
        })
    }

    pub fn slots(&self) -> &SlotManager {
        &self.slots
    }

    pub fn blocks(&self) -> &BlockManager {
        &self.blocks
    }

    pub fn accounts(&self) -> &AccountManager {
        &self.accounts
    }

    pub fn transactions(&self) -> &TransactionManager {
        &self.transactions
    }

    pub async fn shutdown(self) -> Result<(), ClickHouseError> {
        let slot_result = self.slots.shutdown().await;
        let block_result = self.blocks.shutdown().await;
        let account_result = self.accounts.shutdown().await;
        let transaction_result = self.transactions.shutdown().await;

        let mut errors = Vec::new();

        if let Err(e) = slot_result {
            log::error!("slot manager shutdown error: {}", e);
            errors.push(e);
        }
        if let Err(e) = block_result {
            log::error!("block manager shutdown error: {}", e);
            errors.push(e);
        }
        if let Err(e) = account_result {
            log::error!("account manager shutdown error: {}", e);
            errors.push(e);
        }
        if let Err(e) = transaction_result {
            log::error!("transaction manager shutdown error: {}", e);
            errors.push(e);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ClickHouseError::Connection(format!(
                "shutdown completed with {} errors",
                errors.len()
            )))
        }
    }
}
