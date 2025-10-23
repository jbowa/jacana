use {
    crate::{
        config::{ArrowCfg, BatchCfg, ChannelMaxBufferCfg, ConnectionCfg},
        workers::{
            account::Account, block::Block, slot::Slot, transaction::Transaction, WorkerManager,
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    clickhouse_arrow::bb8,
};

pub mod buffer;
pub mod pool;

#[derive(Debug, thiserror::Error)]
pub enum ClickHouseError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error(transparent)]
    Driver(#[from] clickhouse_arrow::Error),

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("channel closed: {0}")]
    ChannelClosed(String),

    #[error("buffer overflow: {0}")]
    BufferOverflow(String),

    #[error("schema validation failed: {0}")]
    SchemaValidation(String),
}

impl From<bb8::RunError<clickhouse_arrow::Error>> for ClickHouseError {
    fn from(e: bb8::RunError<clickhouse_arrow::Error>) -> Self {
        match e {
            bb8::RunError::TimedOut => {
                ClickHouseError::Timeout("timed out waiting for connection".into())
            }
            bb8::RunError::User(inner) => ClickHouseError::Driver(inner),
        }
    }
}

impl From<ClickHouseError> for GeyserPluginError {
    fn from(err: ClickHouseError) -> Self {
        GeyserPluginError::Custom(Box::new(err))
    }
}

#[derive(Debug)]
pub struct ClickHouseClient {
    worker_manager: WorkerManager,
}

impl ClickHouseClient {
    pub async fn new(
        conn_cfg: &ConnectionCfg,
        batch_cfg: &BatchCfg,
        arrow_cfg: &ArrowCfg,
        channel_cfg: &ChannelMaxBufferCfg,
    ) -> Result<Self, ClickHouseError> {
        let worker_manager =
            WorkerManager::new(conn_cfg, batch_cfg, arrow_cfg, channel_cfg).await?;
        Ok(Self { worker_manager })
    }

    #[inline]
    pub fn try_send_slot(&self, slot: Slot) -> Result<(), ClickHouseError> {
        self.worker_manager.slots().try_send_slot(slot)
    }

    #[inline]
    pub fn try_send_block(&self, block: Block) -> Result<(), ClickHouseError> {
        self.worker_manager.blocks().try_send_block(block)
    }

    #[inline]
    pub fn try_send_account(&self, account: Account) -> Result<(), ClickHouseError> {
        self.worker_manager.accounts().try_send_account(account)
    }

    pub async fn shutdown(self) -> Result<(), ClickHouseError> {
        self.worker_manager.shutdown().await
    }

    #[inline]
    pub fn channel_capacity(&self) -> usize {
        self.worker_manager.slots().channel_capacity()
    }

    #[inline]
    pub fn try_send_transaction(&self, transaction: Transaction) -> Result<(), ClickHouseError> {
        self.worker_manager
            .transactions()
            .try_send_transaction(transaction)
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.worker_manager.slots().worker_count()
    }
}
