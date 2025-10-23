use {
    crate::{
        clickhouse::ClickHouseError,
        config::{ArrowCfg, BatchCfg, ChannelMaxBufferCfg, ConnectionCfg},
        workers::{
            account::AccountManager, block::BlockManager, slot::SlotManager,
            transaction::TransactionManager,
        },
    },
    async_trait::async_trait,
};

pub mod account;
pub mod block;
pub mod consts;
pub mod slot;
pub mod transaction;

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
