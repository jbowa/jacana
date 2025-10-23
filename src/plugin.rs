use {
    crate::{
        clickhouse::{ClickHouseClient, ClickHouseError},
        config::Config,
        filters::{accounts::AccountsFilter, transactions::TransactionsFilter},
        workers::{
            account::Account,
            block::{build_block_v3, build_block_v4},
            slot::Slot,
            transaction::{BalanceChange, Transaction},
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
    },
    solana_message::VersionedMessage,
    tokio::runtime::Builder,
};

#[derive(Default)]
struct JacanaPlugin {
    runtime: Option<tokio::runtime::Runtime>,
    client: Option<ClickHouseClient>,
    accounts_filter: Option<AccountsFilter>,
    transactions_filter: Option<TransactionsFilter>,
    starting_slot: Option<u64>,
}

impl std::fmt::Debug for JacanaPlugin {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for JacanaPlugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        // Load config.
        let config = Config::load_from_file(config_file)?;

        // Setup logger
        solana_logger::setup_with_default(&config.log_level);
        log::info!("Loading plugin: {:?}", self.name());

        // Setup filters.
        self.accounts_filter = Some(AccountsFilter::from_cfg(&config.filters.accounts)?);
        self.transactions_filter =
            Some(TransactionsFilter::from_cfg(&config.filters.transactions)?);
        self.starting_slot = Some(config.starting_slot);

        // Ensure at least 1 runtime thread
        let runtime_threads = config.runtime_threads.max(1);
        let runtime = Builder::new_multi_thread()
            .worker_threads(runtime_threads)
            .thread_name("jacana-worker")
            .enable_all()
            .build()
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;

        let client = runtime
            .block_on(async {
                ClickHouseClient::new(
                    &config.connection,
                    &config.batch,
                    &config.arrow,
                    &config.channel_max_buffer,
                )
                .await
            })
            .map_err(|e| {
                log::error!("failed to initialize ClickHouse client: {}", e);
                GeyserPluginError::Custom(Box::new(e))
            })?;

        self.runtime = Some(runtime);
        self.client = Some(client);

        log::info!("plugin loaded successfully with coordinator");
        Ok(())
    }

    fn on_unload(&mut self) {
        log::info!("unloading plugin: {:?}", self.name());

        if let (Some(client), Some(runtime)) = (self.client.take(), self.runtime.as_ref()) {
            let _ = runtime.block_on(async { client.shutdown().await });
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(std::time::Duration::from_secs(30));
        }
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        let filter = match &self.accounts_filter {
            Some(f) => f,
            None => return Ok(()),
        };

        if is_startup {
            if let Some(starting_slot) = self.starting_slot {
                if slot < starting_slot {
                    return Ok(());
                }
            }
        }

        let account_info = match account {
            ReplicaAccountInfoVersions::V0_0_3(info) => info,
            _ => {
                log::warn!("unsupported account info version. only V0_0_3 is supported");
                return Ok(());
            }
        };

        let account_pubkey = match account_info.pubkey.try_into() {
            Ok(bytes) => solana_pubkey::Pubkey::new_from_array(bytes),
            Err(_) => {
                log::error!(
                    "invalid account pubkey length: {} bytes at slot {}",
                    account_info.pubkey.len(),
                    slot
                );
                return Ok(());
            }
        };
        let owner_pubkey = match account_info.owner.try_into() {
            Ok(bytes) => solana_pubkey::Pubkey::new_from_array(bytes),
            Err(_) => {
                log::error!(
                    "invalid owner pubkey length: {} bytes at slot {}",
                    account_info.owner.len(),
                    slot
                );
                return Ok(());
            }
        };

        if !filter.matches(&account_pubkey, &owner_pubkey) {
            return Ok(());
        }

        let client = match &self.client {
            Some(c) => c,
            None => {
                return Err(GeyserPluginError::Custom(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "ClickHouse client not initialized",
                ))));
            }
        };

        let account_data = Account {
            pubkey: account_pubkey.to_bytes(),
            owner: owner_pubkey.to_bytes(),
            slot,
            lamports: account_info.lamports,
            executable: account_info.executable,
            rent_epoch: account_info.rent_epoch,
            data: account_info.data.to_vec(),
            write_version: account_info.write_version,
            updated_at: chrono::Utc::now(),
        };

        // Send to ClickHouse with backpressure handling.
        match client.try_send_account(account_data) {
            Ok(()) => Ok(()),
            Err(ClickHouseError::BufferOverflow(_)) => {
                // Channel full - drop update to maintain throughput
                // Log sparingly (every 10k slots).
                if slot % 10000 == 0 {
                    log::warn!("account channel at capacity at slot {}", slot);
                }
                Ok(())
            }
            Err(ClickHouseError::ChannelClosed(_)) => {
                log::error!("account ingestion system shutdown at slot {}", slot);
                Ok(())
            }
            Err(e) => {
                log::error!("account send failed for slot {}: {}", slot, e);
                Ok(())
            }
        }
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        let filter = match &self.transactions_filter {
            Some(f) => f,
            None => return Ok(()),
        };

        let client = match &self.client {
            Some(c) => c,
            None => {
                return Err(GeyserPluginError::Custom(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "ClickHouse client not initialized",
                ))));
            }
        };

        let tx_data = match transaction {
            ReplicaTransactionInfoVersions::V0_0_3(tx_info) => {
                if !filter.matches(
                    tx_info.is_vote,
                    tx_info.transaction.message.static_account_keys().iter(),
                ) {
                    return Ok(());
                }

                let message_type = match &tx_info.transaction.message {
                    VersionedMessage::Legacy(_) => 0u8,
                    VersionedMessage::V0(_) => 1u8,
                };
                let static_account_keys = tx_info.transaction.message.static_account_keys();
                let signature = *tx_info.signature.as_array();
                let now = chrono::Utc::now();
                let pre_balances = &tx_info.transaction_status_meta.pre_balances;
                let post_balances = &tx_info.transaction_status_meta.post_balances;
                let fee = tx_info.transaction_status_meta.fee;
                let balance_changes: Vec<BalanceChange> = pre_balances
                    .iter()
                    .zip(post_balances.iter())
                    .enumerate()
                    .filter_map(|(index, (&pre, &post))| {
                        if pre == post {
                            return None;
                        }
                        // Skip if index exceeds u16::MAX (extremely unlikely but possible)
                        if index > u16::MAX as usize {
                            log::warn!("Skipping balance change at index {} (exceeds u16::MAX) for transaction at slot {}", index, slot);
                            return None;
                        }

                        let account = static_account_keys.get(index)?;
                        let account_bytes: [u8; 32] = account.as_ref().try_into().ok()?;

                        Some(BalanceChange {
                            signature,
                            account: account_bytes,
                            account_index: index as u16,
                            pre_balance: pre,
                            post_balance: post,
                            updated_at: now,
                        })
                    })
                    .collect();

                Transaction {
                    signature,
                    slot,
                    tx_index: tx_info.index as usize,
                    is_vote: tx_info.is_vote,
                    message_type,
                    success: tx_info.transaction_status_meta.status.is_ok(),
                    fee,
                    balance_changes,
                    updated_at: now,
                }
            }
            _ => {
                log::warn!("unsupported transaction info version. only V0_0_3 is supported");
                return Ok(());
            }
        };

        match client.try_send_transaction(tx_data) {
            Ok(()) => Ok(()),
            Err(ClickHouseError::BufferOverflow(_)) => {
                log::warn!("transaction channel at capacity at slot {}", slot);
                Ok(())
            }
            Err(ClickHouseError::ChannelClosed(_)) => {
                log::error!("transaction ingestion system shutdown at slot {}", slot);
                Ok(())
            }
            Err(e) => {
                log::error!("transaction send failed for slot {}: {}", slot, e);
                Ok(())
            }
        }
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        let client = match &self.client {
            Some(c) => c,
            None => {
                return Err(GeyserPluginError::Custom(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "ClickHouse client not initialized",
                ))));
            }
        };

        let slot_data = Slot {
            slot,
            parent,
            status: match status {
                SlotStatus::Processed => 1,
                SlotStatus::Confirmed => 2,
                SlotStatus::Rooted => 3,
                SlotStatus::FirstShredReceived => 4,
                SlotStatus::Completed => 5,
                SlotStatus::CreatedBank => 6,
                SlotStatus::Dead(_) => 7,
            },
            updated_at: chrono::Utc::now(),
        };

        match client.try_send_slot(slot_data) {
            Ok(()) => Ok(()),
            Err(ClickHouseError::BufferOverflow(_)) => {
                // Channel is full, log occasionally.
                if slot % 1000 == 0 {
                    log::debug!("slot channel backpressure at slot {}", slot);
                }
                Ok(())
            }
            Err(ClickHouseError::ChannelClosed(_)) => {
                log::error!("slot ingestion system shutdown");
                Ok(())
            }
            Err(e) => {
                log::error!("slot send failed: {}", e);
                Ok(())
            }
        }
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> PluginResult<()> {
        let client = match &self.client {
            Some(c) => c,
            None => {
                return Err(GeyserPluginError::Custom(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "ClickHouse client not initialized",
                ))));
            }
        };

        let (slot, block) = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_3(b) => build_block_v3(b),
            ReplicaBlockInfoVersions::V0_0_4(b) => build_block_v4(b),
            _ => {
                return Err(GeyserPluginError::SlotStatusUpdateError {
                    msg: "unsupported block info version V0_0_1 or V0_0_2. only V0_0_3 and V0_0_4 are supported"
                        .to_string(),
                })
            }
        };

        if let Err(e) = client.try_send_block(block) {
            match e {
                ClickHouseError::BufferOverflow(_) => {
                    if slot % 1000 == 0 {
                        log::debug!("block channel backpressure at slot {}", slot);
                    }
                }
                ClickHouseError::ChannelClosed(_) => {
                    log::error!("block ingestion system shutdown");
                }
                other => {
                    log::error!("block send failed: {}", other);
                }
            }
        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        log::info!("validator startup complete, ready to receive updates");
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        self.accounts_filter
            .as_ref()
            .map_or_else(|| false, |filter| filter.is_enabled())
    }

    fn transaction_notifications_enabled(&self) -> bool {
        self.transactions_filter
            .as_ref()
            .map_or_else(|| false, |filter| filter.is_enabled())
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the JacanaPlugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = JacanaPlugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
