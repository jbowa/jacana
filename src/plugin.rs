use {
    crate::{
        clickhouse::ClickHouseClient,
        config::{BatchCfg, Config},
        filters::{accounts::AccountsFilter, transactions::TransactionsFilter},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, Result as PluginResult,
    },
    tokio::runtime::Builder,
};

#[derive(Default)]
struct Plugin {
    client: Option<ClickHouseClient>,
    accounts_filter: Option<AccountsFilter>,
    transactions_filter: Option<TransactionsFilter>,
    batch: Option<BatchCfg>,
    starting_slot: Option<u64>,
}

impl std::fmt::Debug for Plugin {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn setup_logger(
        &self,
        logger: &'static dyn log::Log,
        level: log::LevelFilter,
    ) -> PluginResult<()> {
        log::set_max_level(level);
        log::set_logger(logger).map_err(|e| GeyserPluginError::Custom(Box::new(e)))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        log::info!("Loading plugin: {:?}", self.name());
        // Load config.
        let config = Config::load_from_file(config_file)?;
        // Setup filters.
        self.accounts_filter = Some(AccountsFilter::from_cfg(&config.filters.accounts)?);
        self.transactions_filter =
            Some(TransactionsFilter::from_cfg(&config.filters.transactions)?);
        self.batch = Some(BatchCfg {
            accounts: config.batch.accounts,
            transactions: config.batch.transactions,
            slots: config.batch.slots,
            blocks: config.batch.blocks,
            tokens: config.batch.tokens,
        });
        self.starting_slot = Some(config.starting_slot);

        let runtime = Builder::new_current_thread()
            .thread_name("clickhouse-coordinator")
            .enable_all()
            .build()
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;

            let client = runtime.block_on(async {
                ClickHouseClient::new(&conn_config, &batch).await
            }).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
    
            self.runtime = Some(runtime);
            self.client = Some(client);
    
            info!("Plugin loaded successfully with coordinator");
            Ok(())

        Ok(())
    }

    fn on_unload(&mut self) {
        log::info!("Unloading plugin: {:?}", self.name());
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the GeyserPluginPostgres pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
