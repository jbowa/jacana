use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    serde::Deserialize,
    std::{fs::read_to_string, path::Path, time::Duration},
};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TransactionsFilterCfg {
    #[serde(default)]
    pub mentions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AccountsFilterCfg {
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub programs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FiltersCfg {
    #[serde(default)]
    pub transactions: TransactionsFilterCfg,
    #[serde(default)]
    pub accounts: AccountsFilterCfg,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchPolicy {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub flush_ms: u64,
    pub workers: u16,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelMaxBufferCfg {
    pub slots: usize,
    pub blocks: usize,
    pub transactions: usize,
    pub accounts: usize,
}

impl Default for ChannelMaxBufferCfg {
    fn default() -> Self {
        Self {
            slots: 10_000,
            blocks: 10_000,
            transactions: 50_000,
            accounts: 100_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowCfg {
    pub batch_size: usize,
    pub memory_limit_mb: usize,
}

impl Default for ArrowCfg {
    fn default() -> Self {
        Self {
            batch_size: 16_384,
            memory_limit_mb: 512,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchCfg {
    pub accounts: BatchPolicy,
    pub transactions: BatchPolicy,
    pub slots: BatchPolicy,
    pub blocks: BatchPolicy,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            max_rows: 50_000,
            max_bytes: 64 * 1024 * 1024,
            flush_ms: 1000,
            workers: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CompressionMethod {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "lz4")]
    Lz4,
    #[serde(rename = "zstd")]
    Zstd,
}

impl Default for CompressionMethod {
    fn default() -> Self {
        Self::Lz4
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolCfg {
    pub max_size: u32,
    pub min_idle: Option<u32>,
    pub connection_timeout_ms: u64,
    pub idle_timeout_ms: Option<u64>,
    pub test_on_check_out: bool,
    pub max_lifetime_ms: Option<u64>,
    pub acquire_timeout_ms: u64,
}

impl Default for PoolCfg {
    fn default() -> Self {
        Self {
            max_size: 25,
            min_idle: Some(8),
            connection_timeout_ms: 30_000,
            idle_timeout_ms: Some(600_000),
            test_on_check_out: true,
            max_lifetime_ms: Some(1_800_000),
            acquire_timeout_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsCfg {
    #[serde(default)]
    pub secure: bool,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub cafile: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionCfg {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub validate_schemas: bool,
    #[serde(default)]
    pub compression: CompressionMethod,
    #[serde(default)]
    pub pool: PoolCfg,
    #[serde(default)]
    pub tls: TlsCfg,
}

impl Default for ConnectionCfg {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 9000,
            database: "default".to_string(),
            username: "default".to_string(),
            validate_schemas: true,
            password: String::new(),
            compression: CompressionMethod::default(),
            pool: PoolCfg::default(),
            tls: TlsCfg::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub connection: ConnectionCfg,
    #[serde(default)]
    pub channel_max_buffer: ChannelMaxBufferCfg,
    #[serde(default)]
    pub log_level: String,
    #[serde(default)]
    pub arrow: ArrowCfg,
    pub filters: FiltersCfg,
    pub libpath: String,
    pub starting_slot: u64,
    pub batch: BatchCfg,
}

impl Config {
    fn load_from_str(config: &str) -> PluginResult<Self> {
        let config: Config = serde_json::from_str(config).map_err(|error| {
            GeyserPluginError::ConfigFileReadError {
                msg: format!("Failed to parse JSON: {}", error),
            }
        })?;

        Ok(config)
    }

    pub fn load_from_file(file: &str) -> PluginResult<Self> {
        if !Path::new(file).exists() {
            return Err(GeyserPluginError::ConfigFileOpenError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Config file not found: {}", file),
            )));
        }

        let config = read_to_string(file).map_err(|error| {
            GeyserPluginError::ConfigFileOpenError(std::io::Error::new(
                error.kind(),
                format!("Failed to read config file '{}': {}", file, error),
            ))
        })?;

        Self::load_from_str(&config)
    }

    pub fn pool_connection_timeout(&self) -> Duration {
        Duration::from_millis(self.connection.pool.connection_timeout_ms)
    }

    pub fn pool_idle_timeout(&self) -> Option<Duration> {
        self.connection
            .pool
            .idle_timeout_ms
            .map(Duration::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::io::Write, tempfile::NamedTempFile};

    fn create_temp_config(content: &str) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file
            .write_all(content.as_bytes())
            .expect("Failed to write to temp file");
        temp_file
    }

    #[test]
    fn test_load_valid_minimal_config() {
        let config_json = r#"
        {
            "filters": {
                "transactions": {},
                "accounts": {}
            },
            "libpath": "/path/to/lib",
            "starting_slot": 12345,
            "batch": {
                "accounts":      { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "transactions":  { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "slots":         { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 },
                "blocks":        { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 }
            }
        }
        "#;

        let cfg = Config::load_from_str(config_json).unwrap();
        assert_eq!(cfg.libpath, "/path/to/lib");
        assert_eq!(cfg.starting_slot, 12345);
        assert!(cfg.filters.transactions.mentions.is_empty());
        assert!(cfg.filters.accounts.addresses.is_empty());
        assert!(cfg.filters.accounts.programs.is_empty());
        assert_eq!(cfg.batch.accounts.max_rows, 10_000);
        assert_eq!(cfg.batch.transactions.flush_ms, 2000);
        assert_eq!(cfg.connection.port, 9000);
        assert_eq!(cfg.channel_max_buffer.slots, 10_000);
        assert_eq!(cfg.channel_max_buffer.blocks, 10_000);
        assert_eq!(cfg.channel_max_buffer.transactions, 50_000);
        assert_eq!(cfg.channel_max_buffer.accounts, 100_000);
        assert_eq!(cfg.arrow.batch_size, 16_384);
        assert_eq!(cfg.arrow.memory_limit_mb, 512);
    }

    #[test]
    fn test_load_valid_full_config() {
        let config_json = r#"
        {
            "connection": {
                "host": "clickhouse.example.com",
                "port": 9440,
                "database": "solana_indexer",
                "username": "indexer",
                "password": "secret123",
                "compression": "zstd",
                "pool": {
                    "max_size": 25,
                    "min_idle": 8,
                    "connection_timeout_ms": 45000,
                    "idle_timeout_ms": 600000,
                    "test_on_check_out": true,
                    "max_lifetime_ms": 1800000,
                    "acquire_timeout_ms": 15000
                },
                "tls": {
                    "secure": true,
                    "domain": "clickhouse.example.com",
                    "cafile": "/etc/ssl/certs/ca.pem"
                }
            },
            "channel_max_buffer": {
                "slots": 5000,
                "blocks": 5000,
                "transactions": 75000,
                "accounts": 150000
            },
            "filters": {
                "transactions": { "mentions": ["*"] },
                "accounts": {
                    "addresses": ["11111111111111111111111111111112"],
                    "programs":  ["11111111111111111111111111111113"]
                }
            },
            "libpath": "/custom/lib/path",
            "starting_slot": 98765,
            "batch": {
                "accounts":      { "max_rows": 60000, "max_bytes": 67108864, "flush_ms": 2000, "workers": 2 },
                "transactions":  { "max_rows": 60000, "max_bytes": 67108864, "flush_ms": 2000, "workers": 4 },
                "slots":         { "max_rows": 1000,  "max_bytes": 1048576,  "flush_ms": 300,  "workers": 1 },
                "blocks":        { "max_rows": 1000,  "max_bytes": 1048576,  "flush_ms": 300,  "workers": 1 }
            }
        }
        "#;

        let cfg = Config::load_from_str(config_json).unwrap();
        assert_eq!(cfg.libpath, "/custom/lib/path");
        assert_eq!(cfg.starting_slot, 98765);
        assert_eq!(cfg.filters.transactions.mentions, vec!["*"]);
        assert_eq!(cfg.connection.host, "clickhouse.example.com");
        assert_eq!(cfg.connection.port, 9440);
        assert_eq!(cfg.connection.pool.max_size, 25);
        assert!(cfg.connection.tls.secure);
        assert_eq!(
            cfg.connection.tls.domain.as_deref(),
            Some("clickhouse.example.com")
        );
        assert_eq!(
            cfg.connection.tls.cafile.as_deref(),
            Some("/etc/ssl/certs/ca.pem")
        );
        assert_eq!(cfg.batch.transactions.workers, 4);
        assert_eq!(cfg.channel_max_buffer.slots, 5000);
        assert_eq!(cfg.channel_max_buffer.blocks, 5000);
        assert_eq!(cfg.channel_max_buffer.transactions, 75000);
        assert_eq!(cfg.channel_max_buffer.accounts, 150000);
    }

    #[test]
    fn test_load_from_file_success() {
        let config_json = r#"
        {
            "filters": { "transactions": {}, "accounts": {} },
            "libpath": "/file/lib",
            "starting_slot": 5000,
            "batch": {
                "accounts":      { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "transactions":  { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "slots":         { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 },
                "blocks":        { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 }
            }
        }
        "#;

        let temp = create_temp_config(config_json);
        let cfg = Config::load_from_file(temp.path().to_str().unwrap()).unwrap();
        assert_eq!(cfg.libpath, "/file/lib");
        assert_eq!(cfg.starting_slot, 5000);
    }

    #[test]
    fn test_load_from_nonexistent_file() {
        let result = Config::load_from_file("/no/such/config.json");
        assert!(matches!(
            result,
            Err(GeyserPluginError::ConfigFileOpenError(_))
        ));
    }

    #[test]
    fn test_load_invalid_json() {
        let invalid = r#"
        {
            "filters": { "transactions": {
        "#;

        let result = Config::load_from_str(invalid);
        assert!(matches!(
            result,
            Err(GeyserPluginError::ConfigFileReadError { .. })
        ));
    }

    #[test]
    fn test_load_missing_required_fields() {
        let incomplete = r#"
        {
            "filters": {},
            "libpath": "/lib",
            "starting_slot": 1000
        }
        "#;

        let result = Config::load_from_str(incomplete);
        assert!(matches!(
            result,
            Err(GeyserPluginError::ConfigFileReadError { .. })
        ));
    }

    #[test]
    fn test_unknown_fields_rejected() {
        let json = r#"
        {
            "filters": {
                "transactions": {
                    "mentions": ["*"],
                    "unknown_field": "nope"
                },
                "accounts": {}
            },
            "libpath": "/lib",
            "starting_slot": 1000,
            "batch": {
                "accounts":      { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "transactions":  { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "slots":         { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 },
                "blocks":        { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 }
            }
        }
        "#;

        let result = Config::load_from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_filter_arrays_are_ok() {
        let json = r#"
        {
            "filters": {
                "transactions": { "mentions": [] },
                "accounts": { "addresses": [], "programs": [] }
            },
            "libpath": "/lib",
            "starting_slot": 1000,
            "batch": {
                "accounts":      { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "transactions":  { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "slots":         { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 },
                "blocks":        { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 }
            }
        }
        "#;

        let cfg = Config::load_from_str(json).unwrap();
        assert!(cfg.filters.transactions.mentions.is_empty());
        assert!(cfg.filters.accounts.addresses.is_empty());
        assert!(cfg.filters.accounts.programs.is_empty());
    }

    #[test]
    fn test_channel_max_buffer_defaults() {
        let json = r#"
        {
            "filters": { "transactions": {}, "accounts": {} },
            "libpath": "/lib",
            "starting_slot": 1000,
            "batch": {
                "accounts":      { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "transactions":  { "max_rows": 10000, "max_bytes": 10485760, "flush_ms": 2000, "workers": 1 },
                "slots":         { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 },
                "blocks":        { "max_rows": 500,   "max_bytes": 1048576,  "flush_ms": 500,  "workers": 1 }
            }
        }
        "#;

        let cfg = Config::load_from_str(json).unwrap();
        assert_eq!(cfg.channel_max_buffer.slots, 10_000);
        assert_eq!(cfg.channel_max_buffer.blocks, 10_000);
        assert_eq!(cfg.channel_max_buffer.transactions, 50_000);
        assert_eq!(cfg.channel_max_buffer.accounts, 100_000);
    }
}
