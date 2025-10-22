use {
    crate::{
        clickhouse::ClickHouseError,
        config::{CompressionMethod as ConfigCompression, ConnectionCfg},
    },
    clickhouse_arrow::{
        arrow::arrow::array::{
            Array, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringArray,
        },
        bb8::{Pool, PooledConnection},
        prelude::Secret,
        ArrowFormat, ClientOptions, CompressionMethod, ConnectionManager, ConnectionPoolBuilder,
    },
    futures_util::StreamExt,
    log::{debug, error, info},
    once_cell::sync::Lazy,
    std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration},
    tokio::time::timeout,
};

pub const TBL_ACCOUNT: &str = "account";
pub const TBL_SLOT: &str = "slot";
pub const TBL_TRANSACTION: &str = "transaction";
pub const TBL_BLOCK: &str = "block";
pub const TBL_TOKEN: &str = "token";

type TableSchema = &'static [(&'static str, &'static str)];

const ACCOUNT_SCHEMA: TableSchema = &[
    ("pubkey", "FixedString(32)"),
    ("owner", "FixedString(32)"),
    ("slot", "UInt64"),
    ("lamports", "UInt64"),
    ("executable", "Bool"),
    ("rent_epoch", "UInt64"),
    ("data", "String"),
    ("write_version", "UInt64"),
    ("updated_at", "DateTime64(3)"),
];

const SLOT_SCHEMA: TableSchema = &[
    ("slot", "UInt64"),
    ("parent", "Nullable(UInt64)"),
    (
        "status",
        "Enum8('processed' = 1, 'confirmed' = 2, 'rooted' = 3, 'first_shred_received' = 4, 'completed' = 5, 'created_bank' = 6, 'dead' = 7)",
    ),
    ("updated_at", "DateTime64(3)"),
];

const BLOCK_SCHEMA: TableSchema = &[
    ("slot", "UInt64"),
    ("blockhash", "String"),
    ("block_time", "Nullable(DateTime64(3))"),
    ("block_height", "Nullable(UInt64)"),
    ("parent_slot", "UInt64"),
    ("rewards_pubkey", "Array(FixedString(32))"),
    ("rewards_lamports", "Array(Int64)"),
    ("rewards_post_balance", "Array(Int64)"),
    (
        "rewards_reward_type",
        "Array(Nullable(Enum8('Unknown' = 0, 'Fee' = 1, 'Rent' = 2, 'Staking' = 3, 'Voting' = 4)))",
    ),
    ("rewards_commission", "Array(Nullable(Int16))"),
    ("updated_at", "DateTime64(3)"),
];

const TOKEN_SCHEMA: TableSchema = &[
    ("mint", "FixedString(32)"),
    ("decimals", "UInt8"),
    ("supply", "UInt128"),
    ("mint_authority", "Nullable(FixedString(32))"),
    ("freeze_authority", "Nullable(FixedString(32))"),
    ("close_authority", "Nullable(FixedString(32))"),
    ("is_initialized", "Bool"),
    ("slot", "UInt64"),
    ("created_at", "DateTime64(3)"),
    ("updated_at", "DateTime64(3)"),
    ("name", "Nullable(String)"),
    ("symbol", "Nullable(String)"),
    ("uri", "Nullable(String)"),
    (
        "token_standard",
        "Enum8('fungible' = 1, 'non_fungible' = 2, 'fungible_asset' = 3)",
    ),
];

const TRANSACTION_SCHEMA: TableSchema = &[
    ("signature", "FixedString(64)"),
    ("slot", "UInt64"),
    ("tx_index", "UInt64"),
    ("is_vote", "Bool"),
    ("message_type", "UInt8"),
    ("success", "Bool"),
    ("fee", "UInt64"),
    ("updated_at", "DateTime64(3)"),
    ("balance_changes.account", "Array(FixedString(32))"),
    ("balance_changes.account_index", "Array(UInt8)"),
    ("balance_changes.pre_balance", "Array(UInt64)"),
    ("balance_changes.post_balance", "Array(UInt64)"),
    ("balance_changes.updated_at", "Array(DateTime64(3))"),
];

static SCHEMA_VALIDATION_CACHE: Lazy<std::sync::RwLock<HashMap<String, bool>>> =
    Lazy::new(|| std::sync::RwLock::new(HashMap::new()));

#[derive(Debug)]
pub struct ConnectionPool {
    pool: Pool<ConnectionManager<ArrowFormat>>,
    acquire_timeout: Duration,
}

impl ConnectionPool {
    pub async fn new(conn_cfg: &ConnectionCfg) -> Result<Arc<Self>, ClickHouseError> {
        log::info!("creating connection pool");
        Self::validate_config(conn_cfg)?;

        let dest = format!("{}:{}", conn_cfg.host, conn_cfg.port);

        let builder = ConnectionPoolBuilder::<ArrowFormat>::new(dest)
            .configure_client(|b| b.with_options(Self::build_client_options(conn_cfg)))
            .configure_pool(|p| {
                let mut p = p.max_size(conn_cfg.pool.max_size);
                if let Some(min_idle) = conn_cfg.pool.min_idle {
                    p = p.min_idle(Some(min_idle));
                }
                p = p
                    .connection_timeout(Duration::from_millis(conn_cfg.pool.connection_timeout_ms));
                if let Some(ms) = conn_cfg.pool.idle_timeout_ms {
                    p = p.idle_timeout(Some(Duration::from_millis(ms)));
                }
                if let Some(ms) = conn_cfg.pool.max_lifetime_ms {
                    p = p.max_lifetime(Some(Duration::from_millis(ms)));
                }

                p
            });

        let pool = if conn_cfg.pool.test_on_check_out {
            builder
                .with_check()
                .build()
                .await
                .map_err(|e| ClickHouseError::Connection(format!("create pool: {e}")))?
        } else {
            builder
                .build()
                .await
                .map_err(|e| ClickHouseError::Connection(format!("create pool: {e}")))?
        };

        let connection_pool = Arc::new(Self {
            pool,
            acquire_timeout: Duration::from_millis(conn_cfg.pool.acquire_timeout_ms),
        });

        connection_pool.test_connectivity().await?;
        connection_pool.validate_schemas(conn_cfg).await?;

        info!(
            "successfully created ClickHouse connection pool with {} max connections to {}:{}",
            conn_cfg.pool.max_size, conn_cfg.host, conn_cfg.port
        );

        Ok(connection_pool)
    }

    pub async fn get_connection(
        &self,
    ) -> Result<PooledConnection<'_, ConnectionManager<ArrowFormat>>, ClickHouseError> {
        timeout(self.acquire_timeout, self.pool.get())
            .await
            .map_err(|_| ClickHouseError::Timeout("connection acquire timeout".into()))?
            .map_err(|e| ClickHouseError::Connection(format!("get connection: {e}")))
    }

    pub async fn health_check(&self) -> Result<(), ClickHouseError> {
        let conn = self.get_connection().await?;
        let mut stream = conn
            .query("SELECT 1", None)
            .await
            .map_err(|e| ClickHouseError::Connection(format!("health query: {e}")))?;

        if let Some(batch) = stream.next().await {
            batch.map_err(|e| ClickHouseError::Connection(format!("health batch: {e}")))?;
        }

        Ok(())
    }

    pub fn pool_status(&self) -> clickhouse_arrow::bb8::State {
        self.pool.state()
    }

    fn validate_config(conn_cfg: &ConnectionCfg) -> Result<(), ClickHouseError> {
        if conn_cfg.host.is_empty() {
            return Err(ClickHouseError::Configuration(
                "host cannot be empty".into(),
            ));
        }
        if conn_cfg.port == 0 {
            return Err(ClickHouseError::Configuration(
                "port must be greater than 0".into(),
            ));
        }
        if conn_cfg.database.is_empty() {
            return Err(ClickHouseError::Configuration(
                "database cannot be empty".into(),
            ));
        }
        if conn_cfg.pool.max_size == 0 {
            return Err(ClickHouseError::Configuration(
                "pool max_size must be greater than 0".into(),
            ));
        }
        if conn_cfg.pool.acquire_timeout_ms == 0 {
            return Err(ClickHouseError::Configuration(
                "pool acquire_timeout_ms must be greater than 0".into(),
            ));
        }
        if conn_cfg.pool.min_idle > Some(conn_cfg.pool.max_size) {
            return Err(ClickHouseError::Configuration(
                "pool min_idle must be no larger than max_size".into(),
            ));
        }

        Ok(())
    }

    fn build_client_options(conn_cfg: &ConnectionCfg) -> ClientOptions {
        let compression = match conn_cfg.compression {
            ConfigCompression::None => CompressionMethod::None,
            ConfigCompression::Lz4 => CompressionMethod::LZ4,
            ConfigCompression::Zstd => CompressionMethod::ZSTD,
        };

        let mut opts = ClientOptions::default()
            .with_username(conn_cfg.username.clone())
            .with_password(Secret::new(conn_cfg.password.clone()))
            .with_default_database(conn_cfg.database.clone())
            .with_use_tls(conn_cfg.tls.secure)
            .with_compression(compression);

        if conn_cfg.tls.secure {
            if let Some(domain) = conn_cfg.tls.domain.as_ref() {
                opts = opts.with_domain(domain.clone());
            } else {
                opts = opts.with_domain(conn_cfg.host.clone());
            }
            if let Some(cafile) = conn_cfg.tls.cafile.as_ref() {
                opts = opts.with_cafile(cafile);
            }
        }

        opts
    }

    async fn test_connectivity(&self) -> Result<(), ClickHouseError> {
        let conn = self.get_connection().await?;
        let mut stream = conn.query("SELECT 1", None).await.map_err(|e| {
            error!("Failed connectivity test: {e}");
            ClickHouseError::Connection(format!("connectivity query: {e}"))
        })?;

        if let Some(batch) = stream.next().await {
            batch.map_err(|e| ClickHouseError::Connection(format!("connectivity batch: {e}")))?;
        }

        debug!("ClickHouse connectivity test passed");
        Ok(())
    }

    async fn validate_schemas(&self, conn_cfg: &ConnectionCfg) -> Result<(), ClickHouseError> {
        if !conn_cfg.validate_schemas {
            info!("skipping schema validation (validate_schemas = false)");
            return Ok(());
        }

        let validations = [
            (TBL_ACCOUNT, ACCOUNT_SCHEMA),
            (TBL_SLOT, SLOT_SCHEMA),
            (TBL_BLOCK, BLOCK_SCHEMA),
            (TBL_TOKEN, TOKEN_SCHEMA),
            (TBL_TRANSACTION, TRANSACTION_SCHEMA),
        ];

        for (table_name, schema) in validations {
            self.validate_table(table_name, schema).await?;
        }

        info!("All table schemas validated successfully");
        Ok(())
    }

    async fn validate_table(
        &self,
        table: &str,
        expected: TableSchema,
    ) -> Result<(), ClickHouseError> {
        {
            let cache = SCHEMA_VALIDATION_CACHE
                .read()
                .map_err(|_| ClickHouseError::Connection("Schema cache lock poisoned".into()))?;

            if cache.get(table) == Some(&true) {
                return Ok(());
            }
        }

        let sql = format!(
            "SELECT CAST(name AS String) AS name, CAST(type AS String) AS type \
             FROM system.columns \
             WHERE database = currentDatabase() AND table = '{}' \
             ORDER BY position",
            Self::escape_sql_string(table)
        );

        let conn = self.get_connection().await.map_err(|e| {
            error!(
                "Failed to acquire connection for schema validation of table '{}': {}",
                table, e
            );
            e
        })?;

        let mut stream = conn.query(&sql, None).await.map_err(|e| {
            error!("Failed to query schema for table '{}': {}", table, e);
            ClickHouseError::Connection(format!("schema query: {e}"))
        })?;

        let mut actual_count = 0usize;
        let expected_count = expected.len();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| ClickHouseError::Connection(format!("schema batch: {e}")))?;

            let names = StrCol::from_any(
                batch
                    .column_by_name("name")
                    .ok_or_else(|| ClickHouseError::Connection("missing 'name' column".into()))?
                    .as_any(),
            )?;

            let types = StrCol::from_any(
                batch
                    .column_by_name("type")
                    .ok_or_else(|| ClickHouseError::Connection("missing 'type' column".into()))?
                    .as_any(),
            )?;

            let batch_rows = names.len().min(types.len());

            for i in 0..batch_rows {
                if actual_count >= expected_count {
                    return self.build_schema_error(
                        table,
                        expected,
                        SchemaError::ExtraColumn {
                            position: actual_count,
                            name: names.value(i)?.to_owned(),
                            column_type: types.value(i)?.to_owned(),
                        },
                    );
                }

                let (expected_name, expected_type) = expected[actual_count];
                let actual_name = names.value(i)?;
                let actual_type = types.value(i)?;

                if actual_name != expected_name {
                    return self.build_schema_error(
                        table,
                        expected,
                        SchemaError::NameMismatch {
                            position: actual_count,
                            expected: expected_name,
                            actual: actual_name.to_owned(),
                        },
                    );
                }

                if !Self::types_match_normalized(actual_type, expected_type, actual_name) {
                    return self.build_schema_error(
                        table,
                        expected,
                        SchemaError::TypeMismatch {
                            position: actual_count,
                            column: expected_name,
                            expected: expected_type,
                            actual: actual_type.to_owned(),
                        },
                    );
                }

                actual_count += 1;
            }
        }

        if actual_count == 0 {
            return Err(ClickHouseError::SchemaValidation(format!(
                "table '{}' does not exist or has no columns",
                table
            )));
        }

        if actual_count != expected_count {
            return self.build_schema_error(
                table,
                expected,
                SchemaError::ColumnCountMismatch {
                    expected: expected_count,
                    actual: actual_count,
                },
            );
        }

        {
            let mut cache = SCHEMA_VALIDATION_CACHE
                .write()
                .map_err(|_| ClickHouseError::Connection("schema cache lock poisoned".into()))?;
            cache.insert(table.to_owned(), true);
        }

        debug!(
            "table '{}' schema validation passed ({} columns)",
            table, expected_count
        );
        Ok(())
    }

    #[inline]
    fn types_match_normalized(actual: &str, expected: &str, column_name: &str) -> bool {
        if actual == expected {
            return true;
        }

        let normalized_actual = Self::normalize_type(actual, column_name);
        let normalized_expected = Self::normalize_type(expected, column_name);

        normalized_actual == normalized_expected
    }

    #[inline]
    fn normalize_type<'a>(column_type: &'a str, column_name: &str) -> Cow<'a, str> {
        let mut result = Cow::Borrowed(column_type);

        if matches!(column_name, "executable" | "is_vote" | "is_initialized")
            && result.contains("UInt8")
        {
            result = Cow::Owned(result.replace("UInt8", "Bool"));
        }

        if result.contains("DateTime64(3,'UTC')") || result.contains("DateTime64(3,\\'UTC\\')") {
            let mut owned = result.into_owned();
            owned = owned
                .replace("DateTime64(3,'UTC')", "DateTime64(3)")
                .replace("DateTime64(3,\\'UTC\\')", "DateTime64(3)");
            result = Cow::Owned(owned);
        }

        if result.contains("Enum8(") || result.contains("Enum16(") {
            let mut owned = result.into_owned();

            if let Some(normalized) = Self::normalize_enum(&owned, "Enum8") {
                owned = normalized;
            }
            if let Some(normalized) = Self::normalize_enum(&owned, "Enum16") {
                owned = normalized;
            }

            result = Cow::Owned(owned);
        }

        result
    }

    #[inline]
    fn escape_sql_string(input: &str) -> String {
        input.replace('\'', "''")
    }

    fn build_schema_error(
        &self,
        table: &str,
        expected: TableSchema,
        error: SchemaError,
    ) -> Result<(), ClickHouseError> {
        let mut msg = match error {
            SchemaError::ColumnCountMismatch { expected, actual } => {
                format!(
                    "table '{}' column count mismatch: expected {}, got {}",
                    table, expected, actual
                )
            }
            SchemaError::NameMismatch {
                position,
                expected,
                actual,
            } => {
                format!(
                    "table '{}' column {} name mismatch: expected '{}', got '{}'",
                    table,
                    position + 1,
                    expected,
                    actual
                )
            }
            SchemaError::TypeMismatch {
                position,
                column,
                expected,
                actual,
            } => {
                format!(
                    "table '{}' column {} ('{}') type mismatch: expected '{}', got '{}'",
                    table,
                    position + 1,
                    column,
                    expected,
                    actual
                )
            }
            SchemaError::ExtraColumn {
                position,
                name,
                column_type,
            } => {
                format!(
                    "table '{}' has extra column {} '{}' of type '{}'",
                    table,
                    position + 1,
                    name,
                    column_type
                )
            }
        };

        msg.push_str(&format!("\n\nexpected schema for table '{}':", table));
        for (i, (name, col_type)) in expected.iter().enumerate() {
            msg.push_str(&format!("\n  {}: {} -> {}", i + 1, name, col_type));
        }

        error!("schema validation failed for table '{}'", table);
        Err(ClickHouseError::SchemaValidation(msg))
    }

    fn normalize_enum(s: &str, tag: &str) -> Option<String> {
        let needle = format!("{tag}(");
        let start = s.find(&needle)?;
        let body_start = start + needle.len();
        let body_end = s[body_start..].find(')')? + body_start;
        let body = &s[body_start..body_end];

        let mut items: Vec<(String, String)> = body
            .split(',')
            .filter(|x| !x.trim().is_empty())
            .map(|kv| {
                let mut parts = kv.split('=');
                let k = parts.next().unwrap().trim().to_string();
                let v = parts.next().unwrap().trim().to_string();
                (k, v)
            })
            .collect();

        items.sort_by(|a, b| a.0.cmp(&b.0));

        let rebuilt = format!(
            "{tag}({})",
            items
                .into_iter()
                .map(|(k, v)| format!("{k} = {v}"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let mut out = String::with_capacity(s.len());
        out.push_str(&s[..start]);
        out.push_str(&rebuilt);
        out.push_str(&s[body_end + 1..]);
        Some(out)
    }
}

#[derive(Debug)]
enum SchemaError {
    ColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    NameMismatch {
        position: usize,
        expected: &'static str,
        actual: String,
    },
    TypeMismatch {
        position: usize,
        column: &'static str,
        expected: &'static str,
        actual: String,
    },
    ExtraColumn {
        position: usize,
        name: String,
        column_type: String,
    },
}

enum StrCol<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    BinaryView(&'a BinaryViewArray),
}

impl<'a> StrCol<'a> {
    fn from_any(any: &'a dyn std::any::Any) -> Result<Self, ClickHouseError> {
        if let Some(a) = any.downcast_ref::<StringArray>() {
            return Ok(StrCol::Utf8(a));
        }
        if let Some(a) = any.downcast_ref::<LargeStringArray>() {
            return Ok(StrCol::LargeUtf8(a));
        }
        if let Some(a) = any.downcast_ref::<BinaryArray>() {
            return Ok(StrCol::Binary(a));
        }
        if let Some(a) = any.downcast_ref::<LargeBinaryArray>() {
            return Ok(StrCol::LargeBinary(a));
        }
        if let Some(a) = any.downcast_ref::<BinaryViewArray>() {
            return Ok(StrCol::BinaryView(a));
        }
        Err(ClickHouseError::Connection(
            "invalid string column type".into(),
        ))
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            StrCol::Utf8(a) => a.len(),
            StrCol::LargeUtf8(a) => a.len(),
            StrCol::Binary(a) => a.len(),
            StrCol::LargeBinary(a) => a.len(),
            StrCol::BinaryView(a) => a.len(),
        }
    }

    #[inline]
    fn value(&self, i: usize) -> Result<&'a str, ClickHouseError> {
        match self {
            StrCol::Utf8(a) => Ok(a.value(i)),
            StrCol::LargeUtf8(a) => Ok(a.value(i)),
            StrCol::Binary(a) => std::str::from_utf8(a.value(i))
                .map_err(|_| ClickHouseError::Connection("invalid utf-8 in string column".into())),
            StrCol::LargeBinary(a) => std::str::from_utf8(a.value(i))
                .map_err(|_| ClickHouseError::Connection("invalid utf-8 in string column".into())),
            StrCol::BinaryView(a) => std::str::from_utf8(a.value(i))
                .map_err(|_| ClickHouseError::Connection("invalid utf-8 in string column".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::config::{CompressionMethod, ConnectionCfg, PoolCfg, TlsCfg},
    };

    fn cfg_base() -> ConnectionCfg {
        ConnectionCfg {
            host: "localhost".into(),
            port: 9000,
            database: "db".into(),
            validate_schemas: true,
            username: "user".into(),
            password: "pass".into(),
            compression: CompressionMethod::Lz4,
            pool: PoolCfg::default(),
            tls: TlsCfg::default(),
        }
    }

    #[test]
    fn validate_config_rejects_empty_host() {
        let mut cfg = cfg_base();
        cfg.host.clear();
        let err = ConnectionPool::validate_config(&cfg).unwrap_err();
        assert!(matches!(err, ClickHouseError::Configuration(_)));
    }

    #[test]
    fn validate_config_rejects_zero_port() {
        let mut cfg = cfg_base();
        cfg.port = 0;
        let err = ConnectionPool::validate_config(&cfg).unwrap_err();
        assert!(matches!(err, ClickHouseError::Configuration(_)));
    }

    #[test]
    fn validate_config_rejects_empty_database() {
        let mut cfg = cfg_base();
        cfg.database.clear();
        let err = ConnectionPool::validate_config(&cfg).unwrap_err();
        assert!(matches!(err, ClickHouseError::Configuration(_)));
    }

    #[test]
    fn validate_config_rejects_zero_max_size() {
        let mut cfg = cfg_base();
        cfg.pool.max_size = 0;
        let err = ConnectionPool::validate_config(&cfg).unwrap_err();
        assert!(matches!(err, ClickHouseError::Configuration(_)));
    }

    #[test]
    fn validate_config_rejects_zero_acquire_timeout() {
        let mut cfg = cfg_base();
        cfg.pool.acquire_timeout_ms = 0;
        let err = ConnectionPool::validate_config(&cfg).unwrap_err();
        assert!(matches!(err, ClickHouseError::Configuration(_)));
    }

    #[test]
    fn validate_config_rejects_invalid_min_idle() {
        let mut cfg = cfg_base();
        cfg.pool.max_size = 10;
        cfg.pool.min_idle = Some(15);
        let err = ConnectionPool::validate_config(&cfg).unwrap_err();
        assert!(matches!(err, ClickHouseError::Configuration(_)));
    }

    #[test]
    fn validate_config_accepts_valid_config() {
        let cfg = cfg_base();
        assert!(ConnectionPool::validate_config(&cfg).is_ok());
    }

    #[test]
    fn normalize_type_handles_bool_columns() {
        let result = ConnectionPool::normalize_type("UInt8", "executable");
        assert_eq!(result, "Bool");

        let result = ConnectionPool::normalize_type("UInt8", "is_vote");
        assert_eq!(result, "Bool");

        let result = ConnectionPool::normalize_type("UInt8", "is_initialized");
        assert_eq!(result, "Bool");

        let result = ConnectionPool::normalize_type("UInt8", "other_column");
        assert_eq!(result, "UInt8");
    }

    #[test]
    fn normalize_type_handles_datetime_variants() {
        let result = ConnectionPool::normalize_type("DateTime64(3,'UTC')", "updated_at");
        assert_eq!(result, "DateTime64(3)");

        let result = ConnectionPool::normalize_type("DateTime64(3,\\'UTC\\')", "created_at");
        assert_eq!(result, "DateTime64(3)");

        let result = ConnectionPool::normalize_type("DateTime64(3)", "timestamp");
        assert_eq!(result, "DateTime64(3)");
    }
}
