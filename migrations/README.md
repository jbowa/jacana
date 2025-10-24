## Database Migration

Jacana expects the following ClickHouse tables.

### Accounts Table

```sql
CREATE TABLE account (
    pubkey FixedString(32),
    owner FixedString(32),
    slot UInt64,
    lamports UInt64,
    executable Bool,
    rent_epoch UInt64,
    data String,                -- Base64-encoded account data
    write_version UInt64,
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(write_version)
ORDER BY (pubkey, slot)
PARTITION BY intDiv(slot, 10000000);
```

### Transactions Table

```sql
CREATE TABLE transaction (
    signature FixedString(64),
    slot UInt64,
    tx_index UInt64,
    is_vote Bool,
    message_type UInt8,         -- 0 = Legacy, 1 = V0
    success Bool,
    fee UInt64,
    updated_at DateTime64(3) DEFAULT now64(3),
    -- Nested balance changes (flattened as parallel arrays)
    balance_changes Nested (
        account FixedString(32),
        account_index UInt16,
        pre_balance UInt64,
        post_balance UInt64,
        updated_at DateTime64(3)
    )
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (slot, tx_index)
PARTITION BY intDiv(slot, 1000000);
```

### Slots Table

```sql
CREATE TABLE slot (
    slot UInt64,
    parent Nullable(UInt64),
    status Enum8(
        'processed' = 1,
        'confirmed' = 2,
        'rooted' = 3,
        'first_shred_received' = 4,
        'completed' = 5,
        'created_bank' = 6,
        'dead' = 7
    ),
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY slot
PARTITION BY intDiv(slot, 1000000);
```

### Blocks Table

```sql
CREATE TABLE block (
    slot UInt64,
    blockhash String,
    block_time Nullable(DateTime64(3)),
    block_height Nullable(UInt64),
    parent_slot UInt64,
    rewards_pubkey Array(FixedString(32)),
    rewards_lamports Array(Int64),
    rewards_post_balance Array(Int64),
    rewards_reward_type Array(Nullable(Enum8('Unknown' = 0, 'Fee' = 1, 'Rent' = 2, 'Staking' = 3, 'Voting' = 4))),
    rewards_commission Array(Nullable(Int16)),
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY slot
PARTITION BY intDiv(slot, 1000000);
```

**Schema Notes**:
- `FixedString(N)` for binary data (pubkeys, signatures) - matches Arrow binary format
- `DateTime64(3)` for millisecond-precision timestamps with automatic default
- `ReplacingMergeTree` engine for automatic deduplication on merge (using `updated_at` or `write_version`)
- Partitioning by slot ranges for efficient data management and querying
- Nested columns (transactions) or parallel arrays (blocks) for one-to-many relationships
- Bloom filter index on transaction signatures for fast lookup

**Bonus**: The migrations include a `balance_change` view that flattens the nested balance changes for easier querying:

```sql
CREATE VIEW balance_change AS
SELECT
    signature,
    slot,
    tx_index,
    balance_changes.account AS account,
    balance_changes.account_index AS account_index,
    balance_changes.pre_balance AS pre_balance,
    balance_changes.post_balance AS post_balance,
    balance_changes.updated_at AS updated_at
FROM geyser.transaction
ARRAY JOIN balance_changes;
```

## Database Setup

Jacana uses [goose](https://github.com/pressly/goose) for database migrations. A `justfile` is provided for convenience:

```bash
# Install goose (if needed)
go install github.com/pressly/goose/v3/cmd/goose@latest

# Configure your ClickHouse connection
export CLICKHOUSE_URL='clickhouse://localhost:9000/default?username=default&password=P%40ssword'

# Apply all migrations
just migrate-up

# Check migration status
just migrate-status

# Rollback last migration
just migrate-down

# Create new migration
just migrate-create my_new_migration
```