-- +goose Up
-- +goose StatementBegin
CREATE DATABASE IF NOT EXISTS geyser;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS geyser.token
(
    mint FixedString(32),
    decimals UInt8,
    supply UInt128,
    mint_authority Nullable(FixedString(32)),
    freeze_authority Nullable(FixedString(32)),
    close_authority Nullable(FixedString(32)),
    is_initialized Bool,
    slot UInt64,
    created_at DateTime64(3) DEFAULT now64(3),
    updated_at DateTime64(3) DEFAULT now64(3),

    -- Compressed metadata with ZSTD
    name Nullable(String) CODEC(ZSTD),
    symbol Nullable(String) CODEC(ZSTD), 
    uri Nullable(String) CODEC(ZSTD),
    token_standard Enum8('fungible' = 1, 'non_fungible' = 2, 'fungible_asset' = 3) DEFAULT 'fungible'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY mint
PARTITION BY toYYYYMM(created_at)
SETTINGS index_granularity = 8192;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE MATERIALIZED VIEW IF NOT EXISTS geyser.token_by_time
(
    mint FixedString(32),
    created_at DateTime64(3),
    slot UInt64,
    supply UInt128,
    token_standard Enum8('fungible' = 1, 'non_fungible' = 2, 'fungible_asset' = 3),
    decimals UInt8
)
ENGINE = MergeTree()
ORDER BY (created_at, mint)
PARTITION BY toYYYYMM(created_at)
AS SELECT
    mint,
    created_at, 
    slot,
    supply,
    token_standard,
    decimals
FROM geyser.token;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS geyser.token_by_time;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS geyser.token;
-- +goose StatementEnd

-- +goose StatementBegin
DROP DATABASE IF EXISTS geyser;
-- +goose StatementEnd