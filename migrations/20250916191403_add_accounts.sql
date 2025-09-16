-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS geyser.slot
(
    slot UInt64,
    parent Nullable(UInt64),
    status Enum8('processed' = 1, 'confirmed' = 2, 'finalized' = 3),
    updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY slot
PARTITION BY intDiv(slot, 1000000)
SETTINGS index_granularity = 8192;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS geyser.block
(
    slot UInt64,
    blockhash String,
    block_time Nullable(UInt64),
    block_height Nullable(UInt64),
    updated_at DateTime64(3) DEFAULT now64(3),
    
    rewards_pubkey Array(FixedString(32)),
    rewards_lamports Array(Int64),
    rewards_post_balance Array(UInt64),
    rewards_reward_type Array(Enum8('Fee' = 1, 'Rent' = 2, 'Staking' = 3, 'Voting' = 4)),
    rewards_commission Array(Nullable(UInt16))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY slot
PARTITION BY intDiv(slot, 1000000)
SETTINGS index_granularity = 8192;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS geyser.account
(
    pubkey FixedString(32),
    owner FixedString(32),
    lamports UInt64,
    slot UInt64,
    executable Bool,
    rent_epoch UInt64,
    data String,
    updated_at DateTime64(3) DEFAULT now64(3),
    txn_signature Nullable(FixedString(64))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY pubkey
PARTITION BY cityHash64(pubkey) % 256
SETTINGS index_granularity = 8192;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS geyser.account;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS geyser.block;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS geyser.slot;
-- +goose StatementEnd