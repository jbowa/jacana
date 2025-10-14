-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS geyser.slot
(
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
    block_time DateTime64(3) NULL,
    block_height Nullable(UInt64),
    parent_slot UInt64,
    rewards_pubkey Array(FixedString(32)),
    rewards_lamports Array(Int64),
    rewards_post_balance Array(Int64),
    rewards_reward_type Array(Nullable(Enum8(
        'Unknown' = 0,
        'Fee'     = 1,
        'Rent'    = 2,
        'Staking' = 3,
        'Voting'  = 4
    ))),
    rewards_commission Array(Nullable(Int16)),
    updated_at DateTime64(3) DEFAULT now64(3)
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
    slot UInt64,
    lamports UInt64,
    executable Bool,
    rent_epoch UInt64,
    data String,
    write_version UInt64,
    updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(write_version)
ORDER BY (pubkey, slot)
PARTITION BY intDiv(slot, 10000000)
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