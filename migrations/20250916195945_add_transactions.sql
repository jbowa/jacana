-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS geyser.transaction
(
    signature FixedString(64),
    slot UInt64,
    tx_index UInt64,
    is_vote Bool,
    message_type UInt8,
    success Bool,
    fee UInt64,
    updated_at DateTime64(3) DEFAULT now64(3),
    balance_changes Nested(
        account FixedString(32),
        account_index UInt8,
        pre_balance UInt64,
        post_balance UInt64,
        updated_at DateTime64(3)
    )
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (slot, tx_index)
PARTITION BY intDiv(slot, 1000000)
SETTINGS index_granularity = 8192;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE VIEW IF NOT EXISTS geyser.balance_change AS
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_transaction_sig
    ON geyser.transaction (signature)
    TYPE bloom_filter
    GRANULARITY 4;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS geyser.balance_change;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS geyser.transaction;
-- +goose StatementEnd