-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS geyser.transaction
(
    signature FixedString(64),
    slot UInt64,
    tx_index UInt64,
    is_vote Bool,
    message_type UInt8,
    tx_type Enum8('Buy' = 1, 'Sell' = 2, 'Swap' = 3, 'Transfer' = 4, 'Mint' = 5, 'Burn' = 6),
    signatures Array(FixedString(64)),
    
    fee UInt64,
    error_code Nullable(Enum8(
        'AccountInUse' = 1,
        'AccountLoadedTwice' = 2,
        'AccountNotFound' = 3,
        'ProgramAccountNotFound' = 4,
        'InsufficientFundsForFee' = 5,
        'InvalidAccountForFee' = 6,
        'AlreadyProcessed' = 7,
        'BlockhashNotFound' = 8,
        'InstructionError' = 9,
        'CallChainTooDeep' = 10,
        'MissingSignatureForFee' = 11,
        'InvalidAccountIndex' = 12,
        'SignatureFailure' = 13,
        'InvalidProgramForExecution' = 14,
        'SanitizeFailure' = 15,
        'ClusterMaintenance' = 16,
        'AccountBorrowOutstanding' = 17,
        'WouldExceedMaxAccountCostLimit' = 18,
        'WouldExceedMaxBlockCostLimit' = 19,
        'UnsupportedVersion' = 20,
        'InvalidWritableAccount' = 21,
        'WouldExceedMaxAccountDataCostLimit' = 22,
        'TooManyAccountLocks' = 23,
        'AddressLookupTableNotFound' = 24,
        'InvalidAddressLookupTableOwner' = 25,
        'InvalidAddressLookupTableData' = 26,
        'InvalidAddressLookupTableIndex' = 27,
        'InvalidRentPayingAccount' = 28,
        'WouldExceedMaxVoteCostLimit' = 29,
        'WouldExceedAccountDataBlockLimit' = 30,
        'WouldExceedAccountDataTotalLimit' = 31,
        'DuplicateInstruction' = 32,
        'InsufficientFundsForRent' = 33,
        'MaxLoadedAccountsDataSizeExceeded' = 34,
        'InvalidLoadedAccountsDataSizeLimit' = 35,
        'ResanitizationNeeded' = 36,
        'UnbalancedTransaction' = 37,
        'ProgramExecutionTemporarilyRestricted' = 38,
        'ProgramCacheHitMaxLimit' = 39,
        'CommitCancelled' = 40
    )),
    pre_balance Nullable(UInt64),
    post_balance Nullable(UInt64),
    sol_amount Nullable(Int64),
    
    token_account Nullable(FixedString(32)),
    token_owner Nullable(FixedString(32)),
    token_mint Nullable(FixedString(32)),
    account_index Nullable(UInt16),
    token_pre_amount Nullable(UInt64),
    token_post_amount Nullable(UInt64),
    token_delta Nullable(Int64),
    
    updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (slot, tx_index)
PARTITION BY intDiv(slot, 1000000)
SETTINGS index_granularity = 8192;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS geyser.transaction;
-- +goose StatementEnd