-- Internal ledger transactions
CREATE TABLE IF NOT EXISTS internal_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- External payment provider / bank feed records
CREATE TABLE IF NOT EXISTS external_gateway_transactions (
    provider_tx_id VARCHAR(64) PRIMARY KEY,
    internal_reference_id VARCHAR(64),
    cleared_amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    settlement_status VARCHAR(20) NOT NULL,
    cleared_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Reconciliation audit log
CREATE TABLE IF NOT EXISTS reconciliation_audit (
    audit_id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    internal_tx_id VARCHAR(64),
    provider_tx_id VARCHAR(64),
    discrepancy_type VARCHAR(50) NOT NULL,
    internal_amount NUMERIC(12, 2),
    external_amount NUMERIC(12, 2),
    flagged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_batch_date ON reconciliation_audit(batch_date);