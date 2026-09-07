INSERT INTO reconciliation_audit (
    batch_date,
    internal_tx_id,
    provider_tx_id,
    discrepancy_type,
    internal_amount,
    external_amount
)
SELECT 
    CURRENT_DATE AS batch_date,
    it.transaction_id AS internal_tx_id,
    eg.provider_tx_id AS provider_tx_id,
    CASE
        WHEN it.transaction_id IS NULL THEN 'MISSING_IN_INTERNAL_LEDGER'
        WHEN eg.provider_tx_id IS NULL THEN 'MISSING_IN_GATEWAY'
        WHEN it.amount <> eg.cleared_amount THEN 'AMOUNT_MISMATCH'
        ELSE 'MATCHED'
    END AS discrepancy_type,
    it.amount AS internal_amount,
    eg.cleared_amount AS external_amount
FROM internal_transactions it
FULL OUTER JOIN external_gateway_transactions eg
    ON it.transaction_id = eg.internal_reference_id
WHERE it.transaction_id IS NULL 
   OR eg.provider_tx_id IS NULL 
   OR it.amount <> eg.cleared_amount;