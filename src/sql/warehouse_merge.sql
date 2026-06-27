-- Merge staged rows into final Snowflake fact and reject tables.
CREATE TABLE IF NOT EXISTS DE_INTEGRATION.ANALYTICS.SALES_FACT (
    source_system VARCHAR,
    source_transaction_id INTEGER,
    warehouse_transaction_id VARCHAR PRIMARY KEY,
    checkout_timestamp TIMESTAMP,
    customer_id INTEGER,
    customer_country VARCHAR,
    sku VARCHAR,
    gross_amount_usd NUMERIC(18, 2),
    tax_amount_usd NUMERIC(18, 2),
    is_refunded BOOLEAN
);

CREATE TABLE IF NOT EXISTS DE_INTEGRATION.ANALYTICS.SALES_REJECTS (
    source_system VARCHAR,
    source_transaction_id INTEGER,
    reject_reason VARCHAR
);

-- UPSERT fact table records
MERGE INTO DE_INTEGRATION.ANALYTICS.SALES_FACT AS target
USING (
    SELECT
        source_system,
        source_transaction_id,
        warehouse_transaction_id,
        checkout_timestamp,
        customer_id,
        customer_country,
        sku,
        gross_amount_usd,
        tax_amount_usd,
        is_refunded
    FROM DE_INTEGRATION.RAW.STG_CORE_SALES_TRANSFORMED
    WHERE reject_reason IS NULL
    UNION ALL
    SELECT
        source_system,
        source_transaction_id,
        warehouse_transaction_id,
        checkout_timestamp,
        customer_id,
        customer_country,
        sku,
        gross_amount_usd,
        tax_amount_usd,
        is_refunded
    FROM DE_INTEGRATION.RAW.STG_ACQUIRED_SALES_TRANSFORMED
    WHERE reject_reason IS NULL
) AS source
ON target.warehouse_transaction_id = source.warehouse_transaction_id
WHEN MATCHED THEN
    UPDATE SET
        checkout_timestamp = source.checkout_timestamp,
        customer_id = source.customer_id,
        customer_country = source.customer_country,
        sku = source.sku,
        gross_amount_usd = source.gross_amount_usd,
        tax_amount_usd = source.tax_amount_usd,
        is_refunded = source.is_refunded
WHEN NOT MATCHED THEN
    INSERT (
        source_system,
        source_transaction_id,
        warehouse_transaction_id,
        checkout_timestamp,
        customer_id,
        customer_country,
        sku,
        gross_amount_usd,
        tax_amount_usd,
        is_refunded
    ) VALUES (
        source.source_system,
        source.source_transaction_id,
        source.warehouse_transaction_id,
        source.checkout_timestamp,
        source.customer_id,
        source.customer_country,
        source.sku,
        source.gross_amount_usd,
        source.tax_amount_usd,
        source.is_refunded
    );

-- Delete old rejects to prevent duplicates for current batch
DELETE FROM DE_INTEGRATION.ANALYTICS.SALES_REJECTS
WHERE (source_system = 'core' AND source_transaction_id IN (SELECT tx_id FROM DE_INTEGRATION.RAW.STG_CORE_SALES))
   OR (source_system = 'acquired' AND source_transaction_id IN (SELECT id FROM DE_INTEGRATION.RAW.STG_ACQUIRED_SALES));

-- Insert new rejects
INSERT INTO DE_INTEGRATION.ANALYTICS.SALES_REJECTS (
    source_system,
    source_transaction_id,
    reject_reason
)
SELECT source_system, source_transaction_id, reject_reason
FROM DE_INTEGRATION.RAW.STG_CORE_SALES_TRANSFORMED
WHERE reject_reason IS NOT NULL
UNION ALL
SELECT source_system, source_transaction_id, reject_reason
FROM DE_INTEGRATION.RAW.STG_ACQUIRED_SALES_TRANSFORMED
WHERE reject_reason IS NOT NULL;
