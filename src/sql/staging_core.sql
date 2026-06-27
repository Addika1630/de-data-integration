-- Stage and normalize core SQL Server source rows.
CREATE OR REPLACE VIEW DE_INTEGRATION.RAW.STG_CORE_SALES_TRANSFORMED AS
WITH ranked_sales AS (
    SELECT
        tx_id,
        checkout_timestamp,
        customer_blob,
        sku_code,
        gross_amt_usd,
        tax_amt,
        is_refunded,
        ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY tx_id) AS rn
    FROM DE_INTEGRATION.RAW.STG_CORE_SALES
)
SELECT
    'core' AS source_system,
    tx_id AS source_transaction_id,
    'core:' || CAST(tx_id AS VARCHAR) AS warehouse_transaction_id,
    COALESCE(
        TRY_TO_TIMESTAMP(checkout_timestamp, 'YYYY-MM-DD HH24:MI:SS'),
        TRY_TO_TIMESTAMP(checkout_timestamp, 'YYYY-MM-DD')
    ) AS checkout_timestamp,
    CAST(GET(TRY_PARSE_JSON(customer_blob), 'id') AS INTEGER) AS customer_id,
    CAST(GET(TRY_PARSE_JSON(customer_blob), 'country') AS VARCHAR) AS customer_country,
    REGEXP_REPLACE(sku_code, '^(PROD-|SKU-)', '') AS sku,
    gross_amt_usd AS gross_amount_usd,
    tax_amt AS tax_amount_usd,
    CASE WHEN is_refunded = 1 THEN TRUE ELSE FALSE END AS is_refunded,
    CASE
        WHEN rn > 1 THEN 'duplicate_source_transaction'
        WHEN customer_blob IS NOT NULL AND TRY_PARSE_JSON(customer_blob) IS NULL THEN 'invalid_customer_blob'
        WHEN sku_code IS NULL OR TRIM(sku_code) = '' THEN 'missing_sku'
        ELSE NULL
    END AS reject_reason
FROM ranked_sales;
