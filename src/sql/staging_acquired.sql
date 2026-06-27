-- Stage and normalize acquired PostgreSQL source rows.
CREATE OR REPLACE VIEW DE_INTEGRATION.RAW.STG_ACQUIRED_SALES_TRANSFORMED AS
WITH ranked_sales AS (
    SELECT
        id,
        sale_date,
        customer_id,
        product_sku,
        total_price,
        currency,
        order_status,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
    FROM DE_INTEGRATION.RAW.STG_ACQUIRED_SALES
)
SELECT
    'acquired' AS source_system,
    s.id AS source_transaction_id,
    'acquired:' || CAST(s.id AS VARCHAR) AS warehouse_transaction_id,
    COALESCE(
        TRY_TO_TIMESTAMP(s.sale_date, 'YYYY-MM-DD HH24:MI:SS'),
        TRY_TO_TIMESTAMP(s.sale_date, 'DD/MM/YYYY HH24:MI:SS'),
        TRY_TO_TIMESTAMP(s.sale_date, 'YYYY/MM/DD HH24:MI:SS')
    ) AS checkout_timestamp,
    s.customer_id,
    CAST(NULL AS VARCHAR) AS customer_country,
    REGEXP_REPLACE(s.product_sku, '^(PROD-|SKU-)', '') AS sku,
    CASE
        WHEN er.rate_to_usd IS NOT NULL THEN s.total_price * er.rate_to_usd
        ELSE s.total_price
    END AS gross_amount_usd,
    0.00 AS tax_amount_usd,
    FALSE AS is_refunded,
    CASE
        WHEN s.rn > 1 THEN 'duplicate_source_transaction'
        WHEN s.product_sku IS NULL OR TRIM(s.product_sku) = '' THEN 'missing_sku'
        WHEN er.rate_to_usd IS NULL THEN 'unknown_currency'
        ELSE NULL
    END AS reject_reason
FROM ranked_sales s
LEFT JOIN DE_INTEGRATION.RAW.STG_EXCHANGE_RATES er
    ON s.currency = er.currency;
