-- Staging -> Silver upsert. A corrected line overwrites its previous version;
-- an unchanged line is not rewritten, so re-runs report 0 rows changed.
INSERT INTO silver.sales_transactions (
    line_key, invoice_no, stock_code, description, quantity, unit_price,
    invoice_ts, customer_id, country, is_cancellation, batch_id
)
SELECT
    line_key, invoice_no, stock_code, description, quantity, unit_price,
    invoice_ts, customer_id, country, is_cancellation, batch_id
FROM silver.sales_staging
ON CONFLICT (line_key) DO UPDATE SET
    description     = EXCLUDED.description,
    country         = EXCLUDED.country,
    is_cancellation = EXCLUDED.is_cancellation,
    batch_id        = EXCLUDED.batch_id,
    processed_at    = now()
WHERE (silver.sales_transactions.description, silver.sales_transactions.country,
       silver.sales_transactions.is_cancellation, silver.sales_transactions.batch_id)
      IS DISTINCT FROM
      (EXCLUDED.description, EXCLUDED.country, EXCLUDED.is_cancellation, EXCLUDED.batch_id);
