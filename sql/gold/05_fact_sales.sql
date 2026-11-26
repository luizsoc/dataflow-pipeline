-- Grain: one invoice line. Runs after the dimensions, because it looks up their keys.
-- Lines without a customer point to the unknown member (-1), so totals stay complete.
INSERT INTO gold.fact_sales (
    line_key, invoice_no, date_key, product_key, customer_key, country_key,
    quantity, unit_price, line_total, is_cancellation
)
SELECT
    s.line_key,
    s.invoice_no,
    to_char(s.invoice_ts, 'YYYYMMDD')::integer,
    p.product_key,
    COALESCE(c.customer_key, -1),
    COALESCE(co.country_key, -1),
    s.quantity,
    s.unit_price,
    s.line_total,
    s.is_cancellation
FROM silver.sales_transactions AS s
JOIN gold.dim_product AS p ON p.stock_code = s.stock_code
LEFT JOIN gold.dim_customer AS c ON c.customer_id = s.customer_id
LEFT JOIN gold.dim_country AS co ON co.country_name = s.country
ON CONFLICT (line_key) DO UPDATE
    SET date_key        = EXCLUDED.date_key,
        product_key     = EXCLUDED.product_key,
        customer_key    = EXCLUDED.customer_key,
        country_key     = EXCLUDED.country_key,
        quantity        = EXCLUDED.quantity,
        unit_price      = EXCLUDED.unit_price,
        line_total      = EXCLUDED.line_total,
        is_cancellation = EXCLUDED.is_cancellation
    WHERE (gold.fact_sales.date_key, gold.fact_sales.product_key, gold.fact_sales.customer_key,
           gold.fact_sales.country_key, gold.fact_sales.quantity, gold.fact_sales.unit_price,
           gold.fact_sales.line_total, gold.fact_sales.is_cancellation)
          IS DISTINCT FROM
          (EXCLUDED.date_key, EXCLUDED.product_key, EXCLUDED.customer_key,
           EXCLUDED.country_key, EXCLUDED.quantity, EXCLUDED.unit_price,
           EXCLUDED.line_total, EXCLUDED.is_cancellation);
