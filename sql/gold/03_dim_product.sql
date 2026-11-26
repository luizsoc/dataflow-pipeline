-- One row per stock code. A code can appear with several descriptions over
-- time; the most frequent one wins (ties: alphabetical, so it is deterministic).
WITH counted AS (
    SELECT stock_code, description, count(*) AS lines
    FROM silver.sales_transactions
    GROUP BY stock_code, description
),
best AS (
    SELECT DISTINCT ON (stock_code) stock_code, description
    FROM counted
    ORDER BY stock_code, (description IS NULL), lines DESC, description
)
INSERT INTO gold.dim_product (stock_code, description)
SELECT stock_code, description
FROM best
ON CONFLICT (stock_code) DO UPDATE
    SET description = EXCLUDED.description
    -- Skip the write when nothing changed: re-runs do not rewrite every row
    WHERE gold.dim_product.description IS DISTINCT FROM EXCLUDED.description;
