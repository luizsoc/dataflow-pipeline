-- 'Unknown' is the pre-seeded member -1, so it is skipped by the conflict.
INSERT INTO gold.dim_country (country_name)
SELECT DISTINCT country
FROM silver.sales_transactions
ON CONFLICT (country_name) DO NOTHING;
