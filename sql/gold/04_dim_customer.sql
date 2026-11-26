-- SCD type 1: one row per customer, attributes overwritten with the latest values.
INSERT INTO gold.dim_customer (customer_id, first_purchase_date, last_purchase_date)
SELECT customer_id, min(invoice_ts)::date, max(invoice_ts)::date
FROM silver.sales_transactions
WHERE customer_id IS NOT NULL
GROUP BY customer_id
ON CONFLICT (customer_id) DO UPDATE
    SET first_purchase_date = EXCLUDED.first_purchase_date,
        last_purchase_date  = EXCLUDED.last_purchase_date
    WHERE (gold.dim_customer.first_purchase_date, gold.dim_customer.last_purchase_date)
          IS DISTINCT FROM (EXCLUDED.first_purchase_date, EXCLUDED.last_purchase_date);
