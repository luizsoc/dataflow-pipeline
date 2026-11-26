-- Average order value per identified customer, and where each customer ranks.
WITH orders AS (
    SELECT f.customer_key, f.invoice_no, SUM(f.line_total) AS order_value
    FROM gold.fact_sales AS f
    WHERE NOT f.is_cancellation
      AND f.customer_key <> -1
    GROUP BY f.customer_key, f.invoice_no
),
per_customer AS (
    SELECT
        customer_key,
        COUNT(*)                     AS orders,
        ROUND(AVG(order_value), 2)   AS avg_order_value,
        SUM(order_value)             AS lifetime_revenue
    FROM orders
    GROUP BY customer_key
)
SELECT
    c.customer_id,
    pc.orders,
    pc.avg_order_value,
    pc.lifetime_revenue,
    NTILE(10) OVER (ORDER BY pc.lifetime_revenue DESC) AS revenue_decile
FROM per_customer AS pc
JOIN gold.dim_customer AS c ON c.customer_key = pc.customer_key
ORDER BY pc.lifetime_revenue DESC
LIMIT 20;
