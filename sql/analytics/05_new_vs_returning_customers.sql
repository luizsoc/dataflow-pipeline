-- Per month: how many buyers were new (first purchase that month) versus returning.
WITH monthly_buyers AS (
    SELECT DISTINCT
        date_trunc('month', d.full_date)::date AS month,
        f.customer_key
    FROM gold.fact_sales AS f
    JOIN gold.dim_date AS d ON d.date_key = f.date_key
    WHERE NOT f.is_cancellation
      AND f.customer_key <> -1
),
first_month AS (
    SELECT customer_key, MIN(month) AS first_month
    FROM monthly_buyers
    GROUP BY customer_key
)
SELECT
    mb.month,
    COUNT(*) FILTER (WHERE mb.month = fm.first_month) AS new_customers,
    COUNT(*) FILTER (WHERE mb.month > fm.first_month) AS returning_customers,
    ROUND(100.0 * COUNT(*) FILTER (WHERE mb.month > fm.first_month) / COUNT(*), 1)
        AS returning_share_pct
FROM monthly_buyers AS mb
JOIN first_month AS fm ON fm.customer_key = mb.customer_key
GROUP BY mb.month
ORDER BY mb.month;
