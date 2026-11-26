-- Monthly revenue: gross sales, cancellations and net, with month-over-month growth.
WITH monthly AS (
    SELECT
        d.year,
        d.month,
        SUM(f.line_total) FILTER (WHERE NOT f.is_cancellation) AS gross_revenue,
        SUM(f.line_total) FILTER (WHERE f.is_cancellation)     AS cancelled_revenue,
        SUM(f.line_total)                                      AS net_revenue,
        COUNT(DISTINCT f.invoice_no) FILTER (WHERE NOT f.is_cancellation) AS orders
    FROM gold.fact_sales AS f
    JOIN gold.dim_date AS d ON d.date_key = f.date_key
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    gross_revenue,
    cancelled_revenue,
    net_revenue,
    orders,
    ROUND(
        100.0 * (net_revenue - LAG(net_revenue) OVER w) / NULLIF(LAG(net_revenue) OVER w, 0), 1
    ) AS net_growth_pct
FROM monthly
WINDOW w AS (ORDER BY year, month)
ORDER BY year, month;
