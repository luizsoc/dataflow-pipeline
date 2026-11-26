-- Top 10 products by net revenue, with their share of the total.
WITH product_revenue AS (
    SELECT
        p.stock_code,
        p.description,
        SUM(f.quantity)   AS units_sold,
        SUM(f.line_total) AS net_revenue
    FROM gold.fact_sales AS f
    JOIN gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY p.stock_code, p.description
)
SELECT
    RANK() OVER (ORDER BY net_revenue DESC) AS revenue_rank,
    stock_code,
    description,
    units_sold,
    net_revenue,
    ROUND(100.0 * net_revenue / SUM(net_revenue) OVER (), 2) AS revenue_share_pct
FROM product_revenue
ORDER BY net_revenue DESC
LIMIT 10;
