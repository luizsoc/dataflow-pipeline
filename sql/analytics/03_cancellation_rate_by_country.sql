-- Share of invoices cancelled per country (countries with at least 100 invoices).
SELECT
    co.country_name,
    COUNT(DISTINCT f.invoice_no)                                AS invoices,
    COUNT(DISTINCT f.invoice_no) FILTER (WHERE f.is_cancellation) AS cancelled_invoices,
    ROUND(
        100.0 * COUNT(DISTINCT f.invoice_no) FILTER (WHERE f.is_cancellation)
        / COUNT(DISTINCT f.invoice_no), 2
    ) AS cancellation_rate_pct
FROM gold.fact_sales AS f
JOIN gold.dim_country AS co ON co.country_key = f.country_key
GROUP BY co.country_name
HAVING COUNT(DISTINCT f.invoice_no) >= 100
ORDER BY cancellation_rate_pct DESC;
