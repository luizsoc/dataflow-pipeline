-- One row per calendar day covered by the sales. Dates never change: DO NOTHING.
INSERT INTO gold.dim_date (
    date_key, full_date, year, quarter, month, month_name, day, day_of_week, is_weekend
)
SELECT
    to_char(d, 'YYYYMMDD')::integer,
    d::date,
    EXTRACT(YEAR FROM d)::smallint,
    EXTRACT(QUARTER FROM d)::smallint,
    EXTRACT(MONTH FROM d)::smallint,
    to_char(d, 'FMMonth'),
    EXTRACT(DAY FROM d)::smallint,
    EXTRACT(ISODOW FROM d)::smallint,
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(
    (SELECT min(invoice_ts)::date FROM silver.sales_transactions),
    (SELECT max(invoice_ts)::date FROM silver.sales_transactions),
    INTERVAL '1 day'
) AS d
ON CONFLICT (date_key) DO NOTHING;
