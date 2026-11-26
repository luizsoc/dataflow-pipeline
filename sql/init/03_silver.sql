-- SILVER: cleaned, typed and deduplicated data.

-- Working area rebuilt on every run (TRUNCATE + INSERT). Quality checks run
-- here before anything reaches silver.sales_transactions.
-- UNLOGGED: no WAL writes, faster; its content is disposable by design.
CREATE UNLOGGED TABLE IF NOT EXISTS silver.sales_staging (
    line_key         CHAR(32)      NOT NULL,
    invoice_no       VARCHAR(20)   NOT NULL,
    stock_code       VARCHAR(20)   NOT NULL,
    description      TEXT,
    quantity         INTEGER       NOT NULL,
    unit_price       NUMERIC(10, 2) NOT NULL,
    invoice_ts       TIMESTAMP     NOT NULL,
    customer_id      INTEGER,
    country          VARCHAR(60)   NOT NULL,
    is_cancellation  BOOLEAN       NOT NULL,
    batch_id         UUID          NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.sales_transactions (
    -- Deterministic hash of the business columns: identical lines share a key,
    -- so exact duplicates collapse and re-runs upsert instead of inserting
    line_key         CHAR(32)       PRIMARY KEY,
    invoice_no       VARCHAR(20)    NOT NULL,
    stock_code       VARCHAR(20)    NOT NULL,
    description      TEXT,
    quantity         INTEGER        NOT NULL CHECK (quantity <> 0),
    unit_price       NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    invoice_ts       TIMESTAMP      NOT NULL,
    customer_id      INTEGER,
    country          VARCHAR(60)    NOT NULL,
    is_cancellation  BOOLEAN        NOT NULL,
    line_total       NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    batch_id         UUID           NOT NULL REFERENCES bronze.ingestion_batches (batch_id),
    processed_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_sales_transactions_invoice_ts
    ON silver.sales_transactions (invoice_ts);
CREATE INDEX IF NOT EXISTS ix_sales_transactions_customer_id
    ON silver.sales_transactions (customer_id);

-- Quarantine: rows rejected by cleaning rules, kept with the reason.
CREATE TABLE IF NOT EXISTS silver.sales_rejected (
    raw_id            BIGINT      PRIMARY KEY REFERENCES bronze.online_retail_raw (raw_id),
    batch_id          UUID        NOT NULL REFERENCES bronze.ingestion_batches (batch_id),
    rejection_reason  TEXT        NOT NULL,
    rejected_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_sales_rejected_reason
    ON silver.sales_rejected (rejection_reason);
