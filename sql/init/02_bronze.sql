-- BRONZE: data exactly as received. Every source column is TEXT so no row
-- is ever lost to a type error; typing happens in Silver.

CREATE TABLE IF NOT EXISTS bronze.ingestion_batches (
    batch_id      UUID        PRIMARY KEY,
    source_url    TEXT        NOT NULL,
    file_name     TEXT        NOT NULL,
    file_sha256   CHAR(64)    NOT NULL,
    row_count     INTEGER     NOT NULL CHECK (row_count >= 0),
    extracted_at  TIMESTAMPTZ NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The same file is never loaded twice (idempotent ingestion)
    CONSTRAINT uq_ingestion_batches_sha256 UNIQUE (file_sha256)
);

CREATE TABLE IF NOT EXISTS bronze.online_retail_raw (
    raw_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id      UUID        NOT NULL REFERENCES bronze.ingestion_batches (batch_id),
    source_sheet  TEXT        NOT NULL,
    source_row    INTEGER     NOT NULL,
    invoice       TEXT,
    stock_code    TEXT,
    description   TEXT,
    quantity      TEXT,
    invoice_date  TEXT,
    price         TEXT,
    customer_id   TEXT,
    country       TEXT,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_online_retail_raw_origin UNIQUE (batch_id, source_sheet, source_row)
);

CREATE INDEX IF NOT EXISTS ix_online_retail_raw_batch_id
    ON bronze.online_retail_raw (batch_id);
