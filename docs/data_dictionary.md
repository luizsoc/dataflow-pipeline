# Data Dictionary

All tables live in the `dataflow` PostgreSQL database. DDL: [`sql/init/`](../sql/init).

## bronze.ingestion_batches

One row per source file loaded. The unique hash makes ingestion idempotent.

| Column | Type | Description |
| --- | --- | --- |
| `batch_id` | UUID, PK | Identifier of the load; carried by every downstream row |
| `source_url` | TEXT | URL the file was downloaded from |
| `file_name` | TEXT | Name of the downloaded file |
| `file_sha256` | CHAR(64), UNIQUE | SHA-256 of the file; a known hash is never loaded again |
| `row_count` | INTEGER | Rows read from the file |
| `extracted_at` | TIMESTAMPTZ | When the file was downloaded |
| `loaded_at` | TIMESTAMPTZ | When the batch was written to Bronze |

## bronze.online_retail_raw

Every source row exactly as received. All source columns are TEXT, so no row is lost to a type error.

| Column | Type | Description |
| --- | --- | --- |
| `raw_id` | BIGINT identity, PK | Row identifier, referenced by the quarantine table |
| `batch_id` | UUID, FK | Batch the row belongs to |
| `source_sheet` | TEXT | Excel sheet name (`Year 2009-2010` or `Year 2010-2011`) |
| `source_row` | INTEGER | Row number in the sheet (row 1 is the header) |
| `invoice` | TEXT | Source `Invoice` |
| `stock_code` | TEXT | Source `StockCode` |
| `description` | TEXT | Source `Description` |
| `quantity` | TEXT | Source `Quantity` |
| `invoice_date` | TEXT | Source `InvoiceDate` |
| `price` | TEXT | Source `Price` |
| `customer_id` | TEXT | Source `Customer ID` |
| `country` | TEXT | Source `Country` |
| `loaded_at` | TIMESTAMPTZ | Load time |

Unique constraint: (`batch_id`, `source_sheet`, `source_row`).

## silver.sales_staging

Working table (UNLOGGED), truncated and refilled on every run. Quality checks run here before
anything reaches `sales_transactions`. Same business columns as `silver.sales_transactions`, without
the computed and audit columns.

## silver.sales_transactions

Clean, typed and deduplicated sales lines.

| Column | Type | Description |
| --- | --- | --- |
| `line_key` | CHAR(32), PK | MD5 of invoice, stock code, quantity, price, timestamp and customer; identical lines share it |
| `invoice_no` | VARCHAR(20) | Invoice number, upper-cased |
| `stock_code` | VARCHAR(20) | Product code, upper-cased |
| `description` | TEXT | Product name, trimmed and upper-cased; may be null |
| `quantity` | INTEGER, CHECK <> 0 | Units; negative for cancellations |
| `unit_price` | NUMERIC(10,2), CHECK >= 0 | Unit price in GBP |
| `invoice_ts` | TIMESTAMP | Invoice date and time |
| `customer_id` | INTEGER, nullable | Customer number; null for about 23% of lines |
| `country` | VARCHAR(60) | Standardised country name; `Unknown` when missing |
| `is_cancellation` | BOOLEAN | True when the invoice starts with `C` |
| `line_total` | NUMERIC(12,2), generated | `quantity * unit_price`, computed by PostgreSQL |
| `batch_id` | UUID, FK | Batch the line came from |
| `processed_at` | TIMESTAMPTZ | Last time the line was written |

## silver.sales_rejected

Quarantine: Bronze rows that failed a cleaning rule.

| Column | Type | Description |
| --- | --- | --- |
| `raw_id` | BIGINT, PK, FK | The rejected Bronze row |
| `batch_id` | UUID, FK | Batch of the row |
| `rejection_reason` | TEXT | `duplicate_line`, `non_positive_price`, `non_product_code`, `missing_required_field`, `invalid_quantity`, `zero_quantity`, `invalid_price` or `invalid_date` |
| `rejected_at` | TIMESTAMPTZ | When the row was quarantined |

## gold.fact_sales

Grain: one invoice line.

| Column | Type | Description |
| --- | --- | --- |
| `sales_key` | BIGINT identity, PK | Surrogate key |
| `line_key` | CHAR(32), UNIQUE | Natural key from Silver; used for the upsert |
| `invoice_no` | VARCHAR(20) | Degenerate dimension |
| `date_key` | INTEGER, FK | `dim_date` |
| `product_key` | INTEGER, FK | `dim_product` |
| `customer_key` | INTEGER, FK | `dim_customer`; `-1` when the sale has no customer |
| `country_key` | INTEGER, FK | `dim_country` |
| `quantity` | INTEGER | Units; negative for cancellations |
| `unit_price` | NUMERIC(10,2) | Unit price in GBP |
| `line_total` | NUMERIC(12,2) | Line revenue in GBP; negative for cancellations |
| `is_cancellation` | BOOLEAN | Cancellation flag |

## gold.dim_date

| Column | Type | Description |
| --- | --- | --- |
| `date_key` | INTEGER, PK | `YYYYMMDD` |
| `full_date` | DATE, UNIQUE | Calendar date |
| `year`, `quarter`, `month`, `day` | SMALLINT | Calendar parts |
| `month_name` | VARCHAR(10) | `January` ... `December` |
| `day_of_week` | SMALLINT | ISO day, 1 = Monday ... 7 = Sunday |
| `is_weekend` | BOOLEAN | Saturday or Sunday |

## gold.dim_product

| Column | Type | Description |
| --- | --- | --- |
| `product_key` | INTEGER identity, PK | Surrogate key |
| `stock_code` | VARCHAR(20), UNIQUE | Natural key |
| `description` | TEXT | Most frequent description seen for the code |

## gold.dim_customer

| Column | Type | Description |
| --- | --- | --- |
| `customer_key` | INTEGER identity, PK | Surrogate key; `-1` is the Unknown member |
| `customer_id` | INTEGER, UNIQUE | Natural key; null only for the Unknown member |
| `first_purchase_date` | DATE | First invoice date |
| `last_purchase_date` | DATE | Last invoice date |

## gold.dim_country

| Column | Type | Description |
| --- | --- | --- |
| `country_key` | INTEGER identity, PK | Surrogate key; `-1` is `Unknown` |
| `country_name` | VARCHAR(60), UNIQUE | Standardised country name |

## audit.pipeline_runs

One row per task execution, written by Airflow callbacks.

| Column | Type | Description |
| --- | --- | --- |
| `run_step_id` | BIGINT identity, PK | Row identifier |
| `dag_run_id` | TEXT | Airflow run ID |
| `task_id` | TEXT | Task name |
| `batch_id` | UUID | Batch processed, when known |
| `status` | VARCHAR(10) | `success`, `failed` or `skipped` |
| `rows_read`, `rows_written` | INTEGER | Volume handled by the task |
| `error_message` | TEXT | Exception type and message on failure |
| `started_at`, `finished_at` | TIMESTAMPTZ | Task timing |

## audit.quality_checks

One row per quality check per run.

| Column | Type | Description |
| --- | --- | --- |
| `check_id` | BIGINT identity, PK | Row identifier |
| `dag_run_id` | TEXT | Airflow run ID |
| `batch_id` | UUID | Batch checked |
| `check_name` | TEXT | Check identifier, e.g. `line_key_unique` |
| `layer` | VARCHAR(10) | `staging`, `silver` or `gold` |
| `severity` | VARCHAR(10) | `critical` (blocks the pipeline) or `warning` |
| `passed` | BOOLEAN | Result |
| `failed_rows` | INTEGER | Rows failing the check |
| `details` | TEXT | What the check verifies |
| `checked_at` | TIMESTAMPTZ | When it ran |
