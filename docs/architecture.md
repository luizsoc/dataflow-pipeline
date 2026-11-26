# Architecture Decisions

Each decision below records the choice, the alternative that was rejected, and why.

| # | Decision | Alternative | Reason |
| --- | --- | --- | --- |
| 1 | UCI Online Retail II as the source | Olist (Kaggle) | Downloads without credentials, so extraction is fully automated; real, documented data quality issues |
| 2 | Pandas for Bronze to Silver | Apache Spark | ~1M rows fit in memory; Spark would add a cluster and complexity with no gain at this volume |
| 3 | SQL inside PostgreSQL for Gold | Pandas | Set-based joins and aggregations run where the data lives; no data leaves the database |
| 4 | Staging table between tasks | XCom or temporary files | XCom is for small metadata; a table can be validated with SQL before promotion |
| 5 | Upsert on natural keys (`ON CONFLICT`) with `IS DISTINCT FROM` | `TRUNCATE` and full reload | Idempotent; surrogate keys stay stable; unchanged rows are not rewritten |
| 6 | Bronze columns all `TEXT` | Typing at ingestion | No row is ever lost to a type error; any rule change can be replayed from Bronze |
| 7 | File hash as ingestion key | Load date | The same file is never loaded twice, whatever the date or the number of retries |
| 8 | Quarantine table with a reason per row | Dropping invalid rows | Every rejection is auditable and countable |
| 9 | Custom SQL quality checks with severity | Great Expectations | Fewer dependencies; each check is one readable query |
| 10 | Critical checks fail without retry | Default retries | Retrying cannot fix bad data; it only delays the alert |
| 11 | 4xx download errors fail immediately; 5xx and timeouts retry | Retry everything | A wrong URL will not fix itself |
| 12 | Unknown member (`-1`) in `dim_customer` and `dim_country` | Dropping sales without a customer | Keeps total revenue correct (about 23% of lines have no customer) |
| 13 | SCD Type 1 dimensions | SCD Type 2 | The dataset has almost no attribute history; Type 2 is a documented next step |
| 14 | Separate PostgreSQL for Airflow metadata and the warehouse | One database | Isolates operational metadata from analytical data, as in production setups |
| 15 | LocalExecutor | CeleryExecutor | A single machine needs no Redis or worker fleet |
| 16 | Audit records written on a separate connection | Inside the task transaction | A failure is still recorded when the task's own transaction rolls back |

## Idempotency, end to end

Running the DAG twice produces exactly the same warehouse. This was verified on the full dataset:
the second run records `load_to_bronze = skipped` and `rows_written = 0` for Silver and Gold in
`audit.pipeline_runs`.

| Layer | Mechanism |
| --- | --- |
| Raw file | Atomic write (temp file + rename); an existing file for the day is reused |
| Bronze | `UNIQUE (file_sha256)`; a known hash skips the load |
| Staging | `TRUNCATE` before every write |
| Quarantine | `ON CONFLICT (raw_id) DO UPDATE` |
| Silver | `ON CONFLICT (line_key) DO UPDATE ... WHERE IS DISTINCT FROM` |
| Gold | Same upsert pattern on each dimension's natural key and on `fact_sales.line_key` |

## Scaling path

What would change at 100 times the volume:

- Incremental loads partitioned by invoice month instead of reprocessing the whole batch.
- Parquet files in object storage (S3) for Bronze instead of a text table.
- Spark or a distributed engine for the Silver transformation.
- dbt models for Gold, with tests and lineage.
- CeleryExecutor or KubernetesExecutor to run tasks in parallel.
