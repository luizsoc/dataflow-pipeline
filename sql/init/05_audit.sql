-- AUDIT: traceability of every pipeline run and every quality check.

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_step_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_run_id    TEXT        NOT NULL,
    task_id       TEXT        NOT NULL,
    batch_id      UUID,
    status        VARCHAR(10) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    rows_read     INTEGER     CHECK (rows_read >= 0),
    rows_written  INTEGER     CHECK (rows_written >= 0),
    error_message TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_dag_run_id ON audit.pipeline_runs (dag_run_id);

CREATE TABLE IF NOT EXISTS audit.quality_checks (
    check_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_run_id    TEXT        NOT NULL,
    batch_id      UUID,
    check_name    TEXT        NOT NULL,
    layer         VARCHAR(10) NOT NULL CHECK (layer IN ('staging', 'silver', 'gold')),
    severity      VARCHAR(10) NOT NULL CHECK (severity IN ('critical', 'warning')),
    passed        BOOLEAN     NOT NULL,
    failed_rows   INTEGER     NOT NULL DEFAULT 0 CHECK (failed_rows >= 0),
    details       TEXT,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_quality_checks_dag_run_id ON audit.quality_checks (dag_run_id);
