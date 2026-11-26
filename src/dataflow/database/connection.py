"""PostgreSQL connection management for the warehouse."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2
import psycopg2.extensions

from dataflow.config import WarehouseSettings
from dataflow.logging_config import kv

logger = logging.getLogger(__name__)


@contextmanager
def get_connection(
    settings: WarehouseSettings | None = None,
) -> Iterator[psycopg2.extensions.connection]:
    """Yield a connection that commits on success and rolls back on any error.

    Using one transaction per block means a failed step never leaves a
    half-written table behind.
    """
    settings = settings or WarehouseSettings.from_env()
    conn = psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
        connect_timeout=10,
        application_name="dataflow-pipeline",
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception(kv(event="transaction_rolled_back", db=settings.dbname))
        raise
    finally:
        conn.close()
