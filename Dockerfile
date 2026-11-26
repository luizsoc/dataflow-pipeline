ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Install project dependencies pinned against the official Airflow constraints,
# so they never upgrade or break a package Airflow depends on.
# requirements-dev.txt adds pytest and ruff, so tests run inside the container
# with the exact same Python and libraries as the pipeline.
COPY requirements.txt requirements-dev.txt /tmp/
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/requirements-dev.txt \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Project code is bind-mounted at runtime (see docker-compose.yml);
# this makes the `dataflow` package importable from DAGs and tests.
ENV PYTHONPATH=/opt/airflow/src
