# One image for every client. Clients are parameters, not images.
FROM prefecthq/prefect:3.8.8-python3.11

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
COPY clients ./clients
RUN pip install --no-cache-dir -e .

ENV PIPELINES_DATA_DIR=/data \
    PREFECT_LOGGING_EXTRA_LOGGERS=pipelines \
    PREFECT_RESULTS_PERSIST_BY_DEFAULT=true \
    PREFECT_LOCAL_STORAGE_PATH=/data/_prefect_results
