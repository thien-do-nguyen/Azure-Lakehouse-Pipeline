ARG PYTHON_IMAGE=python:3.12-slim-bookworm

FROM ${PYTHON_IMAGE} AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --upgrade pip \
    && python -m pip wheel --wheel-dir /wheels .

FROM ${PYTHON_IMAGE} AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /wheels /wheels
COPY . .

RUN python -m pip install --no-cache-dir /wheels/*.whl \
    "adlfs>=2024.7.0" \
    "azure-identity>=1.17.0" \
    "azure-keyvault-secrets>=4.8.0"

ENTRYPOINT ["python", "-m", "ecommerce_pipeline.jobs.run_batch"]
CMD ["--config", "configs/local.yaml"]
