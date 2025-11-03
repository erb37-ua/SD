FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tini && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir confluent-kafka

RUN useradd -m appuser
USER appuser

ENTRYPOINT ["/usr/bin/tini","--"]