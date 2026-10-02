# Local, offline-capable image: Python 3.13 + OpenJDK 21 headless for PySpark local mode.
FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    CLAIMS_HOME=/app

WORKDIR /app
COPY requirements.txt requirements-dev.txt pyproject.toml README.md ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY config ./config
RUN pip install --no-cache-dir --no-deps . \
    && useradd --create-home lakehouse && chown -R lakehouse /app
USER lakehouse

ENTRYPOINT ["python", "-m", "claims_lakehouse.run"]
CMD ["--batch", "all", "--reset"]
