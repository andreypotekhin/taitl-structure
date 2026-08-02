FROM python:3.11-slim-bookworm

ARG PYSPARK_VERSION
ARG SEDONA_VERSION=1.9.0

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONPATH=/workspace/src:/workspace/res

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash openjdk-17-jre-headless procps tini \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/local/openjdk-17 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/local/openjdk-17

RUN pip install --no-cache-dir \
    click==8.1.8 \
    pytest==8.4.2 \
    rich==13.9.4 \
    toml==0.10.2 \
    "pyspark[connect]==${PYSPARK_VERSION}" \
    "apache-sedona==${SEDONA_VERSION}"

COPY infra/compose/images/pyspark/log4j2-integration.properties /etc/spark/log4j2-integration.properties
COPY infra/compose/images/pyspark/run-integration.sh /usr/local/bin/run-integration
RUN chmod +x /usr/local/bin/run-integration

WORKDIR /workspace
ENTRYPOINT ["tini", "--"]
