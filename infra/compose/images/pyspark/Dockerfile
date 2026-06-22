FROM python:3.11-slim

ARG PYSPARK_VERSION

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONPATH=/workspace/src:/workspace/res

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash openjdk-17-jre-headless procps tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    click==8.1.8 \
    pytest==8.4.2 \
    rich==13.9.4 \
    toml==0.10.2 \
    "pyspark==${PYSPARK_VERSION}"

WORKDIR /workspace
ENTRYPOINT ["tini", "--"]
