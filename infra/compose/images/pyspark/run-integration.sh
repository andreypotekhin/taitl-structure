#!/usr/bin/env bash
set -euo pipefail

backend="${1:?integration backend is required}"
connect_pid=""
connect_log=""

mkdir -p /tmp/artifacts /tmp/spark-artifacts
cd /tmp

if [[ "${backend}" == spark-connect* ]]; then
    : "${STRUCTURE_EXPECTED_SPARK:?STRUCTURE_EXPECTED_SPARK is required for Spark Connect integration}"

    connect_port="${STRUCTURE_SPARK_CONNECT_PORT:-15002}"
    connect_master="${STRUCTURE_SPARK_CONNECT_MASTER:-${STRUCTURE_SPARK_MASTER:-local[2]}}"
    export STRUCTURE_SPARK_REMOTE="${STRUCTURE_SPARK_REMOTE:-sc://127.0.0.1:${connect_port}}"

    connect_args=(
        --master "${connect_master}"
        --driver-memory "${STRUCTURE_SPARK_CONNECT_DRIVER_MEMORY:-3g}"
        --class org.apache.spark.sql.connect.service.SparkConnectServer
        --conf "spark.connect.grpc.binding.port=${connect_port}"
        --conf "spark.connect.grpc.binding.address=127.0.0.1"
        --conf "spark.sql.shuffle.partitions=1"
        --conf "spark.sql.session.timeZone=UTC"
        --conf "spark.sql.artifact.dir=/tmp/spark-artifacts"
    )

    connect_jars=("${SPARK_HOME}"/jars/spark-connect_*.jar)
    if [[ -e "${connect_jars[0]}" ]]; then
        connect_args+=("${connect_jars[0]}")
    else
        scala_version="${SPARK_CONNECT_SCALA_VERSION:-2.12}"
        connect_args+=(--packages "org.apache.spark:spark-connect_${scala_version}:${STRUCTURE_EXPECTED_SPARK}")
    fi

    connect_log="/tmp/spark-connect-server.log"
    SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-} -Dlog4j.configurationFile=file:/etc/spark/log4j2-integration.properties" \
        spark-submit "${connect_args[@]}" >"${connect_log}" 2>&1 &
    connect_pid="$!"
fi

cleanup() {
    if [[ -n "${connect_pid}" ]]; then
        kill "${connect_pid}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

pytest_status=0
python -m pytest /workspace/tests/integration /workspace/tests/concepts/live_pyspark \
    --rootdir=/workspace \
    -p no:cacheprovider \
    --run-integration \
    "--integration-backend=${backend}" \
    -W 'ignore:distutils Version classes are deprecated:DeprecationWarning' \
    -W 'ignore:The copy keyword is deprecated:Warning' \
    -W 'ignore:ReleaseExecute failed with exception:UserWarning' \
    ${INTEGRATION_PYTEST_ARGS:-} || pytest_status=$?

if (( pytest_status != 0 )) && [[ -n "${connect_log}" && -f "${connect_log}" ]]; then
    echo "Spark Connect server output (last 200 lines):" >&2
    tail -n 200 "${connect_log}" >&2
fi

exit "${pytest_status}"
