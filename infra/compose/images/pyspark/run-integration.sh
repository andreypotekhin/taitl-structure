#!/usr/bin/env bash
set -euo pipefail

backend="${1:?integration backend is required}"
connect_pid=""
connect_log=""
connect_checkpoints=""

# Include source modules used by pickled UDFs, including with older cached images.
export PYTHONPATH="/workspace:/workspace/src:/workspace/res:/workspace/tests${PYTHONPATH:+:${PYTHONPATH}}"

if [[ "${backend}" != spark-connect* ]]; then
    submit_args=()
    if [[ -n "${STRUCTURE_SPARK_DRIVER_MEMORY:-}" ]]; then
        submit_args+=(--driver-memory "${STRUCTURE_SPARK_DRIVER_MEMORY}")
    fi
    if (( ${#submit_args[@]} > 0 )); then
        printf -v submit_args_string '%q ' "${submit_args[@]}"
        export PYSPARK_SUBMIT_ARGS="${submit_args_string}pyspark-shell"
    fi
fi

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
        --conf "spark.sql.maxPlanStringLength=8192"
        --conf "spark.sql.ui.explainMode=simple"
        --conf "spark.sql.artifact.dir=/tmp/spark-artifacts"
    )

    if [[ "${backend}" == "spark-connect40" || "${backend}" == "spark-connect41" ]]; then
        mkdir -p /workspace/.pytest-workspace-tmp/integration
        connect_checkpoints=$(mktemp -d /workspace/.pytest-workspace-tmp/integration/connect-checkpoints.XXXXXX)
        connect_args+=(--conf "spark.checkpoint.dir=${connect_checkpoints}")
    fi

    connect_jars=("${SPARK_HOME}"/jars/spark-connect_*.jar)
    connect_packages=()
    connect_resource=""
    if [[ -e "${connect_jars[0]}" ]]; then
        connect_resource="${connect_jars[0]}"
    else
        scala_version="${SPARK_CONNECT_SCALA_VERSION:-2.12}"
        connect_packages+=("org.apache.spark:spark-connect_${scala_version}:${STRUCTURE_EXPECTED_SPARK}")
    fi

    # Spark Connect rejects static session options from the client.  Install
    # integration-only extensions on the server instead, where Spark can apply
    # them during session construction (for example, Sedona geometry support).
    if [[ -n "${STRUCTURE_SPARK_JARS_PACKAGES:-}" ]]; then
        connect_packages+=("${STRUCTURE_SPARK_JARS_PACKAGES}")
        if [[ "${STRUCTURE_SPARK_JARS_PACKAGES}" == *sedona* ]]; then
            connect_args+=(--conf "spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions")
        fi
    fi
    if [[ "${backend}" == "spark-connect41" ]]; then
        connect_packages+=(
            "io.delta:delta-connect-server_4.1_2.13:${STRUCTURE_EXPECTED_DELTA:-4.1.0}"
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"
            "com.google.protobuf:protobuf-java:4.33.0"
        )
        connect_args+=(
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
            --conf "spark.sql.catalog.structure_iceberg=org.apache.iceberg.spark.SparkCatalog"
            --conf "spark.sql.catalog.structure_iceberg.type=hadoop"
            --conf "spark.sql.catalog.structure_iceberg.warehouse=file:/workspace/.pytest-workspace-tmp/integration/iceberg-warehouse"
            --conf "spark.connect.extensions.relation.classes=org.apache.spark.sql.connect.delta.DeltaRelationPlugin"
            --conf "spark.connect.extensions.command.classes=org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
        )
    fi
    if (( ${#connect_packages[@]} > 0 )); then
        packages_arg=$(IFS=,; printf '%s' "${connect_packages[*]}")
        connect_args+=(--packages "${packages_arg}")
    fi
    if [[ -n "${connect_resource}" ]]; then
        connect_args+=("${connect_resource}")
    fi

    connect_log="/tmp/spark-connect-server.log"
    SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-} -Dlog4j.configurationFile=file:/etc/spark/log4j2-integration.properties" \
        spark-submit "${connect_args[@]}" >"${connect_log}" 2>&1 &
    connect_pid="$!"
fi

cleanup() {
    if [[ -n "${connect_pid}" ]]; then
        kill "${connect_pid}" >/dev/null 2>&1 || true
        wait "${connect_pid}" 2>/dev/null || true
    fi
    if [[ -n "${connect_checkpoints}" ]]; then
        rm -rf -- "${connect_checkpoints}"
    fi
}
trap cleanup EXIT

if [[ "${backend}" == "pyspark35" ]]; then
    iceberg_runtime="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
elif [[ "${backend}" == "pyspark40" ]]; then
    iceberg_runtime="org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0"
elif [[ "${backend}" == "pyspark41" || "${backend}" == "spark-connect41" ]]; then
    iceberg_runtime="org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"
fi

if [[ -n "${iceberg_runtime:-}" ]]; then
    echo "Running native Iceberg evidence with ${iceberg_runtime}" >&2
    STRUCTURE_ICEBERG_TESTS=1 STRUCTURE_SPARK_JARS_PACKAGES="${iceberg_runtime}" \
        timeout --signal=TERM --kill-after=15s "${STRUCTURE_INTEGRATION_TIMEOUT:-3600}" \
        python -m pytest \
        /workspace/tests/integration/pyspark/iceberg/test_native_iceberg.py \
        /workspace/tests/integration/pyspark/iceberg/test_iceberg_sql.py \
        /workspace/tests/integration/pyspark/iceberg/test_iceberg_transform.py \
        --rootdir=/workspace \
        -p no:cacheprovider \
        --run-integration \
        "--integration-backend=${backend}" \
        -W 'ignore:distutils Version classes are deprecated:DeprecationWarning' \
        ${INTEGRATION_PYTEST_ARGS:-}
fi

if [[ "${STRUCTURE_ICEBERG_ONLY:-0}" == "1" ]]; then
    if [[ -z "${iceberg_runtime:-}" ]]; then
        echo "Iceberg-only integration requires a backend with a pinned Iceberg runtime." >&2
        exit 2
    fi
    exit 0
fi

pytest_status=0
pytest_args=()
if [[ -n "${iceberg_runtime:-}" ]]; then
    # The dedicated pass above has the Iceberg-only jars and catalog. Do not
    # rediscover that suite in the general process without its isolated setup.
    pytest_args+=(--ignore=/workspace/tests/integration/pyspark/iceberg)
fi
test_paths=(/workspace/tests/integration /workspace/tests/concepts/live_pyspark)
if [[ "${backend}" == "pyspark35" || "${backend}" == "pyspark40" || "${backend}" == "pyspark41" || "${backend}" == "spark-connect41" ]]; then
    # Keep Delta's pinned live evidence in its own process before the broader
    # PySpark tests initialize a driver or Connect client.
    timeout --signal=TERM --kill-after=15s "${STRUCTURE_INTEGRATION_TIMEOUT:-3600}" \
        python -m pytest /workspace/tests/integration/pyspark/v11/test_delta_transform_live.py \
        --rootdir=/workspace \
        -p no:cacheprovider \
        --run-integration \
        "--integration-backend=${backend}" \
        -W 'ignore:distutils Version classes are deprecated:DeprecationWarning' \
        -W 'ignore:The copy keyword is deprecated:Warning' \
        -W 'ignore:ReleaseExecute failed with exception:UserWarning' \
        ${INTEGRATION_PYTEST_ARGS:-} || pytest_status=$?
    if (( pytest_status != 0 )); then
        exit "${pytest_status}"
    fi
    pytest_args+=(--ignore=/workspace/tests/integration/pyspark/v11/test_delta_transform_live.py)
fi

if [[ "${backend}" == "pyspark41" ]]; then
    test_paths=(
        /workspace/tests/integration/pyspark/backend/test_runtime_versions.py
        /workspace/tests/integration/pyspark/v11
    )
fi
timeout --signal=TERM --kill-after=15s "${STRUCTURE_INTEGRATION_TIMEOUT:-3600}" \
    python -m pytest "${test_paths[@]}" \
    --rootdir=/workspace \
    -p no:cacheprovider \
    --run-integration \
    "--integration-backend=${backend}" \
    -W 'ignore:distutils Version classes are deprecated:DeprecationWarning' \
    -W 'ignore:The copy keyword is deprecated:Warning' \
    -W 'ignore:ReleaseExecute failed with exception:UserWarning' \
    "${pytest_args[@]}" \
    ${INTEGRATION_PYTEST_ARGS:-} || pytest_status=$?

if (( pytest_status == 124 || pytest_status == 137 )); then
    echo "Integration deadline reached (${STRUCTURE_INTEGRATION_TIMEOUT:-3600}s); backend=${backend}." >&2
fi

if (( pytest_status != 0 )) && [[ -n "${connect_log}" && -f "${connect_log}" ]]; then
    echo "Spark Connect server output (last 200 lines):" >&2
    tail -n 200 "${connect_log}" >&2
fi

exit "${pytest_status}"
