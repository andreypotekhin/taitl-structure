from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)

from structure import Schema, Transform, input, output, step
from structure.plugin.pyspark import (
    PandasGroupState,
    PandasGroupStateProcessor,
    apply_in_pandas_with_state,
    external_pandas_state_function,
    integer,
    string,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark35", "pyspark40", "pyspark41"},
        reason="Legacy Pandas state evidence targets ordinary PySpark 3.5, 4.0, and 4.1",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_apply_in_pandas_with_state"
GENERATED_PACKAGE = "integration_v11_legacy_pandas_state_generated"


class Event(Schema):
    account_id = string(nullable=False)
    amount = integer(nullable=False)


class AccountKey(Schema):
    account_id = string(nullable=False)


class TotalState(Schema):
    total = integer(nullable=False)


class TotalOutput(Schema):
    account_id = string(nullable=False)
    total = integer(nullable=False)


class AccountTotals(PandasGroupStateProcessor[Event, AccountKey, TotalState, TotalOutput]):
    def on_batches(self, key, batches, state: PandasGroupState[TotalState]):
        import pandas as pd  # type: ignore[import-untyped]

        current = state.get()
        total = 0 if current is None else current.total
        for batch in batches:
            total += int(batch["amount"].sum())
        state.update(TotalState(total=total))
        yield pd.DataFrame({"account_id": [key.account_id], "total": [total]})


def native_totals(key, batches, state):
    import pandas as pd  # type: ignore[import-untyped]

    current = state.get[0] if state.exists else 0
    for batch in batches:
        current += int(batch["amount"].sum())
    state.update((current,))
    yield pd.DataFrame({"account_id": [key[0]], "total": [current]})


class TypedTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return apply_in_pandas_with_state(
            key=event.account_id,
            processor=AccountTotals,
            output_mode="Update",
            timeout="none",
        )


class NativeTotals(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return apply_in_pandas_with_state(
            key=event.account_id,
            processor=external_pandas_state_function(
                native_totals,
                input=Event,
                key=AccountKey,
                state=TotalState,
                output=TotalOutput,
            ),
            output_mode="Update",
            timeout="none",
        )


def test_legacy_pandas_state_matches_native_online_generated_and_restart(
    spark, tmp_path, integration_shared_dir
) -> None:
    files = render_generated_projects(
        (
            (TypedTotals, f"{SOURCE_MODULE}.TypedTotals"),
            (NativeTotals, f"{SOURCE_MODULE}.NativeTotals"),
        ),
        generated_package=GENERATED_PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Event, TotalOutput]},
    )
    generated_source = "\n".join(files.values())
    assert "apply_legacy_pandas_state(" in generated_source
    assert "applyInPandasWithState" not in generated_source
    for token in ("readStream", "writeStream", "checkpointLocation", "toPandas(", ".rdd"):
        assert token not in generated_source

    with TemporaryDirectory(prefix=f"legacy-pandas-state-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
        root = Path(shared_root)

        def run_pipeline(transform, *, source: Path, checkpoint: Path, mode: str) -> list[tuple[str, int]]:
            source.mkdir(parents=True, exist_ok=True)
            emitted: list[tuple[str, int]] = []
            events = spark.readStream.schema("account_id STRING NOT NULL, amount INT NOT NULL").json(str(source))
            result = transform(events=events).run(
                session(spark, execution_mode=mode, generated_package=GENERATED_PACKAGE)
            )

            def collect_batch(frame, _batch_id: int) -> None:
                emitted.extend((row.account_id, row.total) for row in frame.collect())

            query = (
                result.totals.writeStream.foreachBatch(collect_batch)
                .outputMode("update")
                .option("checkpointLocation", str(checkpoint))
                .trigger(once=True)
                .start()
            )
            try:
                assert query.awaitTermination(120), f"{mode} legacy Pandas state query did not complete"
            finally:
                query.stop()
            return emitted

        def run_restart_pair(transform, *, label: str, mode: str) -> list[tuple[str, int]]:
            source = root / label / "source"
            checkpoint = root / label / "checkpoint"
            source.mkdir(parents=True)
            (source / "first.json").write_text(
                json.dumps({"account_id": "a", "amount": 2})
                + "\n"
                + json.dumps({"account_id": "a", "amount": 1})
                + "\n",
                encoding="utf-8",
            )
            first = run_pipeline(transform, source=source, checkpoint=checkpoint, mode=mode)
            (source / "second.json").write_text(json.dumps({"account_id": "a", "amount": 4}) + "\n", encoding="utf-8")
            second = run_pipeline(transform, source=source, checkpoint=checkpoint, mode=mode)
            return first + second

        reference_root = root / "native-reference"
        reference_root.mkdir()
        reference_source = reference_root / "source"
        reference_source.mkdir()
        reference_checkpoint = reference_root / "checkpoint"
        reference_outputs: list[tuple[str, int]] = []

        def direct_reference_batch(frame, _batch_id: int) -> None:
            reference_outputs.extend((row.account_id, row.total) for row in frame.collect())

        from pyspark.sql.streaming.state import GroupStateTimeout

        for filename, amount in (("first.json", 2), ("first-extra.json", 1)):
            with (reference_source / filename).open("w", encoding="utf-8") as source_file:
                source_file.write(json.dumps({"account_id": "a", "amount": amount}) + "\n")

        def run_direct_reference() -> None:
            direct_events = spark.readStream.schema("account_id STRING NOT NULL, amount INT NOT NULL").json(
                str(reference_source)
            )
            direct = direct_events.groupBy("account_id").applyInPandasWithState(
                native_totals,
                outputStructType="account_id string, total int",
                stateStructType="total int",
                outputMode="Update",
                timeoutConf=GroupStateTimeout.NoTimeout,
            )
            query = (
                direct.writeStream.foreachBatch(direct_reference_batch)
                .outputMode("update")
                .option("checkpointLocation", str(reference_checkpoint))
                .trigger(once=True)
                .start()
            )
            try:
                assert query.awaitTermination(120), "native legacy Pandas state reference did not complete"
            finally:
                query.stop()

        run_direct_reference()
        with (reference_source / "second.json").open("w", encoding="utf-8") as source_file:
            source_file.write(json.dumps({"account_id": "a", "amount": 4}) + "\n")
        run_direct_reference()

        assert reference_outputs == [("a", 3), ("a", 7)]
        with generated_project(tmp_path, GENERATED_PACKAGE, files):
            assert run_restart_pair(TypedTotals, label="typed-online", mode="online") == reference_outputs
            assert run_restart_pair(TypedTotals, label="typed-generated", mode="generated") == reference_outputs
            assert run_restart_pair(NativeTotals, label="native-online", mode="online") == reference_outputs
            assert run_restart_pair(NativeTotals, label="native-generated", mode="generated") == reference_outputs
