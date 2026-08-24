from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.plugin.pyspark.compiler.model.PySparkAggregateKey import PySparkAggregateKey
from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkOrderedTimelineScanRecipe import PySparkOrderedTimelineScanRecipe
from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.plugin.pyspark.compiler.model.PySparkWatermarkRecipe import PySparkWatermarkRecipe
from structure.plugin.pyspark.dsl.joins import Join, JoinMethod
from structure.plugin.pyspark.dsl.types import ArrayType, DecimalType, MapType, StructType, StructureType
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression
from structure.plugin.pyspark.render.logic.steps.RenderPySparkAggregatePlan import RenderPySparkAggregatePlan
from structure.plugin.pyspark.render.logic.steps.RenderPySparkFilters import RenderPySparkFilters
from structure.plugin.pyspark.render.logic.steps.RenderPySparkMapGenerator import RenderPySparkMapGenerator
from structure.plugin.pyspark.render.logic.steps.RenderPySparkScalarGenerator import RenderPySparkScalarGenerator
from structure.plugin.pyspark.render.logic.steps.RenderPySparkStructGenerator import RenderPySparkStructGenerator


class RenderPySparkStep:

    def __init__(self, schema_names: Mapping[type[Schema], str] | None = None) -> None:
        self._aggregate_renderer = RenderPySparkAggregatePlan(self)
        self._filters_renderer = RenderPySparkFilters()
        self._struct_generator_renderer = RenderPySparkStructGenerator()
        self._scalar_generator_renderer = RenderPySparkScalarGenerator()
        self._map_generator_renderer = RenderPySparkMapGenerator()
        from structure.plugin.pyspark.api.PySpark import PySpark

        self._schema = PySpark.schema.render(schema_names)

    def __call__(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        *,
        current: str,
        sources: dict[str, str] | None = None,
        source_transform: str | None = None,
        generated_hooks: bool = False,
        backend_target: str = ">=3.5,<4.1",
    ) -> str:
        if isinstance(step, PySparkStepRecipe) and len(step.results) > 1:
            return self._multiple(
                step,
                current=current,
                sources=sources or {},
                source_transform=source_transform,
                generated_hooks=generated_hooks,
                backend_target=backend_target,
            )
        target = self._target(step)
        lines = [f"        # Step method: {step.name}"]
        active = current
        if step.before_hooks:
            lines.extend(
                self._hooks(
                    step.before_hooks,
                    sources=sources or {},
                    source_transform=source_transform,
                    generated_hooks=generated_hooks,
                )
            )
        lines.append(f'        {target} = {active}.alias("{step.input_alias}")')
        lines.extend(self._operations(step, sources=sources or {}, target=target, backend_target=backend_target))
        lines.extend(self._projection(step, target=target))
        if isinstance(step, PySparkStepRecipe):
            hook_sources = {**(sources or {}), step.results[0].frame: target}
            lines.extend(
                self._hooks(
                    step.after_hooks,
                    sources=hook_sources,
                    source_transform=source_transform,
                    generated_hooks=generated_hooks,
                )
            )
        lines.extend(self._validations(step.validations, target=target))
        lines.extend(self._post_operations(step, target=target))
        return "\n".join(lines)

    def _multiple(
        self,
        step: PySparkStepRecipe,
        *,
        current: str,
        sources: dict[str, str],
        source_transform: str | None,
        generated_hooks: bool,
        backend_target: str,
    ) -> str:
        lines = [f"        # Step method: {step.name}"]
        active = current
        if step.before_hooks:
            lines.extend(
                self._hooks(
                    step.before_hooks,
                    sources=sources,
                    source_transform=source_transform,
                    generated_hooks=generated_hooks,
                )
            )
        base = f"{step.name}_base"
        lines.append(f'        {base} = {active}.alias("{step.input_alias}")')
        lines.extend(self._operations(step, sources=sources, target=base, backend_target=backend_target))
        for result in step.results:
            lines.extend(self._result_projection(step, result, base=base))
        for result in step.results:
            lines.extend(
                self._hooks(
                    result.after_hooks,
                    sources={**sources, **{item.frame: item.frame for item in step.results}},
                    source_transform=source_transform,
                    generated_hooks=generated_hooks,
                )
            )
            lines.extend(self._validations(result.validations, target=result.frame))
            lines.extend(self._post_operations(step, target=result.frame))
        return "\n".join(lines)

    def _result_projection(self, step: PySparkStepRecipe, result, *, base: str) -> list[str]:
        lines = [f"        {result.frame} = {base}.select("]
        for assignment in result.projection:
            lines.append(f"            {self._assignment(assignment, scope_aliases=self._scope_aliases(step))},")
        lines.append("        )")
        return lines

    def _hooks(
        self,
        hooks: tuple[PySparkHookRecipe, ...],
        *,
        sources: dict[str, str],
        source_transform: str | None,
        generated_hooks: bool,
    ) -> list[str]:
        lines: list[str] = []
        for hook in hooks:
            arguments = ", ".join(
                f"{lane}={sources.get(source, source.removeprefix('input:'))}"
                for lane, source in zip(hook.lanes, hook.sources, strict=True)
            )
            outputs = ", ".join(hook.outputs)
            callee, prefix = self._hook_call(
                hook,
                source_transform=source_transform,
                generated_hooks=generated_hooks,
            )
            arguments = f"{prefix}{arguments}" if prefix else arguments
            lines.append(f"        {outputs} = {callee}({arguments}, spark=self.spark, ctx=self.ctx)")
        return lines

    def _hook_call(
        self,
        hook: PySparkHookRecipe,
        *,
        source_transform: str | None,
        generated_hooks: bool,
    ) -> tuple[str, str]:
        if generated_hooks:
            origin = hook.origin
            if origin is not None and source_transform is not None and origin.import_name != source_transform:
                return f"{origin.class_name}Generated.{origin.member_name}", "self, "
            return f"self.{hook.name}", ""
        origin = hook.origin
        if origin is None or source_transform is None or origin.import_name == source_transform:
            return f"self._impl.{hook.name}", ""
        return f"self.{self._hook_impl_field(hook)}.{origin.member_name}", ""

    def _hook_impl_field(self, hook: PySparkHookRecipe) -> str:
        origin = hook.origin
        if origin is None:
            return "_impl"
        return f"_impl_{self._identifier(f'{self._hook_stage(hook)}_{origin.class_name}')}"

    def _hook_stage(self, hook: PySparkHookRecipe) -> str:
        origin = hook.origin
        if "." in hook.target:
            return hook.target.split(".", 1)[0]
        return "" if origin is None else origin.class_name

    def _joins(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        *,
        sources: dict[str, str],
        target: str = "df",
    ) -> list[str]:
        lines: list[str] = (
            self._streaming_guard(step, sources=sources)
            if any(join.assert_singleton_in_batch for join in step.joins)
            else []
        )
        for join in step.joins:
            lines.extend(self._join(step, join, sources=sources, target=target))
        return lines

    def _operations(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        *,
        sources: dict[str, str],
        target: str,
        backend_target: str,
    ) -> list[str]:
        if not step.operations:
            lines = self._joins(step, sources=sources, target=target)
            if step.filters:
                lines.extend(
                    self._filters_renderer(step.filters, scope_aliases=self._scope_aliases(step), target=target)
                )
            return lines

        ordered_lines: list[str] = []
        if any(
            operation.kind == "join" and operation.join is not None and operation.join.assert_singleton_in_batch
            for operation in step.operations
        ):
            ordered_lines.extend(self._streaming_guard(step, sources=sources))
        pending_filters: list[PySparkExpressionRecipe] = []
        prepared_sources = dict(sources)
        joined_scopes: set[str] = set()
        dedupe_index = 0
        exact_one_index = 0
        generator_index = 0
        for index, operation in enumerate(step.operations):
            if operation.kind == "filter" and operation.filter is not None:
                pending_filters.append(operation.filter)
                continue
            if pending_filters:
                ordered_lines.extend(
                    self._filters_renderer(
                        tuple(pending_filters), scope_aliases=self._scope_aliases(step), target=target
                    )
                )
                pending_filters = []
            if operation.kind == "join" and operation.join is not None:
                ordered_lines.extend(self._join(step, operation.join, sources=prepared_sources, target=target))
                joined_scopes.add(operation.join.input_name)
            if operation.kind == "aggregate" and operation.aggregate is not None:
                ordered_lines.extend(
                    self._aggregate_renderer(step, operation.aggregate, target=target, backend_target=backend_target)
                )
            if operation.kind == "selected_rows" and operation.selected_rows is not None:
                ordered_lines.extend(self._selected_rows(step, operation.selected_rows, target=target))
            if operation.kind == "drop_duplicates":
                duplicate_rows = operation.duplicate_rows or PySparkDuplicateRowsRecipe()
                if self._prepares_relation(duplicate_rows, step=step, joined_scopes=joined_scopes):
                    dedupe_index += 1
                    scope = cast(str, duplicate_rows.scope)
                    source_key = self._source_for_scope(step, scope)
                    source = prepared_sources.get(source_key, prepared_sources.get(scope, source_key))
                    prepared = f"{target}_{self._identifier(scope)}_deduped_{dedupe_index}"
                    ordered_lines.extend(
                        self._drop_duplicates(
                            source,
                            prepared,
                            duplicate_rows,
                        )
                    )
                    prepared_sources[source_key] = prepared
                    prepared_sources[scope] = prepared
                else:
                    ordered_lines.extend(self._drop_duplicates(target, target, duplicate_rows))
            if operation.kind == "exactly_one" and operation.exactly_one is not None:
                exact_one_index += 1
                scope = operation.exactly_one.scope
                if scope == getattr(step, "source_scope", None):
                    ordered_lines.extend(self._exactly_one(target, target, scope, index=exact_one_index))
                else:
                    source_key = self._source_for_scope(step, scope)
                    source = prepared_sources.get(source_key, prepared_sources.get(scope, source_key))
                    prepared = f"{target}_{self._identifier(scope)}_exactly_one_{exact_one_index}"
                    ordered_lines.extend(self._exactly_one(source, prepared, scope, index=exact_one_index))
                    prepared_sources[source_key] = prepared
                    prepared_sources[scope] = prepared
            if operation.kind == "posexplode_struct" and operation.posexplode_struct is not None:
                generator_index += 1
                ordered_lines.extend(
                    self._struct_generator_renderer(
                        operation.posexplode_struct,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if operation.kind == "posexplode_outer_struct" and operation.posexplode_struct is not None:
                generator_index += 1
                ordered_lines.extend(
                    self._struct_generator_renderer(
                        operation.posexplode_struct,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if operation.kind == "explode_struct" and operation.posexplode_struct is not None:
                generator_index += 1
                ordered_lines.extend(
                    self._struct_generator_renderer(
                        operation.posexplode_struct,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if operation.kind == "explode_outer_struct" and operation.posexplode_struct is not None:
                generator_index += 1
                ordered_lines.extend(
                    self._struct_generator_renderer(
                        operation.posexplode_struct,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if operation.kind == "inline_struct" and operation.posexplode_struct is not None:
                generator_index += 1
                ordered_lines.extend(
                    self._struct_generator_renderer(
                        operation.posexplode_struct,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if operation.kind == "inline_outer_struct" and operation.posexplode_struct is not None:
                generator_index += 1
                ordered_lines.extend(
                    self._struct_generator_renderer(
                        operation.posexplode_struct,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if operation.kind == "variant_explode" and operation.posexplode_struct is not None:
                generator_index += 1
                ordered_lines.extend(
                    self._struct_generator_renderer(
                        operation.posexplode_struct,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if operation.kind == "variant_explode_outer" and operation.posexplode_struct is not None:
                generator_index += 1
                ordered_lines.extend(
                    self._struct_generator_renderer(
                        operation.posexplode_struct,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if (
                operation.kind
                in {
                    "explode_array",
                    "explode_outer_array",
                    "posexplode_array",
                    "posexplode_outer_array",
                }
                and operation.scalar_generator is not None
            ):
                generator_index += 1
                ordered_lines.extend(
                    self._scalar_generator_renderer(
                        operation.scalar_generator,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if (
                operation.kind in {"explode_map", "explode_outer_map", "posexplode_map", "posexplode_outer_map"}
                and operation.map_generator is not None
            ):
                generator_index += 1
                ordered_lines.extend(
                    self._map_generator_renderer(
                        operation.map_generator,
                        aliases=self._scope_aliases(step),
                        target=target,
                        index=generator_index,
                    )
                )
            if operation.kind == "ordered_timeline_scan" and operation.ordered_timeline_scan is not None:
                ordered_lines.extend(
                    self._ordered_timeline_scan(
                        operation.ordered_timeline_scan,
                        step=step,
                        target=target,
                        index=index,
                    )
                )
            if operation.relation_alias is not None:
                continue
            if operation.relation_assertion is not None:
                ordered_lines.extend(
                    self._relation_assertion(
                        operation.relation_assertion,
                        step=step,
                        sources=prepared_sources,
                        target=target,
                        index=index,
                    )
                )
            if operation.relation_order is not None:
                ordered_lines.extend(self._relation_order(operation.relation_order, step=step, target=target))
            if operation.relation_bound is not None:
                ordered_lines.extend(self._relation_bound(operation.kind, operation.relation_bound, target=target))
            if operation.relation_sample is not None:
                ordered_lines.extend(self._relation_sample(operation.relation_sample, target=target))
            if operation.relation_priority_selection is not None:
                ordered_lines.extend(
                    self._relation_priority_selection(
                        operation.relation_priority_selection,
                        step=step,
                        target=target,
                        index=index,
                    )
                )
            if operation.relation_hierarchy_closure is not None:
                ordered_lines.extend(
                    self._relation_hierarchy_closure(
                        operation.relation_hierarchy_closure,
                        step=step,
                        target=target,
                        index=index,
                    )
                )
            if operation.relation_hierarchy_fallback is not None:
                source = prepared_sources.get(
                    operation.relation_hierarchy_fallback.parent_source,
                    prepared_sources.get(
                        operation.relation_hierarchy_fallback.parent_input,
                        operation.relation_hierarchy_fallback.parent_source,
                    ),
                )
                ordered_lines.extend(
                    self._relation_hierarchy_fallbacks(
                        operation.relation_hierarchy_fallback,
                        parent_source=source,
                        step=step,
                        target=target,
                        index=index,
                    )
                )
            if operation.relation_set is not None:
                source = prepared_sources.get(
                    operation.relation_set.source,
                    prepared_sources.get(operation.relation_set.input_name, operation.relation_set.source),
                )
                ordered_lines.extend(self._relation_set(source, operation.relation_set, step=step, target=target))
            if operation.kind == "watermark" and operation.watermark is not None:
                if operation.watermark.scope == getattr(step, "source_scope", ""):
                    ordered_lines.extend(self._watermark(operation.watermark, target=target))
            if operation.kind == "persist" and operation.persist is not None:
                storage = (
                    "()"
                    if operation.persist.storage_level is None
                    else self._storage_level(operation.persist.storage_level)
                )
                ordered_lines.append(
                    f"        {target} = {target}.persist({storage})"
                    if storage != "()"
                    else f"        {target} = {target}.persist()"
                )
            if operation.kind == "unpersist" and operation.unpersist is not None:
                ordered_lines.append(
                    f"        {target} = {target}.unpersist(blocking={operation.unpersist.blocking!r})"
                )
            if operation.kind == "checkpoint" and operation.checkpoint is not None:
                ordered_lines.append(f"        {target} = {target}.checkpoint(eager={operation.checkpoint.eager!r})")
            if operation.kind == "local_checkpoint" and operation.local_checkpoint is not None:
                ordered_lines.append(
                    f"        {target} = {target}.localCheckpoint(eager={operation.local_checkpoint.eager!r})"
                )
        if pending_filters:
            ordered_lines.extend(
                self._filters_renderer(tuple(pending_filters), scope_aliases=self._scope_aliases(step), target=target)
            )
        return ordered_lines

    def _post_operations(self, step: PySparkStepRecipe | PySparkOutputRecipe, *, target: str) -> list[str]:
        return [
            (
                f"        {target} = {target}.persist({self._cache_storage_level(operation)})"
                if operation.cache is not None and operation.cache.storage_level is not None
                else f"        {target} = {target}.persist()"
            )
            for operation in step.operations
            if operation.kind == "cache"
        ]

    def _cache_storage_level(self, operation) -> str:
        assert operation.cache is not None and operation.cache.storage_level is not None
        return self._storage_level(operation.cache.storage_level)

    @staticmethod
    def _storage_level(storage_level: tuple[bool, bool, bool, bool, int]) -> str:
        return f"StorageLevel({', '.join(map(str, storage_level))})"

    def _dedupe_subset(self, duplicate_rows: PySparkDuplicateRowsRecipe) -> str:
        if not duplicate_rows.subset:
            return ""
        return json.dumps(tuple(self._field_column(expression) for expression in duplicate_rows.subset))

    def _drop_duplicates(
        self,
        source: str,
        target: str,
        duplicate_rows: PySparkDuplicateRowsRecipe,
    ) -> list[str]:
        subset = self._dedupe_subset(duplicate_rows)
        if duplicate_rows.within_watermark:
            return [f"        {target} = {source}.dropDuplicatesWithinWatermark({subset})"]
        return [
            f"        if {source}.isStreaming:",
            f"            {target} = {source}.dropDuplicatesWithinWatermark({subset})",
            "        else:",
            f"            {target} = {source}.dropDuplicates({subset})",
        ]

    def _exactly_one(self, source: str, target: str, scope: str, *, index: int) -> list[str]:
        count = f"{target}_{self._identifier(scope)}_count_{index}" if source == target else f"{target}_count"
        assertion = f"REL-E0701: exactly_one({scope}) requires exactly one row; " "see docs/Diagnostics.md#rel-e0701"
        return [
            f'        {count} = {source}.agg(F.count(F.lit(1)).alias("__structure_count"))',
            f"        {count} = {count}.select(",
            f'            F.assert_true(F.col("__structure_count") == F.lit(1), {assertion!r})',
            '            .alias("__structure_exactly_one")',
            "        )",
            f'        {target} = {count}.crossJoin({source}).drop("__structure_exactly_one")',
        ]

    def _relation_assertion(self, assertion, *, step, sources: dict[str, str], target: str, index: int) -> list[str]:
        if assertion.operation == "require_unique":
            return self._require_unique(assertion, step=step, target=target, index=index)
        if assertion.operation == "require_all":
            return self._require_all(assertion, step=step, target=target, index=index)
        if assertion.operation == "require_reference":
            return self._require_reference(assertion, step=step, sources=sources, target=target, index=index)
        if assertion.operation == "require_parent_hierarchy":
            return self._require_parent_hierarchy(assertion, step=step, target=target, index=index)
        raise TypeError(f"Unsupported relation assertion: {assertion.operation}")

    def _require_unique(self, assertion, *, step, target: str, index: int) -> list[str]:
        keys = ", ".join(
            render_pyspark_expression(expression, scope_aliases=self._scope_aliases(step))
            for expression in assertion.keys
        )
        prefix = f"{target}_require_unique_{index}"
        message = "REL-E0702: require_unique(...) found duplicate keys; " "see docs/Diagnostics.md#rel-e0702"
        return [
            f"        {prefix}_duplicates = {target}.groupBy({keys}).agg(",
            '            F.count(F.lit(1)).alias("__structure_count")',
            "        )",
            f'        {prefix}_duplicates = {prefix}_duplicates.where(F.col("__structure_count") > F.lit(1))',
            f"        {prefix}_violations = {prefix}_duplicates.agg(",
            '            F.count(F.lit(1)).alias("__structure_violations")',
            "        )",
            f"        {prefix}_assertion = {prefix}_violations.select(",
            f'            F.assert_true(F.col("__structure_violations") == F.lit(0), {message!r})',
            '            .alias("__structure_require_unique")',
            "        )",
            f'        {target} = {prefix}_assertion.crossJoin({target}).drop("__structure_require_unique")',
        ]

    def _require_all(self, assertion, *, step, target: str, index: int) -> list[str]:
        assert assertion.predicate is not None
        predicate = render_pyspark_expression(assertion.predicate, scope_aliases=self._scope_aliases(step))
        prefix = f"{target}_require_all_{index}"
        message = (
            "REL-E0703: require_all(...) found rows that do not satisfy the predicate; "
            "see docs/Diagnostics.md#rel-e0703"
        )
        return [
            f"        {prefix}_violations = {target}.where(~F.coalesce({predicate}, F.lit(False))).agg(",
            '            F.count(F.lit(1)).alias("__structure_violations")',
            "        )",
            f"        {prefix}_assertion = {prefix}_violations.select(",
            f'            F.assert_true(F.col("__structure_violations") == F.lit(0), {message!r})',
            '            .alias("__structure_require_all")',
            "        )",
            f'        {target} = {prefix}_assertion.crossJoin({target}).drop("__structure_require_all")',
        ]

    def _require_reference(self, assertion, *, step, sources: dict[str, str], target: str, index: int) -> list[str]:
        assert assertion.value is not None
        assert assertion.reference_key is not None
        value_column = f"__structure_reference_value_{index}"
        key_column = f"__structure_reference_key_{index}"
        prefix = f"{target}_require_reference_{index}"
        source = sources.get(
            assertion.reference_source,
            sources.get(assertion.reference_input, assertion.reference_source),
        )
        reference_aliases = {
            assertion.reference_input: "",
            assertion.reference_schema.__name__: "",
        }
        value = render_pyspark_expression(assertion.value, scope_aliases=self._scope_aliases(step))
        reference_key = render_pyspark_expression(assertion.reference_key, scope_aliases=reference_aliases)
        candidates = f"{prefix}_candidates"
        if assertion.nulls == "allow":
            candidates_line = (
                f'        {candidates} = {prefix}_left.where(F.col({self._literal(value_column)}).isNotNull())'
            )
        else:
            candidates_line = f"        {candidates} = {prefix}_left"
        message = (
            "REL-E0704: require_reference(...) found values without a reference row; "
            "see docs/Diagnostics.md#rel-e0704"
        )
        return [
            f"        {prefix}_left = {target}.withColumn({self._literal(value_column)}, {value})",
            f"        {prefix}_right = {source}.select(",
            f"            {reference_key}.alias({self._literal(key_column)})",
            "        )",
            f"        {prefix}_right = {prefix}_right.dropDuplicates([{self._literal(key_column)}])",
            candidates_line,
            f"        {prefix}_violations = {candidates}.join(",
            f"            {prefix}_right,",
            f"            F.col({self._literal(value_column)}) == F.col({self._literal(key_column)}),",
            '            "left_anti",',
            "        )",
            f"        {prefix}_violations = {prefix}_violations.agg(",
            '            F.count(F.lit(1)).alias("__structure_violations")',
            "        )",
            f"        {prefix}_assertion = {prefix}_violations.select(",
            f'            F.assert_true(F.col("__structure_violations") == F.lit(0), {message!r})',
            '            .alias("__structure_require_reference")',
            "        )",
            f'        {target} = {prefix}_assertion.crossJoin({target}).drop("__structure_require_reference")',
        ]

    def _require_parent_hierarchy(self, assertion, *, step, target: str, index: int) -> list[str]:
        assert assertion.parent is not None
        assert assertion.order_by is not None
        assert assertion.max_depth is not None
        aliases = self._scope_aliases(step)
        node = f"__structure_hierarchy_node_{index}"
        parent = f"__structure_hierarchy_parent_{index}"
        order = f"__structure_hierarchy_order_{index}"
        path = f"__structure_hierarchy_path_{index}"
        prefix = f"{target}_require_parent_hierarchy_{index}"
        node_id = render_pyspark_expression(assertion.keys[0], scope_aliases=aliases)
        parent_id = render_pyspark_expression(assertion.parent, scope_aliases=aliases)
        order_by = render_pyspark_expression(self._order_value(assertion.order_by), scope_aliases=aliases)
        message = (
            "REL-E0706: require_parent_hierarchy(...) found missing parent, cycle, depth overrun, "
            "or non-increasing child order; see docs/Diagnostics.md#rel-e0706"
        )
        lines = [
            f"        {prefix}_nodes = {target}.select(",
            f"            {node_id}.alias({self._literal(node)}),",
            f"            {parent_id}.alias({self._literal(parent)}),",
            f"            {order_by}.alias({self._literal(order)}),",
            "        )",
            f"        {prefix}_ids = {prefix}_nodes.select(",
            f"            F.col({self._literal(node)}).alias({self._literal('__structure_hierarchy_known_parent')}),",
            "        )",
            f"        {prefix}_missing = {prefix}_nodes.where(F.col({self._literal(parent)}).isNotNull()).join(",
            f"            {prefix}_ids,",
            f"            F.col({self._literal(parent)}) == F.col({self._literal('__structure_hierarchy_known_parent')}),",
            '            "left_anti",',
            "        )",
            f"        {prefix}_parent_order = {prefix}_nodes.alias(\"child\").join(",
            f"            {prefix}_nodes.alias(\"parent\"),",
            f"            F.col(\"child.{parent}\") == F.col(\"parent.{node}\"),",
            '            "inner",',
            "        )",
            f"        {prefix}_parent_order = {prefix}_parent_order.where(",
            f"            ~(F.col(\"child.{order}\") > F.col(\"parent.{order}\"))",
            "        )",
            f"        {prefix}_parent_order = {prefix}_parent_order.select(",
            f"            F.col(\"child.{node}\").alias({self._literal(node)}),",
            f"            F.col(\"child.{parent}\").alias({self._literal(parent)}),",
            f"            F.col(\"child.{order}\").alias({self._literal(order)}),",
            "        )",
            f"        {prefix}_frontier = {prefix}_nodes.where(F.col({self._literal(parent)}).isNotNull()).select(",
            f"            F.col({self._literal(node)}),",
            f"            F.col({self._literal(parent)}),",
            f"            F.col({self._literal(order)}),",
            f"            F.array(F.col({self._literal(node)})).alias({self._literal(path)}),",
            "        )",
            f"        {prefix}_cycles = {prefix}_frontier.where(",
            f"            F.array_contains(F.col({self._literal(path)}), F.col({self._literal(parent)}))",
            "        )",
        ]
        for depth in range(assertion.max_depth):
            lines.extend(
                [
                    f"        {prefix}_frontier = {prefix}_frontier.where(",
                    f"            F.col({self._literal(parent)}).isNotNull()",
                    f"            & ~F.array_contains(F.col({self._literal(path)}), F.col({self._literal(parent)}))",
                    "        )",
                    f"        {prefix}_frontier = {prefix}_frontier.withColumn(",
                    f"            {self._literal(path)}, F.array_append(F.col({self._literal(path)}), F.col({self._literal(parent)}))",
                    "        )",
                    f"        {prefix}_frontier = {prefix}_frontier.alias(\"frontier\").join(",
                    f"            {prefix}_nodes.alias(\"next_parent\"),",
                    f"            F.col(\"frontier.{parent}\") == F.col(\"next_parent.{node}\"),",
                    '            "left",',
                    "        ).select(",
                    f"            F.col(\"frontier.{node}\").alias({self._literal(node)}),",
                    f"            F.col(\"next_parent.{parent}\").alias({self._literal(parent)}),",
                    f"            F.col(\"frontier.{order}\").alias({self._literal(order)}),",
                    f"            F.col(\"frontier.{path}\").alias({self._literal(path)}),",
                    "        )",
                    f"        {prefix}_cycles = {prefix}_cycles.unionByName(",
                    f"            {prefix}_frontier.where(",
                    f"                F.array_contains(F.col({self._literal(path)}), F.col({self._literal(parent)}))",
                    "            ),",
                    "            allowMissingColumns=False,",
                    "        )",
                ]
            )
        lines.extend(
            [
                f"        {prefix}_overrun = {prefix}_frontier.where(F.col({self._literal(parent)}).isNotNull())",
                f"        {prefix}_violations = {prefix}_missing.unionByName(",
                f"            {prefix}_parent_order,",
                "            allowMissingColumns=False,",
                "        ).unionByName(",
                f"            {prefix}_cycles.select({prefix}_missing.columns),",
                "            allowMissingColumns=False,",
                "        ).unionByName(",
                f"            {prefix}_overrun.select({prefix}_missing.columns),",
                "            allowMissingColumns=False,",
                "        )",
                f"        {prefix}_violations = {prefix}_violations.agg(",
                '            F.count(F.lit(1)).alias("__structure_violations")',
                "        )",
                f"        {prefix}_assertion = {prefix}_violations.select(",
                f'            F.assert_true(F.col("__structure_violations") == F.lit(0), {message!r})',
                '            .alias("__structure_require_parent_hierarchy")',
                "        )",
                f'        {target} = {prefix}_assertion.crossJoin({target}).drop("__structure_require_parent_hierarchy")',
            ]
        )
        return lines

    def _ordered_timeline_scan(
        self,
        scan: PySparkOrderedTimelineScanRecipe,
        *,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        target: str,
        index: int,
    ) -> list[str]:
        prefix = f"{target}_{self._identifier(scan.scope)}_{index}"
        keyed = f"{prefix}_keyed"
        grouped = f"{prefix}_grouped"
        folded = f"{prefix}_folded"
        expanded = f"{prefix}_expanded"
        guard = f"{prefix}_guard"
        partition_columns = tuple(f"{prefix}_partition_{position}" for position, _ in enumerate(scan.partition_by))
        order_columns = tuple(f"{prefix}_order_{position}" for position, _ in enumerate(scan.order_by))
        items = f"{prefix}_items"
        folded_column = f"{prefix}_scan"
        row = f"{prefix}_row"
        position = f"{prefix}_pos"
        guard_column = f"__structure_{scan.scope.strip('_')}_guard"
        aliases = self._scope_aliases(step)
        input_columns = tuple(field.column for field in step.input_schema._structure_fields.values())

        lines = [f"        {keyed} = {target}"]
        for column, expression in zip(partition_columns, scan.partition_by, strict=True):
            rendered = render_pyspark_expression(expression, scope_aliases=aliases)
            lines.append(f"        {keyed} = {keyed}.withColumn({self._literal(column)}, {rendered})")
        for column, expression in zip(order_columns, scan.order_by, strict=True):
            rendered = render_pyspark_expression(expression, scope_aliases=aliases)
            lines.append(f"        {keyed} = {keyed}.withColumn({self._literal(column)}, {rendered})")

        lines.extend(self._scan_guard(keyed, guard, partition_columns, order_columns, scan))
        lines.append(f"        {keyed} = {guard}.crossJoin({keyed})")

        payload = self._scan_payload(input_columns)
        item = self._scan_item(order_columns, payload)
        groups = ", ".join(self._literal(column) for column in (*partition_columns, guard_column))
        initial_state = self._scan_initial_state(scan, aliases)
        initial_accumulator = (
            "F.struct("
            f"{initial_state}.alias('__state'), "
            f"F.array().cast({self._scan_rows_type(step, scan)}).alias('__rows')"
            ")"
        )
        merge = self._scan_merge(scan)
        lines.extend(
            [
                f"        {grouped} = {keyed}.groupBy({groups}).agg(",
                f"            F.sort_array(F.collect_list({item})).alias({self._literal(items)})",
                "        )",
                f"        {folded} = {grouped}.select(",
                f"            F.aggregate(F.col({self._literal(items)}), {initial_accumulator}, {merge})",
                f"            .alias({self._literal(folded_column)})",
                "        )",
                f"        {expanded} = {folded}.select(",
                f"            F.posexplode(F.col({self._literal(folded_column)}).getField('__rows'))",
                f"            .alias({self._literal(position)}, {self._literal(row)})",
                "        )",
                f"        {target} = {expanded}.select(",
            ]
        )
        for column in input_columns:
            lines.append(
                f"            F.col({self._literal(f'{row}.__payload.{column}')}).alias({self._literal(column)}),"
            )
        for field in scan.state_schema._structure_fields.values():
            lines.append(
                f"            F.col({self._literal(f'{row}.__state.{field.column}')}).alias({self._literal(field.column)}),"
            )
        lines.append("        )")
        return lines

    def _scan_guard(
        self,
        keyed: str,
        guard: str,
        partition_columns: tuple[str, ...],
        order_columns: tuple[str, ...],
        scan: PySparkOrderedTimelineScanRecipe,
    ) -> list[str]:
        violation = "__structure_scan_violation"
        count = "__structure_scan_count"
        null_order = " | ".join(f"F.col({self._literal(column)}).isNull()" for column in order_columns)
        grouped_keys = ", ".join(self._literal(column) for column in (*partition_columns, *order_columns))
        grouped_partitions = ", ".join(self._literal(column) for column in partition_columns)
        message = (
            "SCAN-E0801: scan(...) found null order keys, duplicate order keys, or a partition above max_rows; "
            "see docs/dev/specifications/OrderedTimelineScan.spec.md"
        )
        guard_column = f"__structure_{scan.scope.strip('_')}_guard"
        return [
            f"        {guard}_nulls = {keyed}.where({null_order}).select(F.lit(1).alias({self._literal(violation)}))",
            f"        {guard}_duplicates = {keyed}.groupBy({grouped_keys}).agg(",
            f"            F.count(F.lit(1)).alias({self._literal(count)})",
            "        )",
            f"        {guard}_duplicates = {guard}_duplicates.where(F.col({self._literal(count)}) > F.lit(1))",
            f"        {guard}_duplicates = {guard}_duplicates.select(F.lit(1).alias({self._literal(violation)}))",
            f"        {guard}_overruns = {keyed}.groupBy({grouped_partitions}).agg(",
            f"            F.count(F.lit(1)).alias({self._literal(count)})",
            "        )",
            f"        {guard}_overruns = {guard}_overruns.where(F.col({self._literal(count)}) > F.lit({scan.max_rows}))",
            f"        {guard}_overruns = {guard}_overruns.select(F.lit(1).alias({self._literal(violation)}))",
            f"        {guard}_violations = {guard}_nulls.unionByName(",
            f"            {guard}_duplicates,",
            "            allowMissingColumns=False,",
            f"        ).unionByName({guard}_overruns, allowMissingColumns=False)",
            f"        {guard} = {guard}_violations.agg(F.count(F.lit(1)).alias({self._literal(count)}))",
            f"        {guard} = {guard}.select(",
            f"            F.assert_true(F.col({self._literal(count)}) == F.lit(0), {self._literal(message)})",
            f"            .alias({self._literal(guard_column)})",
            "        )",
        ]

    def _scan_payload(self, input_columns: tuple[str, ...]) -> str:
        columns = ", ".join(
            f"F.col({self._literal(column)}).alias({self._literal(column)})" for column in input_columns
        )
        return f"F.struct({columns}).alias('__payload')"

    def _scan_item(self, order_columns: tuple[str, ...], payload: str) -> str:
        order = ", ".join(f"F.col({self._literal(column)}).alias({self._literal(column)})" for column in order_columns)
        return f"F.struct({order}, {payload})"

    def _scan_initial_state(self, scan: PySparkOrderedTimelineScanRecipe, aliases: dict[str, str]) -> str:
        fields = []
        for name, expression in scan.initial:
            field = scan.state_schema._structure_fields[name]
            rendered = render_pyspark_expression(expression, scope_aliases=aliases)
            fields.append(f"{rendered}.cast({self._scan_type(field.type)}).alias({self._literal(field.column)})")
        return f"F.struct({', '.join(fields)})"

    def _scan_merge(self, scan: PySparkOrderedTimelineScanRecipe) -> str:
        before = (
            "F.struct(" "item.getField('__payload').alias('__payload'), " "acc.getField('__state').alias('__state')" ")"
        )
        next_state = self._scan_next_state(scan)
        rows = f"F.concat(acc.getField('__rows'), F.array({before})).alias('__rows')"
        return f"lambda acc, item: F.struct({next_state}.alias('__state'), {rows})"

    def _scan_next_state(self, scan: PySparkOrderedTimelineScanRecipe) -> str:
        fields = []
        for name, expression in scan.transition:
            field = scan.state_schema._structure_fields[name]
            rewritten = self._scan_rewrite(expression, scan, state="acc", payload="item")
            rendered = render_pyspark_expression(rewritten, scope_aliases={})
            fields.append(f"{rendered}.cast({self._scan_type(field.type)}).alias({self._literal(field.column)})")
        return f"F.struct({', '.join(fields)})"

    def _scan_rows_type(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        scan: PySparkOrderedTimelineScanRecipe,
    ) -> str:
        payload_fields = ", ".join(self._inline_field(field) for field in step.input_schema._structure_fields.values())
        state_fields = ", ".join(
            self._inline_field(field, scan_internal=True) for field in scan.state_schema._structure_fields.values()
        )
        return (
            "T.ArrayType("
            "T.StructType(["
            f"T.StructField('__payload', T.StructType([{payload_fields}]), False), "
            f"T.StructField('__state', T.StructType([{state_fields}]), False)"
            "]), containsNull=False)"
        )

    def _inline_field(self, field, *, scan_internal: bool = False) -> str:
        nullable = "True" if field.nullable else "False"
        type_ = self._scan_type(field.type) if scan_internal else self._schema.type(field.type)
        return f"T.StructField({self._literal(field.column)}, {type_}, {nullable})"

    def _scan_type(self, type_: StructureType) -> str:
        if isinstance(type_, ArrayType):
            return f"T.ArrayType({self._scan_type(type_.element)}, containsNull=True)"
        if isinstance(type_, MapType):
            return f"T.MapType({self._scan_type(type_.key)}, {self._scan_type(type_.value)}, valueContainsNull=True)"
        if isinstance(type_, StructType):
            fields = ", ".join(
                self._inline_field(field, scan_internal=True) for field in type_.schema._structure_fields.values()
            )
            return f"T.StructType([{fields}])"
        return self._schema.type(type_)

    def _scan_rewrite(self, expression, scan, *, state: str, payload: str):
        if expression.kind == "field" and "scope" in expression.data:
            scope = str(expression.data["scope"])
            if scope == scan.state_scope:
                return self._scan_field(expression, state, "__state")
            if scope == scan.row_scope:
                return self._scan_field(expression, payload, "__payload")
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data=expression.data,
            args=tuple(
                self._scan_rewrite(argument, scan, state=state, payload=payload) for argument in expression.args
            ),
        )

    def _scan_field(self, expression, root: str, first: str):
        rewritten = PySparkExpressionRecipe(
            kind="lambda_arg",
            type=None,
            nullable=False,
            data={"name": root},
        )
        path = (first, *expression.data.get("path", (expression.data["field"],)))
        for field in path:
            rewritten = PySparkExpressionRecipe(
                kind="get_field",
                type=expression.type,
                nullable=expression.nullable,
                data={"field": str(field)},
                args=(rewritten,),
            )
        return rewritten

    def _relation_set(self, source: str, relation_set, *, step, target: str) -> list[str]:
        lines: list[str] = []
        for path, default in relation_set.defaults:
            left_path = self._field_path(step.input_schema, path)
            right_path = self._field_path(relation_set.schema, path)
            if left_path is None and right_path is None:
                raise TypeError(f"Unknown relation-set default field: {path}")
            target_path = right_path if left_path is None else left_path
            frame = target if left_path is None else source
            rendered = render_pyspark_expression(default, scope_aliases=self._scope_aliases(step))
            rendered = f"{rendered}.cast({self._schema.type(target_path[-1].type)})"
            for index in range(len(target_path) - 1, 0, -1):
                parent = ".".join(field.column for field in target_path[:index])
                rendered = (
                    f"F.col({self._literal(parent)}).withField({self._literal(target_path[index].column)}, "
                    f"{rendered})"
                )
            lines.append(f"        {frame} = {frame}.withColumn({self._literal(target_path[0].column)}, {rendered})")
        if relation_set.by_name:
            lines.append(
                f"        {target} = {target}.unionByName("
                f"{source}, allowMissingColumns={relation_set.allow_missing_columns})"
            )
            lines.append(f'        {target} = {target}.alias({self._literal(step.input_alias)})')
            return lines
        function = {
            "union_all": "union",
            "intersect": "intersect",
            "intersect_all": "intersectAll",
            "subtract": "subtract",
            "except_all": "exceptAll",
        }[relation_set.operation]
        lines.append(f"        {target} = {target}.{function}({source})")
        lines.append(f'        {target} = {target}.alias({self._literal(step.input_alias)})')
        return lines

    def _field_path(self, schema, path):
        fields = schema._structure_fields
        resolved = []
        parts = path.split(".")
        for index, name in enumerate(parts):
            field = fields.get(name)
            if field is None:
                return None
            resolved.append(field)
            if index < len(parts) - 1:
                if not isinstance(field.type, StructType):
                    return None
                fields = field.type.schema._structure_fields
        return tuple(resolved)

    def _relation_order(
        self, relation_order, *, step: PySparkStepRecipe | PySparkOutputRecipe, target: str
    ) -> list[str]:
        order = ", ".join(
            render_pyspark_expression(expression, scope_aliases=self._scope_aliases(step))
            for expression in relation_order.order_by
        )
        return [f"        {target} = {target}.orderBy({order})"]

    def _relation_bound(self, kind: str, relation_bound, *, target: str) -> list[str]:
        return [f"        {target} = {target}.{kind}({relation_bound.count})"]

    def _relation_sample(self, relation_sample, *, target: str) -> list[str]:
        arguments = [
            f"withReplacement={relation_sample.with_replacement}",
            f"fraction={relation_sample.fraction!r}",
        ]
        if relation_sample.seed is not None:
            arguments.append(f"seed={relation_sample.seed}")
        return [f"        {target} = {target}.sample({', '.join(arguments)})"]

    def _relation_priority_selection(
        self,
        selection,
        *,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        target: str,
        index: int,
    ) -> list[str]:
        aliases = self._scope_aliases(step)
        prefix = f"{target}_select_first_qualified_{index}"
        key_columns = tuple(f"__structure_priority_key_{index}_{position}" for position, _ in enumerate(selection.keys))
        priority = f"__structure_priority_order_{index}"
        rank = f"__structure_priority_rank_{index}"
        rendered_keys = tuple(render_pyspark_expression(key, scope_aliases=aliases) for key in selection.keys)
        predicate = render_pyspark_expression(selection.predicate, scope_aliases=aliases)
        order_value = render_pyspark_expression(self._order_value(selection.order_by), scope_aliases=aliases)
        ordering = render_pyspark_expression(selection.order_by, scope_aliases=aliases)
        lines = [
            f"        {prefix}_keys = {target}.select(",
            *(
                f"            {key}.alias({self._literal(column)}),"
                for key, column in zip(rendered_keys, key_columns, strict=True)
            ),
            "        )",
            f"        {prefix}_keys = {prefix}_keys.dropDuplicates({list(key_columns)!r})",
            f"        {prefix}_eligible = {target}.where(F.coalesce({predicate}, F.lit(False)))",
            f"        {prefix}_eligible = {prefix}_eligible.withColumn({self._literal(priority)}, {order_value})",
            f"        {prefix}_eligible_keys = {prefix}_eligible.select(",
            *(
                f"            {key}.alias({self._literal(column)}),"
                for key, column in zip(rendered_keys, key_columns, strict=True)
            ),
            "        )",
            f"        {prefix}_eligible_keys = {prefix}_eligible_keys.dropDuplicates({list(key_columns)!r})",
        ]
        guards: list[str] = []
        if selection.missing == "error":
            guards.append(f"{prefix}_missing_assertion")
            message = (
                "REL-E0705: select_first_qualified(...) found a key without an eligible candidate; "
                "see docs/Diagnostics.md#rel-e0705"
            )
            lines.extend(
                [
                    f"        {prefix}_missing = {prefix}_keys.join(",
                    f"            {prefix}_eligible_keys,",
                    f"            {list(key_columns)!r},",
                    '            "left_anti",',
                    "        )",
                    f"        {prefix}_missing = {prefix}_missing.agg(",
                    '            F.count(F.lit(1)).alias("__structure_violations")',
                    "        )",
                    f"        {prefix}_missing_assertion = {prefix}_missing.select(",
                    f'            F.assert_true(F.col("__structure_violations") == F.lit(0), {message!r})',
                    '            .alias("__structure_select_first_missing")',
                    "        )",
                ]
            )
        message = (
            "REL-E0705: select_first_qualified(...) found tied eligible candidates; "
            "see docs/Diagnostics.md#rel-e0705"
        )
        lines.extend(
            [
                f"        {prefix}_eligible = {prefix}_eligible.withColumn(",
                f'            "__structure_priority_tie_count_{index}",',
                f"            F.count(F.lit(1)).over(Window.partitionBy({', '.join((*rendered_keys, f'F.col({self._literal(priority)})'))})),",
                "        )",
                f"        {prefix}_eligible = {prefix}_eligible.where(",
                "            F.coalesce(",
                f"                F.when(F.col({self._literal(f'__structure_priority_tie_count_{index}')}) > F.lit(1),",
                f"                    F.assert_true(F.lit(False), {message!r})",
                "                ),",
                "                F.lit(True),",
                "            )",
                "        )",
                f"        {prefix}_ranked = {prefix}_eligible.withColumn(",
                f"            {self._literal(rank)},",
                f"            F.row_number().over(Window.partitionBy({', '.join(rendered_keys)}).orderBy({ordering})),",
                "        )",
                f"        {prefix}_ranked = {prefix}_ranked.where(F.col({self._literal(rank)}) == F.lit(1))",
            ]
        )
        if guards:
            lines.append(f"        {target} = {guards[0]}")
            for guard in guards[1:]:
                lines.append(f"        {target} = {target}.crossJoin({guard})")
            lines.append(f"        {target} = {target}.crossJoin({prefix}_ranked)")
        else:
            lines.append(f"        {target} = {prefix}_ranked")
        lines.extend(
            [
                f"        {target} = {target}.drop(",
                f"            {self._literal(rank)},",
                f"            {self._literal(priority)},",
                f'            "__structure_priority_tie_count_{index}",',
                '            "__structure_select_first_missing",',
                "        )",
            ]
        )
        return lines

    def _relation_hierarchy_closure(
        self,
        closure,
        *,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        target: str,
        index: int,
    ) -> list[str]:
        aliases = self._scope_aliases(step)
        prefix = f"{target}_hierarchy_closure_{index}"
        source_node = f"__structure_hierarchy_node_{index}"
        source_parent = f"__structure_hierarchy_parent_{index}"
        node = closure.schema._structure_fields[closure.node].column
        ancestor = closure.schema._structure_fields[closure.ancestor].column
        depth = closure.schema._structure_fields[closure.depth].column
        id_expression = render_pyspark_expression(closure.id, scope_aliases=aliases)
        parent_expression = render_pyspark_expression(closure.parent, scope_aliases=aliases)
        lines = [
            f"        {prefix}_nodes = {target}.select(",
            f"            {id_expression}.alias({self._literal(source_node)}),",
            f"            {parent_expression}.alias({self._literal(source_parent)}),",
            "        )",
            f"        {target} = {prefix}_nodes.select(",
            f"            F.col({self._literal(source_node)}).alias({self._literal(node)}),",
            f"            F.col({self._literal(source_node)}).alias({self._literal(ancestor)}),",
            f"            F.lit(0).cast(T.LongType()).alias({self._literal(depth)}),",
            "        )",
            f"        {prefix}_frontier = {prefix}_nodes",
        ]
        for depth_value in range(1, closure.max_depth + 1):
            branch = f"{prefix}_depth_{depth_value}"
            lines.extend(
                [
                    f"        {branch} = {prefix}_frontier.where(F.col({self._literal(source_parent)}).isNotNull())",
                    f"        {branch} = {branch}.select(",
                    f"            F.col({self._literal(source_node)}).alias({self._literal(node)}),",
                    f"            F.col({self._literal(source_parent)}).alias({self._literal(ancestor)}),",
                    f"            F.lit({depth_value}).cast(T.LongType()).alias({self._literal(depth)}),",
                    "        )",
                    f"        {target} = {target}.unionByName({branch}, allowMissingColumns=False)",
                    f"        {prefix}_frontier = {prefix}_frontier.where(",
                    f"            F.col({self._literal(source_parent)}).isNotNull()",
                    "        )",
                    f"        {prefix}_frontier = {prefix}_frontier.alias(\"frontier\").join(",
                    f"            {prefix}_nodes.alias(\"parent\"),",
                    f"            F.col(\"frontier.{source_parent}\") == F.col(\"parent.{source_node}\"),",
                    '            "left",',
                    "        ).select(",
                    f"            F.col(\"frontier.{source_node}\").alias({self._literal(source_node)}),",
                    f"            F.col(\"parent.{source_parent}\").alias({self._literal(source_parent)}),",
                    "        )",
                ]
            )
        return lines

    def _relation_hierarchy_fallbacks(
        self,
        fallback,
        *,
        parent_source: str,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        target: str,
        index: int,
    ) -> list[str]:
        aliases = self._scope_aliases(step)
        parent_aliases = {
            fallback.parent_input: "",
            fallback.parent_schema.__name__: "",
        }
        prefix = f"{target}_hierarchy_fallbacks_{index}"
        source_key = f"__structure_fallback_source_{index}"
        path = f"__structure_fallback_path_{index}"
        parent_node = f"__structure_fallback_parent_node_{index}"
        parent_value = f"__structure_fallback_parent_value_{index}"
        last = f"__structure_fallback_last_{index}"
        source = fallback.schema._structure_fields[fallback.source].column
        ordinal = fallback.schema._structure_fields[fallback.ordinal].column
        source_id = render_pyspark_expression(fallback.source_id, scope_aliases=aliases)
        path_expression = render_pyspark_expression(fallback.path, scope_aliases=aliases)
        parent_id = render_pyspark_expression(fallback.parent_id, scope_aliases=parent_aliases)
        parent = render_pyspark_expression(fallback.parent, scope_aliases=parent_aliases)
        lines = [
            f"        {prefix}_parents = {parent_source}.select(",
            f"            {parent_id}.alias({self._literal(parent_node)}),",
            f"            {parent}.alias({self._literal(parent_value)}),",
            "        )",
            f"        {prefix}_frontier = {target}.select(",
            f"            {source_id}.alias({self._literal(source_key)}),",
            f"            {path_expression}.alias({self._literal(path)}),",
            "        )",
            f"        {target} = {prefix}_frontier.select(",
            f"            F.col({self._literal(source_key)}).alias({self._literal(source)}),",
            f"            {self._fallback_id(fallback, path)}.alias({self._literal(fallback.schema._structure_fields[fallback.fallback].column)}),",
            f"            F.lit(0).cast(T.LongType()).alias({self._literal(ordinal)}),",
            "        )",
        ]
        for ordinal_value in range(1, fallback.max_depth + 1):
            active = f"{prefix}_active_{ordinal_value}"
            joined = f"{prefix}_joined_{ordinal_value}"
            branch = f"{prefix}_branch_{ordinal_value}"
            lines.extend(
                [
                    f"        {active} = {prefix}_frontier.where(F.size(F.col({self._literal(path)})) > F.lit(0))",
                    f"        {joined} = {active}.withColumn(",
                    f"            {self._literal(last)}, F.element_at(F.col({self._literal(path)}), F.lit(-1))",
                    "        ).join(",
                    f"            {prefix}_parents,",
                    f"            F.col({self._literal(last)}) == F.col({self._literal(parent_node)}),",
                    '            "left",',
                    "        )",
                    f"        {prefix}_frontier = {joined}.select(",
                    f"            F.col({self._literal(source_key)}).alias({self._literal(source_key)}),",
                    f"            {self._fallback_next_path(path, parent_value)}.alias({self._literal(path)}),",
                    "        )",
                    f"        {branch} = {prefix}_frontier.select(",
                    f"            F.col({self._literal(source_key)}).alias({self._literal(source)}),",
                    f"            {self._fallback_id(fallback, path)}.alias({self._literal(fallback.schema._structure_fields[fallback.fallback].column)}),",
                    f"            F.lit({ordinal_value}).cast(T.LongType()).alias({self._literal(ordinal)}),",
                    "        )",
                    f"        {target} = {target}.unionByName({branch}, allowMissingColumns=False)",
                ]
            )
        return lines

    def _fallback_next_path(self, path: str, parent: str) -> str:
        head = f"F.slice(F.col({self._literal(path)}), " f"F.lit(1), F.size(F.col({self._literal(path)})) - F.lit(1))"
        return (
            f"F.when(F.col({self._literal(parent)}).isNull(), {head})"
            f".when(F.array_contains({head}, F.col({self._literal(parent)})), {head})"
            f".otherwise(F.concat({head}, F.array(F.col({self._literal(parent)}))))"
        )

    def _fallback_id(self, fallback, path: str) -> str:
        return (
            f"F.when(F.size(F.col({self._literal(path)})) > F.lit(0), "
            f"F.sha2(F.concat_ws({fallback.separator!r}, F.col({self._literal(path)})), 256))"
        )

    def _order_value(self, expression: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
        return expression.args[0] if expression.kind == "order" else expression

    def _prepares_relation(
        self,
        duplicate_rows: PySparkDuplicateRowsRecipe,
        *,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        joined_scopes: set[str],
    ) -> bool:
        scope = duplicate_rows.scope
        return bool(
            scope
            and scope != getattr(step, "source_scope", None)
            and scope not in joined_scopes
            and any(join.input_name == scope for join in step.joins)
        )

    def _identifier(self, value: str) -> str:
        return "".join(character if character.isalnum() or character == "_" else "_" for character in value)

    def _source_for_scope(self, step: PySparkStepRecipe | PySparkOutputRecipe, scope: str) -> str:
        for join in step.joins:
            if join.input_name == scope:
                return join.source
        return scope

    def _watermark(self, watermark: PySparkWatermarkRecipe, *, target: str) -> list[str]:
        return [f"        {target} = {target}.withWatermark({self._literal(watermark.column)}, {watermark.delay!r})"]

    def _field_column(self, expression: PySparkExpressionRecipe) -> str:
        if expression.kind != "field":
            raise TypeError("drop_duplicates(...) subset can only render field expressions")
        return str(expression.data["field"])

    def _right_watermarks(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        join: PySparkJoinRecipe,
    ) -> tuple[PySparkWatermarkRecipe, ...]:
        return tuple(
            operation.watermark
            for operation in step.operations
            if operation.kind == "watermark"
            and operation.watermark is not None
            and operation.watermark.scope == join.input_name
        )

    def _selected_rows(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        selected_rows: PySparkSelectedRowsRecipe,
        *,
        target: str,
    ) -> list[str]:
        rank = f"__structure_{step.name}_{selected_rows.direction}_rank"
        aliases = self._scope_aliases(step)
        partition = ", ".join(
            render_pyspark_expression(expression, scope_aliases=aliases) for expression in selected_rows.partition_by
        )
        order_by = render_pyspark_expression(selected_rows.order_by, scope_aliases=aliases)
        ordering = f"{order_by}.desc()" if selected_rows.direction == "latest" else f"{order_by}.asc()"
        window = f"Window.partitionBy({partition}).orderBy({ordering})"
        return [
            f'        {target} = {target}.withColumn("{rank}", F.row_number().over({window}))',
            f'        {target} = {target}.where(F.col("{rank}") == F.lit(1))',
            f'        {target} = {target}.drop("{rank}")',
        ]

    def _aggregate_having(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        aggregate: PySparkAggregateRecipe,
        *,
        target: str,
    ) -> list[str]:
        if aggregate.having is None:
            return []
        aliases = {**self._scope_aliases(step), step.output_schema.__name__: ""}
        predicate = render_pyspark_expression(aggregate.having, scope_aliases=aliases)
        return [f"        {target} = {target}.where({predicate})"]

    def _grouping_set_select(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        aggregate: PySparkAggregateRecipe,
        level: set[str],
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        alias = self._literal(assignment.field.column)
        if assignment.function == "key":
            if assignment.key in level:
                column = self._grouping_set_key_column(assignment, key_columns=key_columns)
                return f"F.col({self._literal(column)}).alias({alias})"
            return f"F.lit(None).cast({self._schema.type(assignment.field.type)}).alias({alias})"
        if assignment.function == "grouping_id":
            mask = self._grouping_id(aggregate, level=level)
            return f"F.lit({mask}).cast({self._schema.type(assignment.field.type)}).alias({alias})"
        if assignment.function == "is_grouped":
            key = self._grouping_set_expression_key(assignment, aggregate=aggregate)
            grouped = "True" if key not in level else "False"
            return f"F.lit({grouped}).cast({self._schema.type(assignment.field.type)}).alias({alias})"
        return f"F.col({alias})"

    def _grouping_id(self, aggregate: PySparkAggregateRecipe, *, level: set[str]) -> int:
        mask = 0
        key_count = len(aggregate.keys)
        for index, key in enumerate(aggregate.keys):
            if key.name not in level:
                mask |= 1 << (key_count - index - 1)
        return mask

    def _grouping_set_expression_key(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        aggregate: PySparkAggregateRecipe,
    ) -> str:
        if assignment.expression is None:
            raise TypeError("is_grouped(...) requires a grouping expression")
        for key in aggregate.keys:
            if assignment.expression == key.expression:
                return key.name
        raise TypeError("is_grouped(...) expression must match a grouping_sets(...) key")

    def _grouping_set_key_column(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        for key, column in key_columns:
            if key.name == assignment.key:
                return column
        raise TypeError(f"Missing aggregate key column for {assignment.key}")

    def _aggregate_assignment(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        step,
        aggregate: PySparkAggregateRecipe,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
        backend_target: str = ">=3.5,<4.1",
    ) -> str:
        alias = self._literal(assignment.field.column)
        if assignment.function == "count":
            expression = "F.lit(1)"
            if assignment.filter is not None:
                predicate = render_pyspark_expression(assignment.filter, scope_aliases=self._scope_aliases(step))
                expression = f"F.when({predicate}, F.lit(1))"
            return f"F.count({expression}).cast({self._schema.type(assignment.field.type)}).alias({alias})"
        if (
            assignment.function == "collect_list"
            and assignment.order_by is not None
            and assignment.expression is not None
        ):
            return self._ordered_collect_list(assignment, step=step, alias=alias)
        if assignment.function == "mode" and assignment.expression is not None:
            options = dict(assignment.options)
            expression = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
            if assignment.filter is not None:
                predicate = render_pyspark_expression(assignment.filter, scope_aliases=self._scope_aliases(step))
                expression = f"F.when({predicate}, {expression})"
            if options.get("deterministic") is True:
                expression = self._deterministic_mode(expression)
            else:
                expression = f"F.mode({expression})"
            return f"{expression}.cast({self._schema.type(assignment.field.type)})" f".alias({alias})"
        if assignment.function == "grouping_id":
            return f"F.grouping_id().cast({self._schema.type(assignment.field.type)}).alias({alias})"
        if assignment.function == "is_grouped" and assignment.expression is not None:
            expression = self._aggregate_grouping_expression(
                assignment,
                step=step,
                aggregate=aggregate,
                key_columns=key_columns,
            )
            return f"F.grouping({expression}).cast({self._schema.type(assignment.field.type)}).alias({alias})"
        arguments = assignment.arguments or (() if assignment.expression is None else (assignment.expression,))
        if assignment.function in self._aggregate_functions() and arguments:
            rendered_arguments = [
                render_pyspark_expression(argument, scope_aliases=self._scope_aliases(step)) for argument in arguments
            ]
            if assignment.filter is not None:
                predicate = render_pyspark_expression(assignment.filter, scope_aliases=self._scope_aliases(step))
                rendered_arguments[0] = f"F.when({predicate}, {rendered_arguments[0]})"
            function = self._aggregate_function(assignment.function)
            options = dict(assignment.options)
            if assignment.function == "approx_count_distinct" and "relative_sd" in options:
                rendered_arguments.append(repr(options["relative_sd"]))
            if assignment.function == "approx_percentile":
                rendered_arguments.append(repr(options["percentage"]))
                if "accuracy" in options:
                    rendered_arguments.append(repr(options["accuracy"]))
            if assignment.function == "percentile":
                rendered_arguments.extend((repr(options["percentage"]), repr(options["frequency"])))
            expression = f"{function}({', '.join(rendered_arguments)})"
            if not self._keeps_struct_collection_type(assignment, backend_target=backend_target):
                expression = f"{expression}.cast({self._schema.type(assignment.field.type)})"
            return f"{expression}.alias({alias})"
        if assignment.function in {"first_value", "last_value"} and assignment.expression is not None:
            if assignment.order_by is None:
                raise TypeError(f"{assignment.function}(...) requires order_by")
            value = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
            order_by = render_pyspark_expression(assignment.order_by, scope_aliases=self._scope_aliases(step))
            if assignment.filter is not None:
                predicate = render_pyspark_expression(assignment.filter, scope_aliases=self._scope_aliases(step))
                order_by = f"F.when({predicate}, {order_by})"
            function = "F.min_by" if assignment.function == "first_value" else "F.max_by"
            return f"{function}({value}, {order_by}).alias({alias})"
        if assignment.function == "first" and assignment.expression is not None:
            expression = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
            return f"F.first({expression}, ignorenulls=False).alias({alias})"
        raise TypeError(f"Unsupported aggregate assignment: {assignment.function}")

    def _ordered_collect_list(self, assignment: PySparkAggregateAssignment, *, step, alias: str) -> str:
        order = assignment.order_by
        assert order is not None and assignment.expression is not None
        descending = order.kind == "order" and order.data.get("direction") == "desc"
        key = order.args[0] if order.kind == "order" else order
        value = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
        rendered_key = render_pyspark_expression(key, scope_aliases=self._scope_aliases(step))
        condition = f"{value}.isNotNull()"
        if assignment.filter is not None:
            predicate = render_pyspark_expression(assignment.filter, scope_aliases=self._scope_aliases(step))
            condition = f"({predicate}) & ({condition})"
        item = f"F.struct({rendered_key}.alias('_structure_order'), {value}.alias('_structure_value'))"
        collected = f"F.collect_list(F.when({condition}, {item}))"
        return f"F.transform(F.sort_array({collected}, asc={not descending}), lambda item: item.getField('_structure_value')).alias({alias})"

    def _keeps_struct_collection_type(self, assignment: PySparkAggregateAssignment, *, backend_target: str) -> bool:
        return (
            backend_target == ">=3.5,<4.0"
            and assignment.function in {"collect_list", "collect_set"}
            and isinstance(assignment.field.type, ArrayType)
            and isinstance(assignment.field.type.element, StructType)
        )

    def _deterministic_mode(self, expression: str) -> str:
        collected = f"F.collect_list({expression})"
        counts = (
            f"F.transform(F.array_distinct({collected}), "
            f"lambda candidate: F.struct(candidate.alias('_structure_value'), "
            f"F.size(F.filter({collected}, lambda item: item == candidate)).alias('_structure_count')))"
        )
        max_count = f"F.array_max(F.transform({counts}, lambda item: item.getField('_structure_count')))"
        tied = (
            f"F.transform(F.filter({counts}, lambda item: item.getField('_structure_count') == {max_count}), "
            f"lambda item: item.getField('_structure_value'))"
        )
        return f"F.array_min({tied})"

    def _aggregate_grouping_expression(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        step,
        aggregate: PySparkAggregateRecipe,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        if assignment.expression is None:
            raise TypeError("is_grouped(...) requires a grouping expression")
        for key in aggregate.keys:
            if assignment.expression == key.expression:
                return self._literal(self._aggregate_key_column(key, key_columns))
        return render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))

    def _aggregate_functions(self) -> set[str]:
        return {
            "approx_count_distinct",
            "approx_percentile",
            "avg",
            "bool_and",
            "bool_or",
            "collect_list",
            "collect_set",
            "corr",
            "covar",
            "count_distinct",
            "max",
            "mode",
            "min",
            "kurtosis",
            "percentile",
            "schema_of_variant_agg",
            "skewness",
            "stddev",
            "sum",
            "variance",
        }

    def _aggregate_function(self, function: str) -> str:
        return {
            "approx_count_distinct": "F.approx_count_distinct",
            "approx_percentile": "F.percentile_approx",
            "avg": "F.avg",
            "bool_and": "F.bool_and",
            "bool_or": "F.bool_or",
            "collect_list": "F.collect_list",
            "collect_set": "F.collect_set",
            "corr": "F.corr",
            "covar": "F.covar_samp",
            "count_distinct": "F.countDistinct",
            "max": "F.max",
            "mode": "F.mode",
            "min": "F.min",
            "kurtosis": "F.kurtosis",
            "percentile": "F.percentile",
            "schema_of_variant_agg": "F.schema_of_variant_agg",
            "skewness": "F.skewness",
            "stddev": "F.stddev",
            "sum": "F.sum",
            "variance": "F.variance",
        }[function]

    def _aggregate_select(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        source = self._aggregate_select_source(assignment, key_columns=key_columns)
        expression = f"F.col({self._literal(str(source))})"
        if str(source) != assignment.field.column:
            expression = f"{expression}.alias({self._literal(assignment.field.column)})"
        return expression

    def _aggregate_select_source(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        if assignment.function == "key":
            for key, column in key_columns:
                if key.name == assignment.key:
                    return column
            if assignment.key is not None:
                return assignment.key
        return assignment.field.column

    def _aggregate_key_columns(
        self,
        aggregate: PySparkAggregateRecipe,
    ) -> tuple[tuple[PySparkAggregateKey, str], ...]:
        return tuple((key, f"__structure_group_{index}_{key.name}") for index, key in enumerate(aggregate.keys))

    def _aggregate_key_column(
        self,
        target: PySparkAggregateKey,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        for key, column in key_columns:
            if key == target:
                return column
        raise TypeError(f"Missing aggregate key column for {target.name}")

    def _join(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        join: PySparkJoinRecipe,
        *,
        sources: dict[str, str],
        target: str,
    ) -> list[str]:
        source = sources.get(join.source, join.source)
        right = source
        for watermark in self._right_watermarks(step, join):
            right = f"{right}.withWatermark({self._literal(watermark.column)}, {watermark.delay!r})"
        if join.strategy is not None:
            right = f'{right}.hint("{join.strategy.hint()}")'
        if join.assert_singleton_in_batch:
            prepared = f"{join.right_alias}_param_joined"
            lines = [f"        {prepared} = {right}", "        if not __structure_streaming_step:"]
            lines.extend(
                f"    {line}" for line in self._exactly_one(right, prepared, join.input_name, index=join.occurrence)
            )
            right = prepared
        else:
            lines = []
        right = f'{right}.alias("{join.right_alias}")'
        if join.dedupe is not None:
            right = self._dedupe(join, right=right)
        if join.hint is not None and join.hint.value == "broadcast":
            right = f"F.broadcast({right})"
        predicate = self._predicate(step, join)
        right_name = f"{join.right_alias}_joined"
        row_id = None
        if join.as_of is not None:
            row_id = f"__structure_{join.left_alias}_{join.right_alias}_row"
            lines.append(f'        {target} = {target}.withColumn("{row_id}", F.monotonically_increasing_id())')
        lines.append(f"        {right_name} = {right}")
        if join.how is Join.CROSS:
            lines.append(f"        {target} = {target}.crossJoin({right_name})")
        else:
            lines.extend(
                [
                    f"        {target} = {target}.join(",
                    f"            {right_name},",
                    f"            {predicate},",
                    f'            "{self._join_mode(join)}",',
                    "        )",
                ]
            )
        if join.as_of is not None:
            lines.extend(self._as_of(join, step=step, target=target, row_id=cast(str, row_id)))
        return lines

    def _streaming_guard(self, step: PySparkStepRecipe | PySparkOutputRecipe, *, sources: dict[str, str]) -> list[str]:
        frame_names = dict.fromkeys((step.source, *getattr(step, "input_sources", ())))
        frames = [sources.get(source, source.removeprefix("input:")) for source in frame_names]
        expression = " or ".join(f"{frame}.isStreaming" for frame in frames)
        return [f"        __structure_streaming_step = {expression or 'False'}"]

    def _predicate(self, step: PySparkStepRecipe | PySparkOutputRecipe, join: PySparkJoinRecipe) -> str:
        aliases = self._scope_aliases(step, join)
        predicate = render_pyspark_expression(join.predicate, scope_aliases=aliases)
        if join.temporal is None and join.as_of is None:
            return predicate
        if join.temporal is not None:
            at = render_pyspark_expression(join.temporal.at, scope_aliases=aliases)
            valid_from = render_pyspark_expression(join.temporal.valid_from, scope_aliases=aliases)
            valid_to = render_pyspark_expression(join.temporal.valid_to, scope_aliases=aliases)
            valid_window = f"(({valid_from} <= {at}) & (({at} < {valid_to}) | {valid_to}.isNull()))"
            predicate = f"({predicate} & {valid_window})"
        if join.as_of is not None:
            left_time = render_pyspark_expression(join.as_of.left_time, scope_aliases=aliases)
            right_time = render_pyspark_expression(join.as_of.right_time, scope_aliases=aliases)
            if join.as_of.direction.value == "nearest":
                as_of = f"({left_time}.isNotNull() & {right_time}.isNotNull())"
                if join.as_of.tolerance is not None:
                    tolerance = render_pyspark_expression(join.as_of.tolerance, scope_aliases=aliases)
                    as_of = f"({as_of} & (F.abs({right_time} - {left_time}) <= {tolerance}))"
            else:
                comparator = "<=" if join.as_of.direction.value == "backward" else ">="
                as_of = f"({right_time} {comparator} {left_time})"
                if join.as_of.tolerance is not None:
                    tolerance = render_pyspark_expression(join.as_of.tolerance, scope_aliases=aliases)
                    bound = ">=" if join.as_of.direction.value == "backward" else "<="
                    arithmetic = "-" if join.as_of.direction.value == "backward" else "+"
                    as_of = f"({as_of} & ({right_time} {bound} ({left_time} {arithmetic} {tolerance})))"
            predicate = f"({predicate} & {as_of})"
        return predicate

    def _as_of(
        self,
        join: PySparkJoinRecipe,
        *,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        target: str,
        row_id: str,
    ) -> list[str]:
        as_of = join.as_of
        if as_of is None:
            raise TypeError("Cannot render as-of lookup without an as-of recipe")
        if as_of.direction.value == "nearest":
            return self._nearest_as_of(join, step=step, target=target, row_id=row_id)
        rank = f"__structure_{join.right_alias}_as_of_rank"
        right_time = render_pyspark_expression(as_of.right_time, scope_aliases={join.input_name: join.right_alias})
        order = "desc" if as_of.direction.value == "backward" else "asc"
        window = f'Window.partitionBy(F.col("{row_id}")).orderBy({right_time}.{order}())'
        return [
            f'        {target} = {target}.withColumn("{rank}", F.row_number().over({window}))',
            f'        {target} = {target}.where(F.col("{rank}") == F.lit(1))',
            f'        {target} = {target}.drop("{rank}").drop("{row_id}")',
        ]

    def _nearest_as_of(
        self,
        join: PySparkJoinRecipe,
        *,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        target: str,
        row_id: str,
    ) -> list[str]:
        as_of = join.as_of
        if as_of is None:
            raise TypeError("Cannot render nearest as-of lookup without an as-of recipe")
        aliases = self._scope_aliases(step, join)
        left_time = render_pyspark_expression(as_of.left_time, scope_aliases=aliases)
        right_time = render_pyspark_expression(as_of.right_time, scope_aliases=aliases)
        distance = f"__structure_{join.right_alias}_as_of_distance"
        minimum = f"__structure_{join.right_alias}_as_of_min_distance"
        ties = f"__structure_{join.right_alias}_as_of_tie_count"
        rank = f"__structure_{join.right_alias}_as_of_rank"
        partition = f'Window.partitionBy(F.col("{row_id}"))'
        message = (
            "JOIN-E0601: as_of_one(direction='nearest', ties='error') found equidistant matches; "
            "add a tolerance or make the right-side time unique"
        )
        return [
            f'        {target} = {target}.withColumn("{distance}", F.abs({right_time} - {left_time}))',
            f'        {target} = {target}.withColumn("{minimum}", F.min(F.col("{distance}")).over({partition}))',
            f'        {target} = {target}.withColumn(',
            f'            "{ties}",',
            f'            F.sum(',
            f'                F.when(F.col("{distance}") == F.col("{minimum}"), F.lit(1)).otherwise(F.lit(0))',
            f"            ).over({partition}),",
            f"        )",
            f"        {target} = {target}.where(",
            f'            F.assert_true((F.col("{ties}") <= F.lit(1)) | F.col("{minimum}").isNull(), {message!r})',
            f"            .isNull()",
            f"        )",
            f'        {target} = {target}.withColumn(',
            f'            "{rank}",',
            f'            F.row_number().over({partition}.orderBy(F.col("{distance}").asc(), {right_time}.asc())),',
            f"        )",
            f'        {target} = {target}.where(F.col("{rank}") == F.lit(1))',
            f'        {target} = {target}.drop("{rank}", "{distance}", "{minimum}", "{ties}").drop("{row_id}")',
        ]

    def _dedupe(self, join: PySparkJoinRecipe, *, right: str) -> str:
        dedupe = join.dedupe
        if dedupe is None:
            raise TypeError("Cannot render lookup dedupe without a dedupe recipe")
        rank = f"__structure_{join.right_alias}_rank"
        partition = ", ".join(
            render_pyspark_expression(key, scope_aliases={join.input_name: join.right_alias})
            for key in self._right_keys(join)
        )
        order_by = render_pyspark_expression(dedupe.order_by, scope_aliases={join.input_name: join.right_alias})
        ordering = f"{order_by}.desc()" if dedupe.direction == "latest" else f"{order_by}.asc()"
        window = f"Window.partitionBy({partition}).orderBy({ordering})"
        return (
            f'{right}.withColumn("{rank}", F.row_number().over({window}))'
            f'.where(F.col("{rank}") == F.lit(1))'
            f'.drop("{rank}")'
            f'.alias("{join.right_alias}")'
        )

    def _right_keys(self, join: PySparkJoinRecipe) -> tuple[PySparkExpressionRecipe, ...]:
        return tuple(self._right_key(join, condition) for condition in self._join_conditions(join.predicate))

    def _right_key(self, join: PySparkJoinRecipe, condition: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
        left, right = condition.args
        if join.input_name in self._scopes(left):
            return left
        return right

    def _join_conditions(self, expression: PySparkExpressionRecipe) -> tuple[PySparkExpressionRecipe, ...]:
        if expression.kind == "and":
            return tuple(condition for argument in expression.args for condition in self._join_conditions(argument))
        if expression.kind in {"eq", "null_safe_eq"}:
            return (expression,)
        return ()

    def _scopes(self, expression: PySparkExpressionRecipe) -> set[str]:
        scopes = set().union(*(self._scopes(argument) for argument in expression.args))
        if expression.kind == "field" and "scope" in expression.data:
            scopes.add(str(expression.data["scope"]))
        return scopes

    def _join_mode(self, join: PySparkJoinRecipe) -> str:
        if join.method is JoinMethod.EXISTS:
            return "left_semi"
        if join.method is JoinMethod.NOT_EXISTS:
            return "left_anti"
        return join.how.value

    def _projection(self, step: PySparkStepRecipe | PySparkOutputRecipe, *, target: str) -> list[str]:
        if not step.projection:
            return []
        lines = [f"        {target} = {target}.select("]
        for assignment in step.projection:
            lines.append(f"            {self._assignment(assignment, scope_aliases=self._scope_aliases(step))},")
        lines.append("        )")
        return lines

    def _assignment(self, assignment, *, scope_aliases: dict[str, str]) -> str:
        expression = render_pyspark_expression(assignment.expression, scope_aliases=scope_aliases)
        if self._needs_cast(assignment):
            expression = f"{expression}.cast({self._schema.type(assignment.field.type)})"
        if self._needs_alias(assignment):
            return f"{expression}.alias({self._literal(assignment.field.column)})"
        return expression

    def _needs_cast(self, assignment) -> bool:
        if isinstance(assignment.field.type, StructType):
            return False
        if assignment.expression.type is None:
            return True
        if not self._same_type(assignment.expression.type, assignment.field.type):
            return True
        return False

    def _needs_alias(self, assignment) -> bool:
        if assignment.expression.kind != "field":
            return True
        return assignment.expression.data["field"] != assignment.field.column

    def _same_type(self, actual: StructureType, target: StructureType) -> bool:
        if actual.name != target.name:
            return False
        if isinstance(actual, DecimalType) and isinstance(target, DecimalType):
            return actual.precision == target.precision and actual.scale == target.scale
        return actual == target or actual.__class__.__name__.removesuffix("Type") == target.__class__.__name__

    def _target(self, step: PySparkStepRecipe | PySparkOutputRecipe) -> str:
        if isinstance(step, PySparkStepRecipe):
            return step.results[0].frame
        return step.name

    def _validations(
        self,
        validations: tuple[PySparkValidationRecipe, ...],
        *,
        target: str = "df",
    ) -> list[str]:
        lines: list[str] = []
        for validation in validations:
            schema = self._schema.constant_name(validation.schema)
            if validation.check:
                lines.append(
                    f'        assert_schema({target}, {schema}, '
                    f'name="{validation.schema.__name__}", mode="{validation.mode.value}")'
                )
            if validation.project:
                lines.append(f"        {target} = project_schema({target}, {schema})")
            if validation.boundary:
                lines.append(f"        {target} = apply_plan_boundary({target}, self.spark)")
        return lines

    def _scope_aliases(
        self, step: PySparkStepRecipe | PySparkOutputRecipe, join: PySparkJoinRecipe | None = None
    ) -> dict[str, str]:
        aliases = {
            step.input_schema.__name__: step.input_alias,
        }
        source_scope = getattr(step, "source_scope", None)
        if source_scope is not None:
            aliases[source_scope] = step.input_alias
        if step.ordinal == 0:
            aliases["orders"] = step.input_alias
        if join is None and not step.joins and any(operation.relation_set is not None for operation in step.operations):
            if source_scope is not None:
                aliases[source_scope] = ""
            aliases[step.input_schema.__name__] = ""
        for item in step.joins:
            aliases[item.input_name] = item.right_alias
        for operation in step.operations:
            if operation.posexplode_struct is not None:
                aliases.update(self._struct_generator_renderer.aliases(step))
            if operation.scalar_generator is not None:
                aliases.update(self._scalar_generator_renderer.aliases(step))
            if operation.map_generator is not None:
                aliases.update(self._map_generator_renderer.aliases(step))
            if operation.ordered_timeline_scan is not None:
                aliases[operation.ordered_timeline_scan.row_scope] = ""
                aliases[operation.ordered_timeline_scan.scope] = ""
                aliases[step.input_schema.__name__] = ""
                source_scope = getattr(step, "source_scope", None)
                if source_scope is not None:
                    aliases[source_scope] = ""
            if operation.relation_hierarchy_closure is not None:
                aliases[operation.relation_hierarchy_closure.scope] = ""
            if operation.relation_hierarchy_fallback is not None:
                aliases[operation.relation_hierarchy_fallback.scope] = ""
        if join is not None:
            aliases[join.input_name] = join.right_alias
        return aliases

    def _literal(self, value: str) -> str:
        return json.dumps(value)


def render_pyspark_step(
    step: PySparkStepRecipe | PySparkOutputRecipe,
    *,
    current: str,
    sources: dict[str, str] | None = None,
    source_transform: str | None = None,
    generated_hooks: bool = False,
    backend_target: str = ">=3.5,<4.1",
) -> str:
    """Render a step through the legacy function-shaped entry point."""
    return RenderPySparkStep()(
        step,
        current=current,
        sources=sources,
        source_transform=source_transform,
        generated_hooks=generated_hooks,
        backend_target=backend_target,
    )
