import json

from structure.plugin.api.v1 import CompileRequest, PluginCompilation

from ..dsl.operations import IterablePlan


class Compiler:
    """Lowers the vendor DSL attached to an authored transform into an opaque payload."""

    def compile(self, request: CompileRequest) -> PluginCompilation:
        plans = [value for value in vars(request.transform).values() if isinstance(value, IterablePlan)]
        if len(plans) != 1:
            raise TypeError("Iterable transforms must declare exactly one IterablePlan class attribute.")
        payload = self._resolved_payload(plans[0], request.transform)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return PluginCompilation(lowered=payload, fingerprint=f"iterable:{encoded}")

    def _resolved_payload(self, plan: IterablePlan, transform: object) -> dict[str, object]:
        payload = dict(plan.payload)
        if payload.get("operation") != "recurrence":
            return payload
        if "input" not in payload:
            payload["input"] = self._sole_input(transform)
        if "value" not in payload:
            payload["value"] = self._recurrence_value(transform)
        return payload

    def _sole_input(self, transform: object) -> str:
        inputs = getattr(transform, "_structure_inputs", {})
        if len(inputs) != 1:
            raise TypeError("Iterable recurrences need exactly one declared input or an explicit input= name.")
        return next(iter(inputs))

    def _recurrence_value(self, transform: object) -> str:
        inputs = getattr(transform, "_structure_inputs", {})
        outputs = getattr(transform, "_structure_outputs", {})
        if len(outputs) != 1:
            raise TypeError("Iterable recurrences need one declared output or an explicit value= name.")
        input_fields = {field for declaration in inputs.values() for field in declaration.schema._structure_fields}
        output = next(iter(outputs.values()))
        values = set(output.schema._structure_fields) - input_fields
        if len(values) != 1:
            raise TypeError("Iterable recurrence output must add exactly one field or declare value= explicitly.")
        return values.pop()
