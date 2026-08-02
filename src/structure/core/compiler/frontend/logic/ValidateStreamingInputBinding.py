from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from structure.plugin.api.v1.model.InputPlan import InputPlan
from structure.plugin.api.v1.model.OutputPlan import OutputPlan


@dataclass(frozen=True)
class StreamingInputBindingViolation:
    problem: str
    use: str
    context: dict[str, str]


class ValidateStreamingInputBinding:
    """Validate the declared mode at a composed output-to-input boundary."""

    def __call__(
        self,
        *,
        producer: str,
        output: OutputPlan,
        consumer: str,
        input_plan: InputPlan,
        consumer_options: Mapping[str, object] | None,
        allow_stream_to_batch: bool,
    ) -> StreamingInputBindingViolation | None:
        if not output.streaming or input_plan.streaming:
            return None

        options = consumer_options or {}
        explicit_batch = input_plan.streaming_declared or options.get("streaming") is False
        if not explicit_batch and bool(options.get("allow_stream_to_batch", allow_stream_to_batch)):
            return None

        if explicit_batch:
            problem = (
                f"Streaming output {producer}.{output.name} is consumed by {consumer}.{input_plan.name}, "
                "which explicitly declares streaming=False."
            )
            use = (
                "Remove streaming=False and declare the input streaming=True when the transform supports streaming; "
                "otherwise keep the transform outside the streaming pipeline."
            )
        else:
            problem = (
                f"Streaming output {producer}.{output.name} is consumed by {consumer}.{input_plan.name}, "
                "but the downstream input is not declared streaming=True."
            )
            use = (
                "Declare the downstream input with streaming=True, or explicitly allow the stream-to-batch boundary "
                "with allow_stream_to_batch=True."
            )
        return StreamingInputBindingViolation(
            problem=problem,
            use=use,
            context={
                "producer": producer,
                "output": output.name,
                "consumer": consumer,
                "input": input_plan.name,
            },
        )


validate_streaming_input_binding = ValidateStreamingInputBinding()
