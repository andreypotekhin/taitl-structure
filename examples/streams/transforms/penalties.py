from examples.streams.schemas.events import JudgeCall, Passage, Penalty
from structure import StreamingMode, Transform, input, output, transform
from structure.plugin.pyspark import *


@transform(streaming_compatible=True)
class CorrelatePenalties(Transform):
    passages = input(Passage, streaming=StreamingMode.YES)
    calls = input(JudgeCall, streaming=StreamingMode.YES)
    penalties = output(Penalty)

    def correlate(self, passage: Passage, call: JudgeCall) -> Penalty:
        watermark(passage.at, delay="10 minutes")
        watermark(call.at, delay="10 minutes")
        inner_join(
            call,
            on=(call.race_id == passage.race_id)  # type: ignore[operator]
            & (call.run_id == passage.run_id)
            & (call.paddler_id == passage.paddler_id)
            & (call.gate_number == passage.gate_number)
            & event_time_between(passage.at, call.at, upper="5 minutes"),
        )
        return Penalty(
            event_id=passage.id,
            call_id=call.id,
            race_id=passage.race_id,
            run_id=passage.run_id,
            paddler_id=passage.paddler_id,
            gate_number=passage.gate_number,
            elapsed_millis=passage.elapsed_millis,
            penalty_code=call.code,
            penalty_seconds=call.penalty_seconds,
            adjusted_millis=passage.elapsed_millis + call.penalty_seconds * 1000,  # type: ignore[operator]
        )
