import structure
from examples.streams.schemas.events import JudgeCall, Passage, Penalty


@structure.transform(streaming_compatible=True)
class CorrelatePenalties(structure.Transform):
    passages = structure.input(Passage, streaming=structure.StreamingMode.YES)
    calls = structure.input(JudgeCall, streaming=structure.StreamingMode.YES)
    penalties = structure.output(Penalty)

    def correlate(self, passage: Passage, call: JudgeCall) -> Penalty:
        structure.watermark(passage.at, delay="10 minutes")
        structure.watermark(call.at, delay="10 minutes")
        structure.inner_join(
            call,
            on=(call.race_id == passage.race_id)  # type: ignore[operator]
            & (call.run_id == passage.run_id)
            & (call.paddler_id == passage.paddler_id)
            & (call.gate_number == passage.gate_number)
            & structure.event_time_between(passage.at, call.at, upper="5 minutes"),
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
