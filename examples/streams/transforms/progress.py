from examples.streams.schemas.events import GateProgress, Passage
from structure import StreamingMode, Transform, count, group_by, input, max, min, output, transform, watermark


@transform(streaming_compatible=True)
class BuildGateProgress(Transform):
    passages = input(Passage, streaming=StreamingMode.YES)
    progress = output(GateProgress)

    def summarize(self, passage: Passage) -> GateProgress:
        watermark(passage.at, delay="10 minutes")
        group_by(
            race_id=passage.race_id,
            run_id=passage.run_id,
            gate_number=passage.gate_number,
        )
        return GateProgress(
            race_id=passage.race_id,
            run_id=passage.run_id,
            gate_number=passage.gate_number,
            passage_count=count(),
            fastest_millis=min(passage.elapsed_millis),
            slowest_millis=max(passage.elapsed_millis),
        )
