import structure
from examples.streams.schemas.events import GateProgress, Passage


@structure.transform(streaming_compatible=True)
class BuildGateProgress(structure.Transform):
    passages = structure.input(Passage, streaming=structure.StreamingMode.YES)
    progress = structure.output(GateProgress)

    def summarize(self, passage: Passage) -> GateProgress:
        structure.watermark(passage.at, delay="10 minutes")
        return (
            structure.group_by(
                race_id=passage.race_id,
                run_id=passage.run_id,
                gate_number=passage.gate_number,
            )
            .agg(
                passage_count=structure.count(),
                fastest_millis=structure.min(passage.elapsed_millis),
                slowest_millis=structure.max(passage.elapsed_millis),
            )
            .as_schema(GateProgress)
        )
