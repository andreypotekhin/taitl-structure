from examples.streams.schemas.events import GateProgress, Passage
from structure import Transform, input, output, transform
from structure.plugin.pyspark import *


@transform(streaming=True)
class BuildGateProgress(Transform):
    passages = input(Passage, streaming=True)
    progress = output(GateProgress)

    def summarize(self, passage: Passage) -> GateProgress:
        watermark(passage.at, delay="10 minutes")
        group_by(
            race_id=passage.race_id,
            run_id=passage.run_id,
            gate_number=passage.gate_number,
        )
        return GateProgress.project(passage)(
            passage_count=count(),
            fastest_millis=min(passage.elapsed_millis),
            slowest_millis=max(passage.elapsed_millis),
        )
