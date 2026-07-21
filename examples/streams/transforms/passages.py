from examples.streams.schemas.events import Passage, RawEvent
from examples.streams.schemas.race import Gate, Paddler, Race
from structure import StreamingMode, Transform, input, output, transform
from structure.plugin.pyspark import *


@transform(streaming_compatible=True)
class PreparePassages(Transform):
    events = input(RawEvent, streaming=StreamingMode.YES)
    races = input(Race)
    paddlers = input(Paddler)
    gates = input(Gate)
    passages = output(Passage)

    def prepare(self, event: RawEvent, race: Race, paddler: Paddler, gate: Gate) -> Passage:
        where(event.elapsed_millis >= 0)  # type: ignore[operator]
        watermark(event.at, delay="10 minutes")
        left_join(race, on=race.id == event.race_id)
        left_join(
            paddler,
            on=(paddler.race_id == event.race_id) & (paddler.id == event.paddler_id),
        )
        left_join(
            gate,
            on=(gate.race_id == event.race_id) & (gate.number == event.gate_number),
        )
        drop_duplicates(event.id)
        return Passage(
            id=event.id,
            race_id=event.race_id,
            run_id=event.run_id,
            paddler_id=event.paddler_id,
            gate_number=event.gate_number,
            at=event.at,
            sequence=event.sequence,
            elapsed_millis=event.elapsed_millis,
            source=event.source,
            race_name=race.name,
            race_date=race.date,
            river=race.river,
            venue=race.venue,
            city=race.city,
            race_country=race.country,
            paddler_name=paddler.name,
            bib=paddler.bib,
            division=paddler.division,
            paddler_country=paddler.country,
            gate_direction=gate.direction,
            sector=gate.sector,
        )
