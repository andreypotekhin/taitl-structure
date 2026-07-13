from examples.streams.schemas.race import Race
from structure import Date, Integer, Long, Schema, String, Timestamp, field


class RawEvent(Schema):
    id = field(String(), nullable=False, primary_key=True)
    race_id = field(String(), nullable=False)
    run_id = field(String(), nullable=False)
    paddler_id = field(String(), nullable=False)
    gate_number = field(Integer(), nullable=False)
    at = field(Timestamp(), nullable=False)
    sequence = field(Long(), nullable=False)
    elapsed_millis = field(Long(), nullable=False)
    source = field(String(), nullable=False)


class Passage(RawEvent):
    race_name = field(String(), nullable=True)
    race_date = field(Date(), nullable=True)
    river = field(String(), nullable=True)
    venue = field(String(), nullable=True)
    city = field(String(), nullable=True)
    race_country = field(String(), nullable=True)
    paddler_name = field(String(), nullable=True)
    bib = field(Integer(), nullable=True)
    division = field(String(), nullable=True)
    paddler_country = field(String(), nullable=True)
    gate_direction = field(String(), nullable=True)
    sector = field(String(), nullable=True)


class JudgeCall(Schema):
    id = field(String(), nullable=False, primary_key=True)
    race_id = field(String(), nullable=False)
    run_id = field(String(), nullable=False)
    paddler_id = field(String(), nullable=False)
    gate_number = field(Integer(), nullable=False)
    at = field(Timestamp(), nullable=False)
    code = field(String(), nullable=False)
    penalty_seconds = field(Integer(), nullable=False)


class GateProgress(Schema):
    race_id = field(String(), nullable=False)
    run_id = field(String(), nullable=False)
    gate_number = field(Integer(), nullable=False)
    passage_count = field(Long(), nullable=False)
    fastest_millis = field(Long(), nullable=True)
    slowest_millis = field(Long(), nullable=True)


class Penalty(Schema):
    event_id = field(String(), nullable=False)
    call_id = field(String(), nullable=False)
    race_id = field(String(), nullable=False)
    run_id = field(String(), nullable=False)
    paddler_id = field(String(), nullable=False)
    gate_number = field(Integer(), nullable=False)
    elapsed_millis = field(Long(), nullable=False)
    penalty_code = field(String(), nullable=False)
    penalty_seconds = field(Integer(), nullable=False)
    adjusted_millis = field(Long(), nullable=False)
