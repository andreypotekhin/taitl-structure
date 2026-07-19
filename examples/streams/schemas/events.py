from examples.streams.schemas.race import Race
from structure import Schema
from structure.platform.pyspark.dsl.field import *


class RawEvent(Schema):
    id = string(nullable=False)
    race_id = string(nullable=False)
    run_id = string(nullable=False)
    paddler_id = string(nullable=False)
    gate_number = integer(nullable=False)
    at = timestamp(nullable=False)
    sequence = long(nullable=False)
    elapsed_millis = long(nullable=False)
    source = string(nullable=False)


class Passage(RawEvent):
    race_name = string(nullable=True)
    race_date = date(nullable=True)
    river = string(nullable=True)
    venue = string(nullable=True)
    city = string(nullable=True)
    race_country = string(nullable=True)
    paddler_name = string(nullable=True)
    bib = integer(nullable=True)
    division = string(nullable=True)
    paddler_country = string(nullable=True)
    gate_direction = string(nullable=True)
    sector = string(nullable=True)


class JudgeCall(Schema):
    id = string(nullable=False)
    race_id = string(nullable=False)
    run_id = string(nullable=False)
    paddler_id = string(nullable=False)
    gate_number = integer(nullable=False)
    at = timestamp(nullable=False)
    code = string(nullable=False)
    penalty_seconds = integer(nullable=False)


class GateProgress(Schema):
    race_id = string(nullable=False)
    run_id = string(nullable=False)
    gate_number = integer(nullable=False)
    passage_count = long(nullable=False)
    fastest_millis = long(nullable=True)
    slowest_millis = long(nullable=True)


class Penalty(Schema):
    event_id = string(nullable=False)
    call_id = string(nullable=False)
    race_id = string(nullable=False)
    run_id = string(nullable=False)
    paddler_id = string(nullable=False)
    gate_number = integer(nullable=False)
    elapsed_millis = long(nullable=False)
    penalty_code = string(nullable=False)
    penalty_seconds = integer(nullable=False)
    adjusted_millis = long(nullable=False)
