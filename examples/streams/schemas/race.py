from structure import Schema
from structure.plugin.pyspark import *


class Race(Schema):
    id = string(nullable=False)
    name = string(nullable=False)
    date = date(nullable=False)
    river = string(nullable=False)
    venue = string(nullable=False)
    city = string(nullable=False)
    country = string(nullable=False)


class Gate(Schema):
    race_id = string(nullable=False)
    number = integer(nullable=False)
    direction = string(nullable=False)
    sector = string(nullable=False)


class Paddler(Schema):
    race_id = string(nullable=False)
    id = string(nullable=False)
    bib = integer(nullable=False)
    name = string(nullable=False)
    country = string(nullable=False)
    division = string(nullable=False)


class RaceWinner(Schema):
    race_id = string(nullable=False)
    run_id = string(nullable=False)
    paddler_id = string(nullable=False)
    rank = integer(nullable=False)
    adjusted_millis = long(nullable=False)
