"""PySpark sequence and series schemas."""

from structure import Schema
from structure.plugin.pyspark import double, long, string


class SequenceTick(Schema):
    """One ordered point in a PySpark sequence partition."""

    series = string(nullable=False)
    index = long(nullable=False)


class FibonacciState(Schema):
    """State carried between ordered Fibonacci rows."""

    previous = long(nullable=False)
    current = long(nullable=False)


class FibonacciRow(Schema):
    """A PySpark Fibonacci value at one sequence position."""

    series = string(nullable=False)
    index = long(nullable=False)
    value = long(nullable=False)


class SeriesState(Schema):
    """State carried while summing a numerical series."""

    term = double(nullable=False)
    total = double(nullable=False)


class SeriesApproximation(Schema):
    """One partial sum of a numerical series."""

    series = string(nullable=False)
    index = long(nullable=False)
    term = double(nullable=False)
    value = double(nullable=False)
