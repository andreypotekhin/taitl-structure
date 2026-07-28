"""PySpark sequence and series schemas."""

from structure import Schema
from structure.plugin.pyspark import array, double, integer, long


class Tick(Schema):
    """One ordered point in a finite PySpark sequence."""

    index = long(nullable=False)


class FibonacciState(Schema):
    """State carried between ordered Fibonacci rows."""

    previous = long(nullable=False)
    current = long(nullable=False)


class FibonacciNumber(Schema):
    """A PySpark Fibonacci value at one sequence position."""

    index = long(nullable=False)
    value = long(nullable=False)


class PrimeState(Schema):
    """State carried while discovering prime numbers."""

    primes = array(integer(), contains_null=False, nullable=False)
    current = integer(nullable=False)


class PrimeNumber(Schema):
    """A PySpark prime number at one sequence position."""

    index = long(nullable=False)
    prime = integer(nullable=False)


class SeriesState(Schema):
    """State carried while summing a numerical series."""

    term = double(nullable=False)
    total = double(nullable=False)


class SeriesApproximation(Schema):
    """One partial sum of a numerical series."""

    index = long(nullable=False)
    term = double(nullable=False)
    value = double(nullable=False)
