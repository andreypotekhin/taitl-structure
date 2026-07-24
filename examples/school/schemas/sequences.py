"""Finite iterable models used by the school sequence demonstrations."""

from structure import Schema


class Student(Schema):
    """One student score supplied to the Iterable projection example."""

    student: str
    score: int


class SequenceRow(Schema):
    """One caller-provided position in a finite sequence."""

    index: int


class FibonacciRow(SequenceRow):
    """A sequence position with its generated Fibonacci value."""

    fibonacci: int
