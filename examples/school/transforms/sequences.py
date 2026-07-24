"""Iterable-only stateful sequence demonstrations."""

from structure_iterable import recurrence, state

from examples.school.schemas.sequences import FibonacciRow, SequenceRow
from structure import Transform, input, output, transform


@transform(target="iterable")
class Fibonacci(Transform):
    """Emits one Fibonacci value for every contiguous caller-supplied sequence row."""

    rows = input(SequenceRow)
    result = output(FibonacciRow)

    operation = recurrence(
        initial=(0, 1),
        output=state[0],
        next=lambda previous, current: (current, previous + current),
    )
