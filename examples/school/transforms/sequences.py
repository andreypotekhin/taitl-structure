"""Example of a non-PySpark transform. Powered by Iterable plugin."""

from structure_iterable import scan, state

from examples.school.schemas.sequences import FibonacciRow, SequenceRow
from structure import Transform, input, output, step, transform


@transform(target="iterable")
class Fibonacci(Transform):
    """Emits one Fibonacci value for every contiguous caller-supplied sequence row."""

    rows = input(SequenceRow)
    result = output(FibonacciRow)

    @step(output=result)
    def generate(self, row: SequenceRow) -> FibonacciRow:
        return scan(
            initial=(0, 1),
            output=FibonacciRow(index=row.index, fibonacci=state[0]),
            next=lambda previous, current: (current, previous + current),
        )
