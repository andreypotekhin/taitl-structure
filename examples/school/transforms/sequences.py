"""PySpark sequence examples."""

from examples.school.schemas.sequences import FibonacciRow, FibonacciState, SequenceTick
from structure import Transform, input, output
from structure.plugin.pyspark import scan


class Fibonacci(Transform):
    """Emits one Fibonacci value for each ordered timeline row."""

    ticks = input(SequenceTick)
    result = output(FibonacciRow)

    def calculate(self, tick: SequenceTick) -> FibonacciRow:
        state = scan(
            initial=FibonacciState(previous=0, current=1),
            partition_by=tick.series,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: FibonacciState(
                previous=state.current,
                current=state.previous + state.current,
            ),
        )
        return FibonacciRow(series=tick.series, index=tick.index, value=state.previous)
