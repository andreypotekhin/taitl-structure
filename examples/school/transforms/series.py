"""PySpark examples for finite numerical series."""

from examples.school.schemas.sequences import SeriesApproximation, SeriesState, Tick
from structure import Transform, input, output
from structure.plugin.pyspark import scan, when


class PiAsSeries(Transform):
    """Approximates pi with the Leibniz series."""

    ticks = input(Tick)
    result = output(SeriesApproximation)

    def approximate(self, tick: Tick) -> SeriesApproximation:
        state = scan(
            initial=SeriesState(term=4.0, total=4.0),
            partition_by=1,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: SeriesState(
                term=when((row.index + 1) % 2 == 0, 4.0).otherwise(-4.0) / (2 * (row.index + 1) + 1),
                total=state.total
                + when((row.index + 1) % 2 == 0, 4.0).otherwise(-4.0) / (2 * (row.index + 1) + 1),
            ),
        )
        return SeriesApproximation(index=tick.index, term=state.term, value=state.total)


class EAsSeries(Transform):
    """Approximates Euler's number with reciprocal factorial terms."""

    ticks = input(Tick)
    result = output(SeriesApproximation)

    def approximate(self, tick: Tick) -> SeriesApproximation:
        state = scan(
            initial=SeriesState(term=1.0, total=1.0),
            partition_by=1,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: SeriesState(
                term=state.term / (row.index + 1),
                total=state.total + state.term / (row.index + 1),
            ),
        )
        return SeriesApproximation(index=tick.index, term=state.term, value=state.total)


class Ln2AsSeries(Transform):
    """Approximates ln(2) with the alternating harmonic series."""

    ticks = input(Tick)
    result = output(SeriesApproximation)

    def approximate(self, tick: Tick) -> SeriesApproximation:
        state = scan(
            initial=SeriesState(term=1.0, total=1.0),
            partition_by=1,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: SeriesState(
                term=when((row.index + 2) % 2 == 1, 1.0).otherwise(-1.0) / (row.index + 2),
                total=state.total + when((row.index + 2) % 2 == 1, 1.0).otherwise(-1.0) / (row.index + 2),
            ),
        )
        return SeriesApproximation(index=tick.index, term=state.term, value=state.total)
