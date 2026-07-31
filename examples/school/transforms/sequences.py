"""PySpark sequence examples."""

from examples.school.schemas.sequences import FibonacciNumber, FibonacciState, PrimeNumber, PrimeState, Tick
from structure import Transform, input, output
from structure.plugin.pyspark import arr_append, arr_filter, arr_forall, array, coalesce, element_at, scan, sequence


class Fibonacci(Transform):
    """Emits one Fibonacci value for each ordered timeline row."""

    ticks = input(Tick)
    result = output(FibonacciNumber)

    def calculate(self, tick: Tick) -> FibonacciNumber:
        state = scan(
            initial=FibonacciState(previous=0, current=1),
            partition_by=1,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: FibonacciState(
                previous=state.current,
                current=state.previous + state.current,
            ),
        )
        return FibonacciNumber.project(tick)(value=state.previous)


class PrimeNumbers(Transform):
    """Emits one prime number for each ordered timeline row."""

    ticks = input(Tick)
    result = output(PrimeNumber)

    def calculate(self, tick: Tick) -> PrimeNumber:
        state = scan(
            initial=PrimeState(primes=array(2), current=2),
            partition_by=1,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: PrimeState(
                primes=arr_append(state.primes, _next_prime(state)),
                current=_next_prime(state),
            ),
        )
        return PrimeNumber.project(tick)(prime=state.current)


def _next_prime(state):
    candidate = element_at(
        arr_filter(
            sequence(state.current + 1, state.current * 2),
            lambda candidate: arr_forall(
                state.primes,
                lambda prime: candidate % prime != 0,
                argument_name="prime",
            ),
        ),
        1,
    )
    return coalesce(candidate, state.current)
