from examples.school.schemas.algebra import FormulaParameters, FormulaResult, ScalarEvent
from structure import Transform, input, output, transform
from structure.plugin.pyspark import *


@transform(streaming=True)
class EvaluateAlgebra(Transform):
    events = input(ScalarEvent, streaming=True)
    parameters = input(FormulaParameters)
    results = output(FormulaResult)

    def evaluate(self, x: ScalarEvent, y: FormulaParameters) -> FormulaResult:
        inner_join(y, on=x.params_id == y.params_id)
        compound_base = 1.0 + y.r / y.n
        compound_valid = (y.n > 0) & (compound_base >= 0)
        return FormulaResult.project(x)(
            sum=x.x + x.y,
            difference=x.x - x.y,
            product=x.x * x.y,
            quotient=when(x.y != 0, x.x / x.y).otherwise(None),
            linear=y.a * x.x + y.b,
            quadratic=y.a * pow(x.x, 2.0) + y.b * x.x + y.c,
            distance=sqrt(pow(x.x, 2.0) + pow(x.y, 2.0) + pow(x.z, 2.0)),
            compound=when(compound_valid, x.x * pow(compound_base, y.n * x.t)).otherwise(None),
            error=when(x.y == 0, "division by zero").otherwise(
                when(compound_valid, None).otherwise("invalid compound parameters")
            ),
        )
