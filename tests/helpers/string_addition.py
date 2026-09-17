from structure import Schema, Transform, input, output
from structure.plugin.pyspark import coalesce, integer, literal, string, types


class StringSource(Schema):
    id = integer(nullable=False)
    first = string(nullable=True)
    last = string(nullable=True)


class StringResult(Schema):
    id = integer(nullable=False)
    joined = string(nullable=True)
    prefixed = string(nullable=True)
    suffixed = string(nullable=True)
    chained = string(nullable=True)
    fallback = string(nullable=False)
    converted = string(nullable=False)
    null_string = string(nullable=True)
    incremented = integer(nullable=False)


class StringAddition(Transform):
    rows = input(StringSource)
    result = output(StringResult)

    def project(self, row: StringSource) -> StringResult:
        return StringResult(
            id=row.id,
            joined=row.first + row.last,
            prefixed="order:" + row.first,
            suffixed=row.first + "!",
            chained=row.first + " / " + row.last,
            fallback=coalesce(row.first, "") + "!",
            converted="n=" + row.id.cast(types.string()),
            null_string=row.first + literal(None).cast(types.string()),
            incremented=row.id + 1,
        )
