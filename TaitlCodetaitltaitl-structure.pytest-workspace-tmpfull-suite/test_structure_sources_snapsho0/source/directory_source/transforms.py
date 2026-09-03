
from structure import Transform, input, output
from directory_source.schemas import Row

class Copy(Transform):
    rows = input(Row)
    copied = output(Row)

    def copy(self, row: Row) -> Row:
        return Row.project(row)(id=row.id)

class CopyAgain(Transform):
    rows = input(Row)
    copied = output(Row)

    def copy(self, row: Row) -> Row:
        return Row.project(row)(id=row.id)
