from builtins import float as scalar_float
from builtins import max as scalar_max

from examples.school.schemas.matrices import LeftMatrixCell, MatrixCell, MatrixVectorCell, RightMatrixCell, VectorCell
from structure import Transform, input, lane, output, raw
from structure.plugin.pyspark import *


class MultiplyMatrices(Transform):
    left = input(LeftMatrixCell)
    right = input(RightMatrixCell)
    product = output(MatrixCell)

    def multiply(self, x: LeftMatrixCell, y: RightMatrixCell) -> MatrixCell:
        inner_join(
            y,
            on=(x.matrix_id == y.matrix_id) & (x.columns == y.rows) & (x.j == y.i),
        )
        group_by(matrix_id=x.matrix_id, rows=x.rows, columns=y.columns, i=x.i, j=y.j)
        return MatrixCell.project(x)(
            columns=y.columns,
            j=y.j,
            x=sum(x.x * y.x),
        )


class MultiplyMatrixVector(Transform):
    matrix = input(LeftMatrixCell)
    vector = input(VectorCell)
    product = output(MatrixVectorCell)

    def multiply(self, x: LeftMatrixCell, y: VectorCell) -> MatrixVectorCell:
        inner_join(
            y,
            on=(x.matrix_id == y.matrix_id) & (x.columns == y.size) & (x.j == y.i),
        )
        group_by(matrix_id=x.matrix_id, size=x.rows, i=x.i)
        return MatrixVectorCell.project(x)(
            size=x.rows,
            x=sum(x.x * y.x),
        )


class InvertMatrices(Transform):
    matrices = input(LeftMatrixCell)
    inverse = output(MatrixCell)

    @raw(inout=input(matrices) | lane(matrices))
    def calculate(self, *, matrices, spark, ctx):
        cells = []
        for matrix_id, matrix in _matrix_rows(matrices).items():
            inverse, _ = _inverse(matrix)
            if inverse is None:
                continue
            size = len(inverse)
            cells.extend((matrix_id, size, size, i, j, inverse[i][j]) for i in range(size) for j in range(size))
        return spark.createDataFrame(
            cells,
            "matrix_id string, rows long, columns long, i long, j long, x double",
        )

    def publish(self, x: LeftMatrixCell) -> MatrixCell:
        return MatrixCell.project(x)


def _matrix_rows(frame):
    matrices = {}
    for row in frame.select("matrix_id", "rows", "columns", "i", "j", "x").collect():
        matrix_id = row.matrix_id
        rows, columns = int(row.rows), int(row.columns)
        if rows <= 0 or columns <= 0:
            matrices[matrix_id] = None
            continue
        matrix = matrices.get(matrix_id)
        if matrix is None and matrix_id in matrices:
            continue
        if matrix is None:
            matrix = {"rows": rows, "columns": columns, "values": {}}
            matrices[matrix_id] = matrix
        if matrix["rows"] != rows or matrix["columns"] != columns:
            matrices[matrix_id] = None
            continue
        matrix["values"][(int(row.i), int(row.j))] = scalar_float(row.x)
    return matrices


def _inverse(matrix):
    if matrix is None:
        return None, "inconsistent matrix metadata"
    rows, columns = matrix["rows"], matrix["columns"]
    if rows != columns:
        return None, "matrix is not square"
    values = matrix["values"]
    if len(values) != rows * columns or any((i, j) not in values for i in range(rows) for j in range(columns)):
        return None, "matrix cells are incomplete"
    original = [[values[(i, j)] for j in range(columns)] for i in range(rows)]
    work = [row[:] + [scalar_float(i == j) for j in range(rows)] for i, row in enumerate(original)]
    for pivot in range(rows):
        selected = scalar_max(range(pivot, rows), key=lambda row: abs(work[row][pivot]))
        if work[selected][pivot] == 0:
            return None, "matrix is singular"
        work[pivot], work[selected] = work[selected], work[pivot]
        divisor = work[pivot][pivot]
        work[pivot] = [value / divisor for value in work[pivot]]
        for row in range(rows):
            if row == pivot:
                continue
            factor = work[row][pivot]
            work[row] = [value - factor * pivot_value for value, pivot_value in zip(work[row], work[pivot])]
    return [row[rows:] for row in work], None
