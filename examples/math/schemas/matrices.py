from structure import Schema
from structure.field import *


class LeftMatrixCell(Schema):
    matrix_id = string(nullable=False)
    rows = long(nullable=False)
    columns = long(nullable=False)
    i = long(nullable=False)
    j = long(nullable=False)
    x = double(nullable=False)


class RightMatrixCell(LeftMatrixCell):
    pass


class MatrixCell(LeftMatrixCell):
    pass


class VectorCell(Schema):
    matrix_id = string(nullable=False)
    size = long(nullable=False)
    i = long(nullable=False)
    x = double(nullable=False)


class MatrixVectorCell(VectorCell):
    pass


class MatrixParameter(Schema):
    rows = long(nullable=False)
    columns = long(nullable=False)


class MatrixMeasure(Schema):
    matrix_id = string(nullable=False)
    determinant = double(nullable=True)
    error = string(nullable=True)
