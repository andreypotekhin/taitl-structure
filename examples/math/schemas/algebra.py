from structure import Schema
from structure.field import *


class ScalarEvent(Schema):
    id = string(nullable=False)
    params_id = string(nullable=False)
    x = double(nullable=False)
    y = double(nullable=False)
    z = double(nullable=False)
    t = double(nullable=False)


class FormulaParameters(Schema):
    params_id = string(nullable=False)
    a = double(nullable=False)
    b = double(nullable=False)
    c = double(nullable=False)
    r = double(nullable=False)
    n = double(nullable=False)


class FormulaResult(Schema):
    id = string(nullable=False)
    params_id = string(nullable=False)
    sum = double(nullable=False)
    difference = double(nullable=False)
    product = double(nullable=False)
    quotient = double(nullable=True)
    linear = double(nullable=False)
    quadratic = double(nullable=False)
    distance = double(nullable=False)
    compound = double(nullable=True)
    error = string(nullable=True)
