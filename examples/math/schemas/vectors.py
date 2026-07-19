from structure import Schema
from structure.platform.pyspark.dsl.field import *


class VectorEvent(Schema):
    vector_id = string(nullable=False)
    x = array(double(), contains_null=False)
    y = array(double(), contains_null=False)
    scalar = double(nullable=False)


class VectorResult(Schema):
    vector_id = string(nullable=False)
    sum = array(double(), contains_null=True, nullable=True)
    difference = array(double(), contains_null=True, nullable=True)
    scaled = array(double(), contains_null=True, nullable=True)
    normalized = array(double(), contains_null=True, nullable=True)
    projection = array(double(), contains_null=True, nullable=True)
    dot = double(nullable=True)
    magnitude_x = double(nullable=True)
    magnitude_y = double(nullable=True)
    cosine = double(nullable=True)
    error = string(nullable=True)
