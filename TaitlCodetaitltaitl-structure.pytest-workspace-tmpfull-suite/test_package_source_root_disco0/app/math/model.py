from structure import Schema
from structure.plugin.pyspark import field


class Metric(Schema):
    id = field.string(nullable=False)
