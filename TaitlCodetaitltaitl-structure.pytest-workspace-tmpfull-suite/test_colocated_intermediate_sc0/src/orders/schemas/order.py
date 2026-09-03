from structure import Schema
from structure.plugin.pyspark import field


class OrderRaw(Schema):
    id = field.string(nullable=False)


class OrderPublished(Schema):
    id = field.string(nullable=False)
