from structure import Schema
from structure.plugin.pyspark import TimeWindow, string, struct


class EvaluationBatch(Schema):
    window = struct(TimeWindow, nullable=False)
    batch_id = string(nullable=False)
