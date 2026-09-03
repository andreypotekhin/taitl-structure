
from structure import Schema
from structure.plugin.pyspark import *

class Row(Schema):
    id = string(nullable=False)
