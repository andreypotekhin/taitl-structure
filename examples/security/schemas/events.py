from structure import Schema
from structure.plugin.pyspark import *


class RawEvent(Schema):
    id = string(nullable=False)
    device_id = string(nullable=False)
    scanner_id = string(nullable=False)
    occurred_at = timestamp(nullable=False)
    event_family = string(nullable=False)


class AppEvent(RawEvent):
    app_id = string(nullable=False)
    action = string(nullable=False)
    version = string(nullable=False)


class VulnEvent(RawEvent):
    vuln_id = string(nullable=False)
    action = string(nullable=False)
    description = string(nullable=False)
    instructions = string(nullable=False)
