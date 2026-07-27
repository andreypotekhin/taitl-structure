from structure import Schema
from structure.plugin.pyspark import *


class VulnType(Schema):
    id = string(nullable=False)
    type = string(nullable=False)
    severity = string(nullable=False)
    description = string(nullable=False)
    instructions = string(nullable=False)


class RemediationPolicy(Schema):
    severity = string(nullable=False)
    target_days = integer(nullable=False)


class Vuln(Schema):
    id = string(nullable=False)
    vuln_type_id = string(nullable=False)
    device_id = string(nullable=False)
    owner_id = string(nullable=False)
    software_id = string(nullable=False)
    date_discovered = date(nullable=False)
    date_addressed = date(nullable=True)
    is_active = boolean(nullable=False)
