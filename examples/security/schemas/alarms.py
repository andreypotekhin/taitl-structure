from structure import Schema
from structure.plugin.pyspark import *


class TeamVulnerabilityAlarm(Schema):
    delivery_key = string(nullable=False)
    vuln_id = string(nullable=False)
    team_id = string(nullable=False)
    team_name = string(nullable=False)
    severity = string(nullable=False)
    target_date = date(nullable=False)
    instructions = string(nullable=False)
