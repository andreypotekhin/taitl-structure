from structure import Schema
from structure.plugin.pyspark import *


class PersonVulnerabilityNotification(Schema):
    delivery_key = string(nullable=False)
    notification_type = string(nullable=False)
    vuln_id = string(nullable=False)
    person_id = string(nullable=False)
    person_name = string(nullable=False)
    person_email = string(nullable=False)
    severity = string(nullable=False)
    target_date = date(nullable=False)
    instructions = string(nullable=False)
