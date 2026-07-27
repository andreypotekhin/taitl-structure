from examples.security.schemas.assets import App
from structure import Schema
from structure.plugin.pyspark import *


class ReportingPeriod(Schema):
    kind = string(nullable=False)
    start = date(nullable=False)
    end = date(nullable=False)


class SecurityEvaluation(Schema):
    as_of_date = date(nullable=False)


class DeliveryReceipt(Schema):
    delivery_key = string(nullable=False)
    delivered_at = timestamp(nullable=False)


class VulnerabilityExposure(Schema):
    vuln_id = string(nullable=False)
    vuln_type = string(nullable=False)
    description = string(nullable=False)
    instructions = string(nullable=False)
    severity = string(nullable=False)
    date_discovered = date(nullable=False)
    date_addressed = date(nullable=True)
    target_date = date(nullable=False)
    is_active = boolean(nullable=False)
    device_id = string(nullable=False)
    device_platform = string(nullable=False)
    device_model = string(nullable=False)
    software_id = string(nullable=False)
    software_name = string(nullable=False)
    software_version = string(nullable=False)
    person_id = string(nullable=False)
    person_name = string(nullable=False)
    team_id = string(nullable=False)
    team_name = string(nullable=False)
    department_id = string(nullable=False)
    department_name = string(nullable=False)
    org_id = string(nullable=False)
    org_name = string(nullable=False)


class VulnerabilityPostureCandidate(VulnerabilityExposure):
    device_owner_id = string(nullable=False)
    device_os_id = string(nullable=False)
    device_vuln_ids = array(string(), contains_null=False, nullable=False)
    device_apps = array(struct(App), contains_null=False, nullable=False)


class DeviceActiveVulnerability(VulnerabilityExposure):
    pass


class PersonActiveVulnerability(VulnerabilityExposure):
    pass


class TeamActiveVulnerability(VulnerabilityExposure):
    pass


class DepartmentActiveVulnerability(VulnerabilityExposure):
    pass


class OrgActiveVulnerability(VulnerabilityExposure):
    pass


class VulnerabilityStatistic(Schema):
    period_kind = string(nullable=False)
    period_start = date(nullable=False)
    period_end = date(nullable=False)
    discovered_count = long(nullable=False)
    addressed_count = long(nullable=False)
    active_count = long(nullable=False)


class VulnerabilityLifecycle(Schema):
    vuln_id = string(nullable=False)
    detected_at = timestamp(nullable=True)
    addressed_at = timestamp(nullable=True)


class VulnerabilityDiscovery(Schema):
    vuln_id = string(nullable=False)
    discovered_at = timestamp(nullable=False)


class VulnerabilityDeadlineActivity(Schema):
    person_id = string(nullable=False)
    team_id = string(nullable=False)
    department_id = string(nullable=False)
    org_id = string(nullable=False)
    as_of_date = date(nullable=False)
    imminent_count = integer(nullable=False)
    overdue_count = integer(nullable=False)


class VulnerabilityDeadlineSummary(Schema):
    as_of_date = date(nullable=False)
    imminent_count = long(nullable=False)
    overdue_count = long(nullable=False)


class PersonVulnerabilityDeadlineSummary(VulnerabilityDeadlineSummary):
    person_id = string(nullable=False)
    person_name = string(nullable=False)


class TeamVulnerabilityDeadlineSummary(VulnerabilityDeadlineSummary):
    team_id = string(nullable=False)
    team_name = string(nullable=False)


class DepartmentVulnerabilityDeadlineSummary(VulnerabilityDeadlineSummary):
    department_id = string(nullable=False)
    department_name = string(nullable=False)


class OrgVulnerabilityDeadlineSummary(VulnerabilityDeadlineSummary):
    org_id = string(nullable=False)
    org_name = string(nullable=False)


class VulnerabilityPeriodActivity(Schema):
    person_id = string(nullable=False)
    team_id = string(nullable=False)
    department_id = string(nullable=False)
    org_id = string(nullable=False)
    period_kind = string(nullable=False)
    period_start = date(nullable=False)
    period_end = date(nullable=False)
    discovered_count = integer(nullable=False)
    addressed_count = integer(nullable=False)
    active_count = integer(nullable=False)


class PersonVulnerabilityStatistic(VulnerabilityStatistic):
    person_id = string(nullable=False)
    person_name = string(nullable=False)


class TeamVulnerabilityStatistic(VulnerabilityStatistic):
    team_id = string(nullable=False)
    team_name = string(nullable=False)


class DepartmentVulnerabilityStatistic(VulnerabilityStatistic):
    department_id = string(nullable=False)
    department_name = string(nullable=False)


class OrgVulnerabilityStatistic(VulnerabilityStatistic):
    org_id = string(nullable=False)
    org_name = string(nullable=False)


class VulnerabilityQualityCheck(Schema):
    vuln_id = string(nullable=False)
    device_id = string(nullable=False)
    owner_id = string(nullable=False)
    software_id = string(nullable=False)
    vuln_type_id = string(nullable=False)
    issues = array(string(), contains_null=False, nullable=False)
    is_valid = boolean(nullable=False)


class VulnerabilityQualityIssue(VulnerabilityQualityCheck):
    pass


class VulnerabilityInventoryCheck(Schema):
    vuln_id = string(nullable=False)
    device_id = string(nullable=False)
    software_id = string(nullable=False)
    device_lists_vulnerability = boolean(nullable=False)
    device_has_software = boolean(nullable=False)
    is_reconciled = boolean(nullable=False)


class VulnerabilityInventoryCandidate(Schema):
    vuln_id = string(nullable=False)
    device_id = string(nullable=False)
    software_id = string(nullable=False)
    device_lists_vulnerability = boolean(nullable=False)
    os_id = string(nullable=False)
    apps = array(struct(App), contains_null=False, nullable=False)
    device_has_software = boolean(nullable=False)
    is_reconciled = boolean(nullable=False)


class VulnerabilityInventoryIssue(VulnerabilityInventoryCheck):
    pass


class AppAuditEvent(Schema):
    id = string(nullable=False)
    occurred_at = timestamp(nullable=False)
    device_id = string(nullable=False)
    device_platform = string(nullable=False)
    scanner_name = string(nullable=False)
    app_id = string(nullable=False)
    app_name = string(nullable=False)
    action = string(nullable=False)
    version = string(nullable=False)


class VulnerabilityAuditEvent(Schema):
    id = string(nullable=False)
    occurred_at = timestamp(nullable=False)
    vuln_id = string(nullable=False)
    device_id = string(nullable=False)
    person_id = string(nullable=False)
    scanner_name = string(nullable=False)
    action = string(nullable=False)
    description = string(nullable=False)
    instructions = string(nullable=False)
