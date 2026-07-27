from examples.security.schemas.reporting import VulnerabilityExposure
from structure import Schema
from structure.plugin.pyspark import *


class RemediationCase(Schema):
    vuln_id = string(nullable=False)
    acknowledged_at = timestamp(nullable=True)
    exception_requested_at = timestamp(nullable=True)
    exception_reason = string(nullable=True)
    exception_approver = string(nullable=True)
    exception_approved_at = timestamp(nullable=True)
    exception_expires_on = date(nullable=True)


class RemediationCaseAggregate(RemediationCase):
    case_count = long(nullable=False)
    vulnerability_exists = boolean(nullable=False)


class RemediationCaseCheck(RemediationCaseAggregate):
    issues = array(string(), contains_null=False, nullable=False)
    is_valid = boolean(nullable=False)


class RemediationCaseIssue(RemediationCaseCheck):
    pass


class VulnerabilityWorkflowExposure(VulnerabilityExposure):
    acknowledged_at = timestamp(nullable=True)
    exception_requested_at = timestamp(nullable=True)
    exception_reason = string(nullable=True)
    exception_approver = string(nullable=True)
    exception_approved_at = timestamp(nullable=True)
    exception_expires_on = date(nullable=True)
    is_acknowledged = boolean(nullable=False)
    is_deadline_paused = boolean(nullable=False)


class UnacknowledgedVulnerability(VulnerabilityWorkflowExposure):
    pass


class PendingExceptionVulnerability(VulnerabilityWorkflowExposure):
    pass


class ExpiringExceptionVulnerability(VulnerabilityWorkflowExposure):
    pass


class ExpiredExceptionVulnerability(VulnerabilityWorkflowExposure):
    pass


class RemediationWorkflowActivity(Schema):
    person_id = string(nullable=False)
    team_id = string(nullable=False)
    department_id = string(nullable=False)
    org_id = string(nullable=False)
    as_of_date = date(nullable=False)
    unacknowledged_count = integer(nullable=False)
    pending_exception_count = integer(nullable=False)
    expiring_exception_count = integer(nullable=False)
    expired_exception_count = integer(nullable=False)


class RemediationWorkflowSummary(Schema):
    as_of_date = date(nullable=False)
    unacknowledged_count = long(nullable=False)
    pending_exception_count = long(nullable=False)
    expiring_exception_count = long(nullable=False)
    expired_exception_count = long(nullable=False)


class PersonRemediationWorkflowSummary(RemediationWorkflowSummary):
    person_id = string(nullable=False)
    person_name = string(nullable=False)


class TeamRemediationWorkflowSummary(RemediationWorkflowSummary):
    team_id = string(nullable=False)
    team_name = string(nullable=False)


class DepartmentRemediationWorkflowSummary(RemediationWorkflowSummary):
    department_id = string(nullable=False)
    department_name = string(nullable=False)


class OrgRemediationWorkflowSummary(RemediationWorkflowSummary):
    org_id = string(nullable=False)
    org_name = string(nullable=False)
