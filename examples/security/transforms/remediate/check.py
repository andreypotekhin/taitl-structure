from examples.security.schemas.remediation import (
    RemediationCase,
    RemediationCaseAggregate,
    RemediationCaseCheck,
    RemediationCaseIssue,
)
from examples.security.schemas.risk import Vuln
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *


class VulnerabilityRemediationPrepare(Transform):
    """Validate caller-owned remediate case snapshots."""

    cases = input(RemediationCase)
    vulnerabilities = input(Vuln)
    case_aggregates = lane(RemediationCaseAggregate)
    case_lane = lane(RemediationCaseCheck)
    case_checks = output(RemediationCaseCheck)
    case_issues = output(RemediationCaseIssue)

    @step(output=case_aggregates)
    def aggregate_cases(self, case: RemediationCase, vuln: Vuln) -> RemediationCaseAggregate:
        left_join(vuln, on=vuln.id == case.vuln_id)
        group_by(vuln_id=case.vuln_id)
        return RemediationCaseAggregate(
            vuln_id=case.vuln_id,
            acknowledged_at=min(case.acknowledged_at),
            exception_requested_at=min(case.exception_requested_at),
            exception_reason=min(case.exception_reason),
            exception_approver=min(case.exception_approver),
            exception_approved_at=min(case.exception_approved_at),
            exception_expires_on=min(case.exception_expires_on),
            case_count=count(),
            vulnerability_exists=bool_or(vuln.id.is_not_null()),
        )

    @step(output=case_lane)
    def check_cases(self, case: RemediationCaseAggregate) -> RemediationCaseCheck:
        issues = arr_compact(
            array(
                when(case.case_count > 1, "duplicate current cases").otherwise(None),
                when(~case.vulnerability_exists, "unknown vulnerability").otherwise(None),
                when(
                    case.exception_approved_at.is_not_null() & case.exception_requested_at.is_null(),
                    "approved exception has no request",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null()
                    & case.exception_requested_at.is_not_null()
                    & (case.exception_approved_at < case.exception_requested_at),
                    "exception approval precedes request",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null() & case.exception_reason.is_null(),
                    "approved exception has no reason",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null() & case.exception_approver.is_null(),
                    "approved exception has no approver",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null() & case.exception_expires_on.is_null(),
                    "approved exception has no expiry",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null()
                    & case.exception_expires_on.is_not_null()
                    & (case.exception_expires_on < to_date(case.exception_approved_at)),
                    "exception expires before approval",
                ).otherwise(None),
            )
        )
        return RemediationCaseCheck.base(case)(issues=issues, is_valid=size(issues) == 0)

    @step(output=case_checks)
    def publish_case_checks(self, check: RemediationCaseCheck) -> RemediationCaseCheck:
        return RemediationCaseCheck.project(check)

    @step(input=case_lane, output=case_issues)
    def publish_case_issues(self, check: RemediationCaseCheck) -> RemediationCaseIssue:
        where(~check.is_valid)
        return RemediationCaseIssue.base(check)
