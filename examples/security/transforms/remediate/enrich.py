from examples.security.schemas.remediation import RemediationCaseCheck, VulnerabilityWorkflowExposure
from examples.security.schemas.reporting import SecurityEvaluation, VulnerabilityExposure
from structure import Transform, input, output
from structure.plugin.pyspark import *


class VulnerabilityRemediationAccess(Transform):
    """Apply valid remediate cases to security exposures."""

    exposures = input(VulnerabilityExposure)
    case_checks = input(RemediationCaseCheck)
    evaluation = input(SecurityEvaluation)
    workflow_exposures = output(VulnerabilityWorkflowExposure)

    def enrich_exposure(
        self, finding: VulnerabilityExposure, check: RemediationCaseCheck, evaluation: SecurityEvaluation
    ) -> VulnerabilityWorkflowExposure:
        left_join(check, on=(check.vuln_id == finding.vuln_id) & check.is_valid)
        cross_join(evaluation, allow_cartesian=True)
        approved = check.exception_approved_at.is_not_null() & check.exception_expires_on.is_not_null()
        return VulnerabilityWorkflowExposure.project(finding, check)(
            vuln_id=finding.vuln_id,
            is_acknowledged=check.acknowledged_at.is_not_null(),
            is_deadline_paused=coalesce(approved & (check.exception_expires_on >= evaluation.as_of_date), False),
        )
