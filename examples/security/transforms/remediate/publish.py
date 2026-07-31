from examples.security.schemas.remediation import (
    ExpiredExceptionVulnerability,
    ExpiringExceptionVulnerability,
    PendingExceptionVulnerability,
    UnacknowledgedVulnerability,
    VulnerabilityWorkflowExposure,
)
from examples.security.schemas.reporting import SecurityEvaluation
from structure import Transform, input, output, step
from structure.plugin.pyspark import *


class VulnerabilityRemediationPublish(Transform):
    """Publish remediate queues from workflow exposures."""

    workflow_exposures = input(VulnerabilityWorkflowExposure)
    evaluation = input(SecurityEvaluation)
    unacknowledged = output(UnacknowledgedVulnerability)
    pending_exceptions = output(PendingExceptionVulnerability)
    expiring_exceptions = output(ExpiringExceptionVulnerability)
    expired_exceptions = output(ExpiredExceptionVulnerability)

    @step(output=unacknowledged)
    def publish_unacknowledged(self, finding: VulnerabilityWorkflowExposure) -> UnacknowledgedVulnerability:
        where(finding.is_active & ~finding.is_acknowledged)
        return UnacknowledgedVulnerability.base(finding)

    @step(input=workflow_exposures, output=pending_exceptions)
    def publish_pending(self, finding: VulnerabilityWorkflowExposure) -> PendingExceptionVulnerability:
        where(
            finding.is_active & finding.exception_requested_at.is_not_null() & finding.exception_approved_at.is_null()
        )
        return PendingExceptionVulnerability.base(finding)

    @step(input=[workflow_exposures, evaluation], output=expiring_exceptions)
    def publish_expiring(
        self, finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation
    ) -> ExpiringExceptionVulnerability:
        cross_join(evaluation, allow_cartesian=True)
        where(
            finding.is_active
            & finding.exception_approved_at.is_not_null()
            & finding.exception_expires_on.is_not_null()
            & (finding.exception_expires_on >= evaluation.as_of_date)
            & (finding.exception_expires_on <= date_add(evaluation.as_of_date, days=7))
        )
        return ExpiringExceptionVulnerability.base(finding)

    @step(input=[workflow_exposures, evaluation], output=expired_exceptions)
    def publish_expired(
        self, finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation
    ) -> ExpiredExceptionVulnerability:
        cross_join(evaluation, allow_cartesian=True)
        where(
            finding.is_active
            & finding.exception_approved_at.is_not_null()
            & finding.exception_expires_on.is_not_null()
            & (finding.exception_expires_on < evaluation.as_of_date)
        )
        return ExpiredExceptionVulnerability.base(finding)
