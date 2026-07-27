from examples.security.schemas.alarms import TeamVulnerabilityAlarm
from examples.security.schemas.remediation import VulnerabilityWorkflowExposure
from examples.security.schemas.reporting import DeliveryReceipt, SecurityEvaluation
from structure import Transform, input, output, step
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl import types


class VulnerabilityAlarms(Transform):
    """Publish receipt-suppressed overdue team alarm outboxes."""

    exposures = input(VulnerabilityWorkflowExposure)
    evaluation = input(SecurityEvaluation)
    receipts = input(DeliveryReceipt)
    overdue_alarms = output(TeamVulnerabilityAlarm)

    @step(input=[exposures, evaluation, receipts], output=overdue_alarms)
    def alarm_team(
        self, finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation, receipt: DeliveryReceipt
    ) -> TeamVulnerabilityAlarm:
        cross_join(evaluation, allow_cartesian=True)
        key = concat_ws(":", finding.vuln_id, "team-overdue", finding.team_id, finding.target_date.cast(types.string()))
        left_join(receipt, on=receipt.delivery_key == key)
        where(
            (finding.is_active & ~finding.is_deadline_paused & (evaluation.as_of_date > finding.target_date))
            & receipt.delivery_key.is_null()
        )
        return TeamVulnerabilityAlarm(
            delivery_key=key,
            vuln_id=finding.vuln_id,
            team_id=finding.team_id,
            team_name=finding.team_name,
            severity=finding.severity,
            target_date=finding.target_date,
            instructions=finding.instructions,
        )
