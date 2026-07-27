from examples.security.schemas.events import VulnEvent
from examples.security.schemas.notifications import PersonVulnerabilityNotification
from examples.security.schemas.organization import Person
from examples.security.schemas.remediation import VulnerabilityWorkflowExposure
from examples.security.schemas.reporting import DeliveryReceipt, SecurityEvaluation, VulnerabilityDiscovery
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl import types


class VulnerabilityNotifications(Transform):
    """Publish receipt-suppressed person notification outboxes."""

    exposures = input(VulnerabilityWorkflowExposure)
    events = input(VulnEvent)
    people = input(Person)
    evaluation = input(SecurityEvaluation)
    receipts = input(DeliveryReceipt)
    deduped_events = lane(VulnEvent)
    discoveries = lane(VulnerabilityDiscovery)
    discovery_notifications = output(PersonVulnerabilityNotification)
    imminent_notifications = output(PersonVulnerabilityNotification)
    overdue_notifications = output(PersonVulnerabilityNotification)

    @step(input=events, output=deduped_events)
    def dedupe_delivery(self, event: VulnEvent) -> VulnEvent:
        drop_duplicates(event.id)
        return VulnEvent.project(event)

    @step(input=deduped_events, output=discoveries)
    def first_detection(self, event: VulnEvent) -> VulnerabilityDiscovery:
        where(event.action == "Detected")
        group_by(vuln_id=event.vuln_id)
        return VulnerabilityDiscovery(vuln_id=event.vuln_id, discovered_at=min(event.occurred_at))

    @step(input=[exposures, discoveries, people, receipts], output=discovery_notifications)
    def notify_discovery(
        self,
        finding: VulnerabilityWorkflowExposure,
        discovery: VulnerabilityDiscovery,
        person: Person,
        receipt: DeliveryReceipt,
    ) -> PersonVulnerabilityNotification:
        inner_join(discovery, on=discovery.vuln_id == finding.vuln_id)
        inner_join(person, on=person.id == finding.person_id)
        key = concat_ws(":", finding.vuln_id, "discovered", discovery.discovered_at.cast(types.string()))
        left_join(receipt, on=receipt.delivery_key == key)
        where(receipt.delivery_key.is_null())
        return self._notification(finding, person, key, "discovered")

    @step(input=[exposures, evaluation, people, receipts], output=imminent_notifications)
    def notify_imminent(
        self,
        finding: VulnerabilityWorkflowExposure,
        evaluation: SecurityEvaluation,
        person: Person,
        receipt: DeliveryReceipt,
    ) -> PersonVulnerabilityNotification:
        cross_join(evaluation, allow_cartesian=True)
        inner_join(person, on=person.id == finding.person_id)
        key = self._deadline_key(finding, person.id, "imminent")
        left_join(receipt, on=receipt.delivery_key == key)
        where(self._imminent(finding, evaluation) & receipt.delivery_key.is_null())
        return self._notification(finding, person, key, "imminent")

    @step(input=[exposures, evaluation, people, receipts], output=overdue_notifications)
    def notify_overdue(
        self,
        finding: VulnerabilityWorkflowExposure,
        evaluation: SecurityEvaluation,
        person: Person,
        receipt: DeliveryReceipt,
    ) -> PersonVulnerabilityNotification:
        cross_join(evaluation, allow_cartesian=True)
        inner_join(person, on=person.id == finding.person_id)
        key = self._deadline_key(finding, person.id, "overdue")
        left_join(receipt, on=receipt.delivery_key == key)
        where(self._overdue(finding, evaluation) & receipt.delivery_key.is_null())
        return self._notification(finding, person, key, "overdue")

    @staticmethod
    def _notification(
        finding: VulnerabilityWorkflowExposure, person: Person, key, notification_type: str
    ) -> PersonVulnerabilityNotification:
        return PersonVulnerabilityNotification(
            delivery_key=key,
            notification_type=notification_type,
            vuln_id=finding.vuln_id,
            person_id=person.id,
            person_name=person.name,
            person_email=person.email,
            severity=finding.severity,
            target_date=finding.target_date,
            instructions=finding.instructions,
        )

    @staticmethod
    def _deadline_key(finding: VulnerabilityWorkflowExposure, recipient, kind: str):
        return concat_ws(":", finding.vuln_id, kind, recipient, finding.target_date.cast(types.string()))

    @staticmethod
    def _imminent(finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation):
        return (
            finding.is_active
            & ~finding.is_deadline_paused
            & (evaluation.as_of_date >= date_sub(finding.target_date, days=7))
            & (evaluation.as_of_date < finding.target_date)
        )

    @staticmethod
    def _overdue(finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation):
        return finding.is_active & ~finding.is_deadline_paused & (evaluation.as_of_date > finding.target_date)
