from examples.security.schemas.assets import Device, DeviceType, Software
from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.reporting import (
    VulnerabilityInventoryCandidate,
    VulnerabilityInventoryCheck,
    VulnerabilityInventoryIssue,
    VulnerabilityQualityCheck,
    VulnerabilityQualityIssue,
)
from examples.security.schemas.risk import Vuln, VulnType
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *


class SecurityInventoryQuality(Transform):
    """Make broken security references and stale device inventory explicit."""

    vulnerabilities = input(Vuln)
    devices = input(Device)
    device_types = input(DeviceType)
    software = input(Software)
    vuln_types = input(VulnType)
    people = input(Person)
    teams = input(Team)
    departments = input(Department)
    orgs = input(Org)
    quality_lane = lane(VulnerabilityQualityCheck)
    inventory_candidates = lane(VulnerabilityInventoryCandidate)
    reconciliation_lane = lane(VulnerabilityInventoryCheck)
    reference_checks = output(VulnerabilityQualityCheck)
    reference_issues = output(VulnerabilityQualityIssue)
    reconciliation_checks = output(VulnerabilityInventoryCheck)
    reconciliation_issues = output(VulnerabilityInventoryIssue)

    @step(output=quality_lane)
    def check_references(
        self,
        vuln: Vuln,
        device: Device,
        device_type: DeviceType,
        software: Software,
        vuln_type: VulnType,
        person: Person,
        team: Team,
        department: Department,
        org: Org,
    ) -> VulnerabilityQualityCheck:
        left_join(device, on=device.id == vuln.device_id)
        left_join(device_type, on=device_type.id == device.device_type_id)
        left_join(software, on=software.id == vuln.software_id)
        left_join(vuln_type, on=vuln_type.id == vuln.vuln_type_id)
        left_join(person, on=person.id == vuln.owner_id)
        left_join(team, on=team.id == person.team_id)
        left_join(department, on=department.id == team.department_id)
        left_join(org, on=org.id == department.org_id)
        issues = arr_compact(
            array(
                when(device.id.is_null(), "missing device").otherwise(None),
                when(device.id.is_not_null() & device_type.id.is_null(), "missing device type").otherwise(None),
                when(software.id.is_null(), "missing software").otherwise(None),
                when(vuln_type.id.is_null(), "missing vulnerability type").otherwise(None),
                when(person.id.is_null(), "missing person").otherwise(None),
                when(person.id.is_not_null() & team.id.is_null(), "missing team").otherwise(None),
                when(team.id.is_not_null() & department.id.is_null(), "missing department").otherwise(None),
                when(department.id.is_not_null() & org.id.is_null(), "missing organization").otherwise(None),
                when(
                    device.id.is_not_null() & (device.owner_id != vuln.owner_id),
                    "device owner disagrees with vulnerability owner",
                ).otherwise(None),
                when(
                    vuln.is_active != vuln.date_addressed.is_null(),
                    "is_active disagrees with date_addressed",
                ).otherwise(None),
            )
        )
        return VulnerabilityQualityCheck.base(vuln)(
            vuln_id=vuln.id,
            issues=issues,
            is_valid=size(issues) == 0,
        )

    def publish_reference_checks(self, check: VulnerabilityQualityCheck) -> VulnerabilityQualityCheck:
        return VulnerabilityQualityCheck.base(check)

    @step(input=quality_lane, output=reference_issues)
    def publish_reference_issues(self, check: VulnerabilityQualityCheck) -> VulnerabilityQualityIssue:
        where(~check.is_valid)
        return VulnerabilityQualityIssue.base(check)

    @step(input=[vulnerabilities, devices], output=inventory_candidates)
    def prepare_inventory_reconciliation(self, vuln: Vuln, device: Device) -> VulnerabilityInventoryCandidate:
        inner_join(device, on=device.id == vuln.device_id)
        device_has_software = (device.os_id == vuln.software_id) | arr_exists(
            device.apps,
            lambda app: app.id == vuln.software_id,
        )
        return VulnerabilityInventoryCandidate.base(vuln, device)(
            vuln_id=vuln.id,
            device_lists_vulnerability=array_contains(device.vuln_ids, vuln.id),
            os_id=device.os_id,
            apps=device.apps,
            device_has_software=device_has_software,
            is_reconciled=array_contains(device.vuln_ids, vuln.id) & device_has_software,
        )

    @step(input=inventory_candidates, output=reconciliation_lane)
    def publish_reconciliation(self, candidate: VulnerabilityInventoryCandidate) -> VulnerabilityInventoryCheck:
        return VulnerabilityInventoryCheck.base(candidate)

    def publish_reconciliation_checks(self, check: VulnerabilityInventoryCheck) -> VulnerabilityInventoryCheck:
        return VulnerabilityInventoryCheck.base(check)

    @step(input=reconciliation_lane, output=reconciliation_issues)
    def publish_reconciliation_issues(self, check: VulnerabilityInventoryCheck) -> VulnerabilityInventoryIssue:
        where(~check.is_reconciled)
        return VulnerabilityInventoryIssue.base(check)
