from examples.security.schemas.assets import Device, DeviceType, Software
from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.reporting import VulnerabilityExposure, VulnerabilityPostureCandidate
from examples.security.schemas.risk import RemediationPolicy, Vuln, VulnType
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *


class SecurityPosture(Transform):
    """Enrich vulnerability inventory with its current organizational context."""

    vulnerabilities = input(Vuln)
    devices = input(Device)
    device_types = input(DeviceType)
    software = input(Software)
    vuln_types = input(VulnType)
    remediation_policies = input(RemediationPolicy)
    people = input(Person)
    teams = input(Team)
    departments = input(Department)
    orgs = input(Org)
    posture_candidates = lane(VulnerabilityPostureCandidate)
    exposure_lane = lane(VulnerabilityExposure)
    exposures = output(VulnerabilityExposure)

    @step(output=posture_candidates)
    def prepare_exposure(
        self,
        vuln: Vuln,
        device: Device,
        device_type: DeviceType,
        software: Software,
        vuln_type: VulnType,
        remediation_policy: RemediationPolicy,
        person: Person,
        team: Team,
        department: Department,
        org: Org,
    ) -> VulnerabilityPostureCandidate:
        inner_join(device, on=device.id == vuln.device_id)
        inner_join(device_type, on=device_type.id == device.device_type_id)
        inner_join(software, on=software.id == vuln.software_id)
        inner_join(vuln_type, on=vuln_type.id == vuln.vuln_type_id)
        inner_join(remediation_policy, on=remediation_policy.severity == vuln_type.severity)
        inner_join(person, on=person.id == vuln.owner_id)
        inner_join(team, on=team.id == person.team_id)
        inner_join(department, on=department.id == team.department_id)
        inner_join(org, on=org.id == department.org_id)
        return VulnerabilityPostureCandidate(
            vuln_id=vuln.id,
            vuln_type=vuln_type.type,
            description=vuln_type.description,
            instructions=vuln_type.instructions,
            severity=vuln_type.severity,
            date_discovered=vuln.date_discovered,
            date_addressed=vuln.date_addressed,
            target_date=date_add(vuln.date_discovered, days=remediation_policy.target_days),
            is_active=vuln.date_addressed.is_null(),
            device_id=device.id,
            device_platform=device_type.platform,
            device_model=device_type.model,
            software_id=software.id,
            software_name=software.name,
            software_version=software.version,
            person_id=person.id,
            person_name=person.name,
            team_id=team.id,
            team_name=team.name,
            department_id=department.id,
            department_name=department.name,
            org_id=org.id,
            org_name=org.name,
            device_owner_id=device.owner_id,
            device_os_id=device.os_id,
            device_vuln_ids=device.vuln_ids,
            device_apps=device.apps,
        )

    @step(input=posture_candidates, output=exposure_lane)
    def expose(self, candidate: VulnerabilityPostureCandidate) -> VulnerabilityExposure:
        installed = arr_exists(candidate.device_apps, lambda app: app.id == candidate.software_id)
        where(
            (candidate.device_owner_id == candidate.person_id)
            & array_contains(candidate.device_vuln_ids, candidate.vuln_id)
            & ((candidate.device_os_id == candidate.software_id) | installed)
        )
        return VulnerabilityExposure.project(candidate)

    def publish(self, finding: VulnerabilityExposure) -> VulnerabilityExposure:
        return VulnerabilityExposure.project(finding)
