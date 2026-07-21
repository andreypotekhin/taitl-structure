from examples.security.schemas.assets import Device, DeviceType, Software
from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.reporting import VulnerabilityExposure, VulnerabilityPostureCandidate
from examples.security.schemas.risk import Vuln, VulnType
from structure import Transform, input, lane, output, raw, step
from structure.plugin.pyspark import *


class SecurityPosture(Transform):
    """Enrich vulnerability inventory with its current organizational context."""

    vulnerabilities = input(Vuln)
    devices = input(Device)
    device_types = input(DeviceType)
    software = input(Software)
    vuln_types = input(VulnType)
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
        person: Person,
        team: Team,
        department: Department,
        org: Org,
    ) -> VulnerabilityPostureCandidate:
        inner_join(device, on=device.id == vuln.device_id)
        inner_join(device_type, on=device_type.id == device.device_type_id)
        inner_join(software, on=software.id == vuln.software_id)
        inner_join(vuln_type, on=vuln_type.id == vuln.vuln_type_id)
        inner_join(person, on=person.id == vuln.owner_id)
        inner_join(team, on=team.id == person.team_id)
        inner_join(department, on=department.id == team.department_id)
        inner_join(org, on=org.id == department.org_id)
        return VulnerabilityPostureCandidate(
            vuln_id=vuln.id,
            vuln_type=vuln_type.type,
            description=vuln_type.description,
            instructions=vuln_type.instructions,
            date_discovered=vuln.date_discovered,
            date_addressed=vuln.date_addressed,
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

    @raw(inout=lane(posture_candidates) | lane(posture_candidates))
    def retain_reconciled_inventory(self, *, posture_candidates, spark, ctx):
        from pyspark.sql import functions as F

        installed = F.exists(F.col("device_apps"), lambda app: app["id"] == F.col("software_id"))
        return posture_candidates.where(
            (F.col("device_owner_id") == F.col("person_id"))
            & F.array_contains(F.col("device_vuln_ids"), F.col("vuln_id"))
            & ((F.col("device_os_id") == F.col("software_id")) | installed)
        )

    @step(input=posture_candidates, output=exposure_lane)
    def expose(self, candidate: VulnerabilityPostureCandidate) -> VulnerabilityExposure:
        return VulnerabilityExposure.base(candidate)

    @step(input=exposure_lane, output=exposures)
    def publish(self, finding: VulnerabilityExposure) -> VulnerabilityExposure:
        return VulnerabilityExposure.base(finding)
