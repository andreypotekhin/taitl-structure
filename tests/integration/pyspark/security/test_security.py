import csv
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from examples.security.schemas.assets import OS, App, Device, DeviceType, Scanner, Software
from examples.security.schemas.events import AppEvent, RawEvent, VulnEvent
from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.reporting import (
    DepartmentActiveVulnerability,
    DepartmentVulnerabilityStatistic,
    DeviceActiveVulnerability,
    OrgActiveVulnerability,
    OrgVulnerabilityStatistic,
    PersonActiveVulnerability,
    PersonVulnerabilityStatistic,
    ReportingPeriod,
    TeamActiveVulnerability,
    TeamVulnerabilityStatistic,
    VulnerabilityExposure,
    VulnerabilityInventoryCandidate,
    VulnerabilityInventoryCheck,
    VulnerabilityInventoryIssue,
    VulnerabilityLifecycle,
    VulnerabilityPeriodActivity,
    VulnerabilityPostureCandidate,
    VulnerabilityQualityCheck,
    VulnerabilityQualityIssue,
)
from examples.security.schemas.risk import Vuln, VulnType
from examples.security.transforms.posture import SecurityPosture
from examples.security.transforms.quality import SecurityInventoryQuality
from examples.security.transforms.reports import ActiveVulnerabilityReports, VulnerabilityStatistics
from structure.core.dsl.model.schemas.Schema import Schema

pytestmark = pytest.mark.integration

PACKAGE = "integration_security_generated"
FIXTURES = Path(__file__).resolve().parents[4] / "examples" / "fixtures" / "security"
SCHEMA_MODULES: Mapping[str, Sequence[type[Schema]]] = {
    "examples.security.schemas.assets": [DeviceType, Software, App, OS, Scanner, Device],
    "examples.security.schemas.events": [RawEvent, AppEvent, VulnEvent],
    "examples.security.schemas.organization": [Org, Department, Team, Person],
    "examples.security.schemas.reporting": [
        ReportingPeriod,
        VulnerabilityExposure,
        DeviceActiveVulnerability,
        PersonActiveVulnerability,
        TeamActiveVulnerability,
        DepartmentActiveVulnerability,
        OrgActiveVulnerability,
        PersonVulnerabilityStatistic,
        TeamVulnerabilityStatistic,
        DepartmentVulnerabilityStatistic,
        OrgVulnerabilityStatistic,
        VulnerabilityLifecycle,
        VulnerabilityPeriodActivity,
        VulnerabilityPostureCandidate,
        VulnerabilityQualityCheck,
        VulnerabilityQualityIssue,
        VulnerabilityInventoryCandidate,
        VulnerabilityInventoryCheck,
        VulnerabilityInventoryIssue,
    ],
    "examples.security.schemas.risk": [VulnType, Vuln],
}
TRANSFORMS = (
    (SecurityPosture, "examples.security.transforms.posture.SecurityPosture"),
    (ActiveVulnerabilityReports, "examples.security.transforms.reports.ActiveVulnerabilityReports"),
    (VulnerabilityStatistics, "examples.security.transforms.reports.VulnerabilityStatistics"),
    (SecurityInventoryQuality, "examples.security.transforms.quality.SecurityInventoryQuality"),
)


def test_security_fixtures_run_online_and_generated(spark, tmp_path) -> None:
    files = {}
    for transform, source in TRANSFORMS:
        files.update(
            render_generated_project(
                transform,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    with generated_project(tmp_path, PACKAGE, files):
        from importlib import import_module

        assets = import_module(f"{PACKAGE}.pyspark.schemas.assets")
        events = import_module(f"{PACKAGE}.pyspark.schemas.events")
        organization = import_module(f"{PACKAGE}.pyspark.schemas.organization")
        reporting = import_module(f"{PACKAGE}.pyspark.schemas.reporting")
        risk = import_module(f"{PACKAGE}.pyspark.schemas.risk")
        inputs = _inputs(spark, assets, events, organization, reporting, risk)

        online = _run(SecurityPosture, SecurityInventoryQuality, spark, "online", None, inputs)
        generated = _run(SecurityPosture, SecurityInventoryQuality, spark, "generated", PACKAGE, inputs)

        for name, order in _ORDERS.items():
            assert rows(online[name], *order) == rows(generated[name], *order)

        assert [row["vuln_id"] for row in rows(generated["org_active"], "vuln_id")] == ["vuln-open"]
        assert _statistic(generated["person_statistics"], date(2026, 1, 1), date(2026, 1, 7)) == (2, 1, 1)
        assert _statistic(generated["person_statistics"], date(2026, 2, 1), date(2026, 2, 7)) == (0, 0, 1)
        assert [row["vuln_id"] for row in rows(generated["reference_issues"], "vuln_id")] == [
            "vuln-bad-state",
            "vuln-missing-device",
        ]
        assert [row["vuln_id"] for row in rows(generated["reconciliation_issues"], "vuln_id")] == [
            "vuln-uninstalled",
            "vuln-unlisted",
        ]


_ORDERS = {
    "exposures": ("vuln_id",),
    "device_active": ("vuln_id",),
    "person_active": ("vuln_id",),
    "team_active": ("vuln_id",),
    "department_active": ("vuln_id",),
    "org_active": ("vuln_id",),
    "person_statistics": ("person_id", "period_kind", "period_start"),
    "team_statistics": ("team_id", "period_kind", "period_start"),
    "department_statistics": ("department_id", "period_kind", "period_start"),
    "org_statistics": ("org_id", "period_kind", "period_start"),
    "reference_checks": ("vuln_id",),
    "reference_issues": ("vuln_id",),
    "reconciliation_checks": ("vuln_id",),
    "reconciliation_issues": ("vuln_id",),
}


def _run(posture_type, quality_type, spark, execution_mode, generated_package, inputs):
    execution = session(spark, execution_mode=execution_mode, generated_package=generated_package)
    exposures = posture_type(**inputs).run(execution).exposures
    active = ActiveVulnerabilityReports(exposures=exposures).run(execution)
    statistics = VulnerabilityStatistics(
        exposures=exposures,
        events=inputs["events"],
        people=inputs["people"],
        teams=inputs["teams"],
        departments=inputs["departments"],
        orgs=inputs["orgs"],
        periods=inputs["periods"],
    ).run(execution)
    quality = quality_type(
        vulnerabilities=inputs["quality_vulnerabilities"],
        devices=inputs["devices"],
        device_types=inputs["device_types"],
        software=inputs["software"],
        vuln_types=inputs["vuln_types"],
        people=inputs["people"],
        teams=inputs["teams"],
        departments=inputs["departments"],
        orgs=inputs["orgs"],
    ).run(execution)
    return {
        "exposures": exposures,
        "device_active": active.device_active,
        "person_active": active.person_active,
        "team_active": active.team_active,
        "department_active": active.department_active,
        "org_active": active.org_active,
        "person_statistics": statistics.person_statistics,
        "team_statistics": statistics.team_statistics,
        "department_statistics": statistics.department_statistics,
        "org_statistics": statistics.org_statistics,
        "reference_checks": quality.reference_checks,
        "reference_issues": quality.reference_issues,
        "reconciliation_checks": quality.reconciliation_checks,
        "reconciliation_issues": quality.reconciliation_issues,
    }


def _inputs(spark, assets, events, organization, reporting, risk):
    software = _csv("software.csv")
    vulnerabilities = _vulnerabilities("vulnerabilities.csv")
    quality_vulnerabilities = vulnerabilities + _vulnerabilities("quality_vulnerabilities.csv")
    return {
        "device_types": spark.createDataFrame(_csv("device_types.csv"), assets.DEVICE_TYPE_SCHEMA),
        "software": spark.createDataFrame(software, assets.SOFTWARE_SCHEMA),
        "devices": spark.createDataFrame(_devices(), assets.DEVICE_SCHEMA),
        "vuln_types": spark.createDataFrame(_csv("vuln_types.csv"), risk.VULN_TYPE_SCHEMA),
        "people": spark.createDataFrame(_csv("people.csv"), organization.PERSON_SCHEMA),
        "teams": spark.createDataFrame(_csv("teams.csv"), organization.TEAM_SCHEMA),
        "departments": spark.createDataFrame(_csv("departments.csv"), organization.DEPARTMENT_SCHEMA),
        "orgs": spark.createDataFrame(_csv("orgs.csv"), organization.ORG_SCHEMA),
        "events": spark.createDataFrame(_events(), events.VULN_EVENT_SCHEMA),
        "periods": spark.createDataFrame(_periods(), reporting.REPORTING_PERIOD_SCHEMA),
        "vulnerabilities": spark.createDataFrame(vulnerabilities, risk.VULN_SCHEMA),
        "quality_vulnerabilities": spark.createDataFrame(quality_vulnerabilities, risk.VULN_SCHEMA),
    }


def _csv(name):
    with (FIXTURES / name).open(newline="", encoding="utf-8") as source:
        return [tuple(row.values()) for row in csv.DictReader(source)]


def _devices():
    return [
        (row["id"], row["device_type_id"], row["owner_id"], row["os_id"], row["apps"], row["vuln_ids"])
        for row in json.loads((FIXTURES / "devices.json").read_text(encoding="utf-8"))
    ]


def _vulnerabilities(name):
    with (FIXTURES / name).open(newline="", encoding="utf-8") as source:
        return [
            (
                row["id"],
                row["vuln_type_id"],
                row["device_id"],
                row["owner_id"],
                row["software_id"],
                date.fromisoformat(row["date_discovered"]),
                date.fromisoformat(row["date_addressed"]) if row["date_addressed"] else None,
                row["is_active"] == "true",
            )
            for row in csv.DictReader(source)
        ]


def _events():
    with (FIXTURES / "vulnerability_events.csv").open(newline="", encoding="utf-8") as source:
        return [
            (
                row["id"],
                row["device_id"],
                row["scanner_id"],
                datetime.fromisoformat(row["occurred_at"]),
                row["event_family"],
                row["vuln_id"],
                row["action"],
                row["description"],
                row["instructions"],
            )
            for row in csv.DictReader(source)
        ]


def _periods():
    with (FIXTURES / "periods.csv").open(newline="", encoding="utf-8") as source:
        return [
            (row["kind"], date.fromisoformat(row["start"]), date.fromisoformat(row["end"]))
            for row in csv.DictReader(source)
        ]


def _statistic(frame, start, end):
    matching = [
        row
        for row in rows(frame, "period_start")
        if row["person_id"] == "person-ava" and row["period_start"] == start and row["period_end"] == end
    ]
    assert len(matching) == 1
    row = matching[0]
    return row["discovered_count"], row["addressed_count"], row["active_count"]
