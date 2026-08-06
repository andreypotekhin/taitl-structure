import csv
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_projects, session
from integration.pyspark.support.rows import rows

from examples.security.schemas.alarms import TeamVulnerabilityAlarm
from examples.security.schemas.assets import OS, App, Device, DeviceType, Scanner, Software
from examples.security.schemas.events import AppEvent, RawEvent, VulnEvent
from examples.security.schemas.notifications import PersonVulnerabilityNotification
from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.remediation import (
    DepartmentRemediationWorkflowSummary,
    ExpiredExceptionVulnerability,
    ExpiringExceptionVulnerability,
    OrgRemediationWorkflowSummary,
    PendingExceptionVulnerability,
    PersonRemediationWorkflowSummary,
    RemediationCase,
    RemediationCaseAggregate,
    RemediationCaseCheck,
    RemediationCaseIssue,
    RemediationWorkflowActivity,
    RemediationWorkflowSummary,
    TeamRemediationWorkflowSummary,
    UnacknowledgedVulnerability,
    VulnerabilityWorkflowExposure,
)
from examples.security.schemas.reporting import (
    DeliveryReceipt,
    DepartmentActiveVulnerability,
    DepartmentVulnerabilityDeadlineSummary,
    DepartmentVulnerabilityStatistic,
    DeviceActiveVulnerability,
    OrgActiveVulnerability,
    OrgVulnerabilityDeadlineSummary,
    OrgVulnerabilityStatistic,
    PersonActiveVulnerability,
    PersonVulnerabilityDeadlineSummary,
    PersonVulnerabilityStatistic,
    ReportingPeriod,
    SecurityEvaluation,
    TeamActiveVulnerability,
    TeamVulnerabilityDeadlineSummary,
    TeamVulnerabilityStatistic,
    VulnerabilityDeadlineActivity,
    VulnerabilityDeadlineSummary,
    VulnerabilityDiscovery,
    VulnerabilityExposure,
    VulnerabilityInventoryCandidate,
    VulnerabilityInventoryCheck,
    VulnerabilityInventoryIssue,
    VulnerabilityLifecycle,
    VulnerabilityPeriodActivity,
    VulnerabilityPostureCandidate,
    VulnerabilityQualityCheck,
    VulnerabilityQualityIssue,
    VulnerabilityStatistic,
)
from examples.security.schemas.risk import RemediationPolicy, Vuln, VulnType
from examples.security.transforms.alarms import VulnerabilityAlarms
from examples.security.transforms.deadlines import VulnerabilityDeadlineReports
from examples.security.transforms.notify import VulnerabilityNotifications
from examples.security.transforms.posture import SecurityPosture
from examples.security.transforms.quality import SecurityInventoryQuality
from examples.security.transforms.remediate.workflow import VulnerabilityRemediationWorkflow
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
        SecurityEvaluation,
        DeliveryReceipt,
        VulnerabilityExposure,
        DeviceActiveVulnerability,
        PersonActiveVulnerability,
        TeamActiveVulnerability,
        DepartmentActiveVulnerability,
        OrgActiveVulnerability,
        VulnerabilityStatistic,
        PersonVulnerabilityStatistic,
        TeamVulnerabilityStatistic,
        DepartmentVulnerabilityStatistic,
        OrgVulnerabilityStatistic,
        VulnerabilityDeadlineSummary,
        PersonVulnerabilityDeadlineSummary,
        TeamVulnerabilityDeadlineSummary,
        DepartmentVulnerabilityDeadlineSummary,
        OrgVulnerabilityDeadlineSummary,
        VulnerabilityLifecycle,
        VulnerabilityDiscovery,
        VulnerabilityDeadlineActivity,
        VulnerabilityPeriodActivity,
        VulnerabilityPostureCandidate,
        VulnerabilityQualityCheck,
        VulnerabilityQualityIssue,
        VulnerabilityInventoryCandidate,
        VulnerabilityInventoryCheck,
        VulnerabilityInventoryIssue,
    ],
    "examples.security.schemas.remediate": [
        RemediationCase,
        RemediationCaseAggregate,
        RemediationCaseCheck,
        RemediationCaseIssue,
        VulnerabilityWorkflowExposure,
        UnacknowledgedVulnerability,
        PendingExceptionVulnerability,
        ExpiringExceptionVulnerability,
        ExpiredExceptionVulnerability,
        RemediationWorkflowActivity,
        RemediationWorkflowSummary,
        PersonRemediationWorkflowSummary,
        TeamRemediationWorkflowSummary,
        DepartmentRemediationWorkflowSummary,
        OrgRemediationWorkflowSummary,
    ],
    "examples.security.schemas.notifications": [PersonVulnerabilityNotification],
    "examples.security.schemas.alarms": [TeamVulnerabilityAlarm],
    "examples.security.schemas.risk": [VulnType, RemediationPolicy, Vuln],
}
TRANSFORMS = (
    (SecurityPosture, "examples.security.transforms.posture.SecurityPosture"),
    (
        VulnerabilityRemediationWorkflow,
        "examples.security.transforms.remediate.workflow.VulnerabilityRemediationWorkflow",
    ),
    (VulnerabilityNotifications, "examples.security.transforms.notify.VulnerabilityNotifications"),
    (VulnerabilityAlarms, "examples.security.transforms.alarms.VulnerabilityAlarms"),
    (VulnerabilityDeadlineReports, "examples.security.transforms.deadlines.VulnerabilityDeadlineReports"),
    (ActiveVulnerabilityReports, "examples.security.transforms.reports.ActiveVulnerabilityReports"),
    (VulnerabilityStatistics, "examples.security.transforms.reports.VulnerabilityStatistics"),
    (SecurityInventoryQuality, "examples.security.transforms.quality.SecurityInventoryQuality"),
)


def test_security_fixtures_run_online_and_generated(spark, tmp_path, cache_frames) -> None:
    files = render_generated_projects(
        TRANSFORMS,
        generated_package=PACKAGE,
        source_schema_modules=SCHEMA_MODULES,
    )

    with generated_project(tmp_path, PACKAGE, files):
        from importlib import import_module

        assets = import_module(f"{PACKAGE}.pyspark.schemas.assets")
        events = import_module(f"{PACKAGE}.pyspark.schemas.events")
        organization = import_module(f"{PACKAGE}.pyspark.schemas.organization")
        remediation = import_module(f"{PACKAGE}.pyspark.schemas.remediate")
        reporting = import_module(f"{PACKAGE}.pyspark.schemas.reporting")
        risk = import_module(f"{PACKAGE}.pyspark.schemas.risk")
        inputs = _inputs(spark, assets, events, organization, remediation, reporting, risk)

        online = _run(SecurityPosture, SecurityInventoryQuality, spark, "online", None, inputs, cache_frames)
        generated = _run(SecurityPosture, SecurityInventoryQuality, spark, "generated", PACKAGE, inputs, cache_frames)

        for name, order in _ORDERS.items():
            assert rows(online[name], *order) == rows(generated[name], *order)

        assert [row["vuln_id"] for row in rows(generated["org_active"], "vuln_id")] == ["vuln-open"]
        assert _statistic(generated["person_statistics"], date(2026, 1, 1), date(2026, 1, 7)) == (2, 1, 1)
        assert _statistic(generated["person_statistics"], date(2026, 2, 1), date(2026, 2, 7)) == (0, 0, 1)
        assert [row["target_date"] for row in rows(generated["person_active"], "vuln_id")] == [date(2026, 1, 8)]
        assert [row["vuln_id"] for row in rows(generated["discovery_notifications"], "vuln_id")] == [
            "vuln-fixed",
            "vuln-open",
        ]
        assert [row["vuln_id"] for row in rows(generated["imminent_notifications"], "vuln_id")] == ["vuln-open"]
        assert rows(generated["overdue_notifications"], "delivery_key") == []
        assert rows(generated["overdue_alarms"], "delivery_key") == []
        assert _deadline_summary(generated["person_summaries"], "person-ava") == (1, 0)
        assert _deadline_summary(generated["team_summaries"], "team-data") == (0, 0)
        assert [row["vuln_id"] for row in rows(generated["reference_issues"], "vuln_id")] == [
            "vuln-bad-state",
            "vuln-missing-device",
        ]
        assert [row["vuln_id"] for row in rows(generated["reconciliation_issues"], "vuln_id")] == [
            "vuln-uninstalled",
            "vuln-unlisted",
        ]


def test_remediation_workflow_pauses_only_valid_current_exceptions(spark, tmp_path, cache_frames) -> None:
    files = render_generated_projects(
        TRANSFORMS,
        generated_package=PACKAGE,
        source_schema_modules=SCHEMA_MODULES,
    )

    with generated_project(tmp_path, PACKAGE, files):
        from importlib import import_module

        assets = import_module(f"{PACKAGE}.pyspark.schemas.assets")
        events = import_module(f"{PACKAGE}.pyspark.schemas.events")
        organization = import_module(f"{PACKAGE}.pyspark.schemas.organization")
        remediation = import_module(f"{PACKAGE}.pyspark.schemas.remediate")
        reporting = import_module(f"{PACKAGE}.pyspark.schemas.reporting")
        risk = import_module(f"{PACKAGE}.pyspark.schemas.risk")
        inputs = _inputs(spark, assets, events, organization, remediation, reporting, risk)
        cache_frames(*inputs.values())

        valid = _with_cases(
            spark,
            inputs,
            remediation,
            [
                (
                    "vuln-open",
                    datetime(2026, 1, 1, 8),
                    datetime(2026, 1, 1, 9),
                    "maintenance",
                    "sec-ava",
                    datetime(2026, 1, 1, 10),
                    date(2026, 1, 2),
                )
            ],
        )
        cache_frames(valid["cases"])
        paused = _run(
            SecurityPosture,
            SecurityInventoryQuality,
            spark,
            "online",
            None,
            valid,
            cache_frames,
            include_reports=False,
            include_quality=False,
        )
        assert [row["vuln_id"] for row in rows(paused["expiring_exceptions"], "vuln_id")] == ["vuln-open"]
        assert rows(paused["imminent_notifications"], "delivery_key") == []
        assert _deadline_summary(paused["person_summaries"], "person-ava") == (0, 0)

        expired = _with_evaluation(spark, valid, reporting, date(2026, 1, 3))
        cache_frames(expired["evaluation"])
        resumed = _run(
            SecurityPosture,
            SecurityInventoryQuality,
            spark,
            "online",
            None,
            expired,
            cache_frames,
            include_reports=False,
            include_quality=False,
        )
        assert [row["vuln_id"] for row in rows(resumed["expired_exceptions"], "vuln_id")] == ["vuln-open"]
        assert [row["vuln_id"] for row in rows(resumed["imminent_notifications"], "vuln_id")] == ["vuln-open"]

        pending = _with_cases(
            spark,
            inputs,
            remediation,
            [("vuln-open", None, datetime(2026, 1, 1, 9), None, None, None, None)],
        )
        cache_frames(pending["cases"])
        pending_output = _run(
            SecurityPosture,
            SecurityInventoryQuality,
            spark,
            "online",
            None,
            pending,
            cache_frames,
            include_reports=False,
            include_quality=False,
        )
        assert [row["vuln_id"] for row in rows(pending_output["unacknowledged"], "vuln_id")] == ["vuln-open"]
        assert [row["vuln_id"] for row in rows(pending_output["pending_exceptions"], "vuln_id")] == ["vuln-open"]

        invalid = _with_cases(
            spark,
            inputs,
            remediation,
            [("vuln-unknown", None, None, None, None, None, None)],
        )
        cache_frames(invalid["cases"])
        invalid_output = _run(
            SecurityPosture,
            SecurityInventoryQuality,
            spark,
            "online",
            None,
            invalid,
            cache_frames,
            include_reports=False,
            include_quality=False,
        )
        assert [row["vuln_id"] for row in rows(invalid_output["case_issues"], "vuln_id")] == ["vuln-unknown"]


_ORDERS = {
    "exposures": ("vuln_id",),
    "workflow_exposures": ("vuln_id",),
    "case_checks": ("vuln_id",),
    "case_issues": ("vuln_id",),
    "unacknowledged": ("vuln_id",),
    "pending_exceptions": ("vuln_id",),
    "expiring_exceptions": ("vuln_id",),
    "expired_exceptions": ("vuln_id",),
    "device_active": ("vuln_id",),
    "person_active": ("vuln_id",),
    "team_active": ("vuln_id",),
    "department_active": ("vuln_id",),
    "org_active": ("vuln_id",),
    "person_statistics": ("person_id", "period_kind", "period_start"),
    "team_statistics": ("team_id", "period_kind", "period_start"),
    "department_statistics": ("department_id", "period_kind", "period_start"),
    "org_statistics": ("org_id", "period_kind", "period_start"),
    "discovery_notifications": ("delivery_key",),
    "imminent_notifications": ("delivery_key",),
    "overdue_notifications": ("delivery_key",),
    "overdue_alarms": ("delivery_key",),
    "person_summaries": ("person_id",),
    "team_summaries": ("team_id",),
    "department_summaries": ("department_id",),
    "org_summaries": ("org_id",),
    "workflow_person_summaries": ("person_id",),
    "workflow_team_summaries": ("team_id",),
    "workflow_department_summaries": ("department_id",),
    "workflow_org_summaries": ("org_id",),
    "reference_checks": ("vuln_id",),
    "reference_issues": ("vuln_id",),
    "reconciliation_checks": ("vuln_id",),
    "reconciliation_issues": ("vuln_id",),
}


def _run(
    posture_type,
    quality_type,
    spark,
    execution_mode,
    generated_package,
    inputs,
    cache_frames: Callable[..., None] | None = None,
    *,
    include_reports: bool = True,
    include_quality: bool = True,
):
    execution = session(spark, execution_mode=execution_mode, generated_package=generated_package)
    exposures = (
        posture_type(
            vulnerabilities=inputs["vulnerabilities"],
            devices=inputs["devices"],
            device_types=inputs["device_types"],
            software=inputs["software"],
            vuln_types=inputs["vuln_types"],
            remediation_policies=inputs["remediation_policies"],
            people=inputs["people"],
            teams=inputs["teams"],
            departments=inputs["departments"],
            orgs=inputs["orgs"],
        )
        .run(execution)
        .exposures
    )
    if cache_frames is not None:
        cache_frames(exposures)
        exposures.count()
    active = (
        ActiveVulnerabilityReports(exposures=exposures).run(execution) if include_reports else None
    )
    statistics = (
        VulnerabilityStatistics(
            exposures=exposures,
            events=inputs["events"],
            people=inputs["people"],
            teams=inputs["teams"],
            departments=inputs["departments"],
            orgs=inputs["orgs"],
            periods=inputs["periods"],
        ).run(execution)
        if include_reports
        else None
    )
    workflow = VulnerabilityRemediationWorkflow(
        exposures=exposures,
        vulnerabilities=inputs["vulnerabilities"],
        cases=inputs["cases"],
        people=inputs["people"],
        teams=inputs["teams"],
        departments=inputs["departments"],
        orgs=inputs["orgs"],
        evaluation=inputs["evaluation"],
    ).run(execution)
    workflow_exposures = workflow.workflow_exposures
    if cache_frames is not None:
        cache_frames(workflow_exposures)
        workflow_exposures.count()
    notifications = VulnerabilityNotifications(
        exposures=workflow_exposures,
        events=inputs["events"],
        people=inputs["people"],
        evaluation=inputs["evaluation"],
        receipts=inputs["receipts"],
    ).run(execution)
    alarms = VulnerabilityAlarms(
        exposures=workflow_exposures,
        evaluation=inputs["evaluation"],
        receipts=inputs["receipts"],
    ).run(execution)
    deadlines = VulnerabilityDeadlineReports(
        exposures=workflow_exposures,
        people=inputs["people"],
        teams=inputs["teams"],
        departments=inputs["departments"],
        orgs=inputs["orgs"],
        evaluation=inputs["evaluation"],
    ).run(execution)
    quality = (
        quality_type(
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
        if include_quality
        else None
    )
    result = {
        "exposures": exposures,
        "workflow_exposures": workflow_exposures,
        "case_checks": workflow.case_checks,
        "case_issues": workflow.case_issues,
        "unacknowledged": workflow.unacknowledged,
        "pending_exceptions": workflow.pending_exceptions,
        "expiring_exceptions": workflow.expiring_exceptions,
        "expired_exceptions": workflow.expired_exceptions,
        "workflow_person_summaries": workflow.person_summaries,
        "workflow_team_summaries": workflow.team_summaries,
        "workflow_department_summaries": workflow.department_summaries,
        "workflow_org_summaries": workflow.org_summaries,
        "discovery_notifications": notifications.discovery_notifications,
        "imminent_notifications": notifications.imminent_notifications,
        "overdue_notifications": notifications.overdue_notifications,
        "overdue_alarms": alarms.overdue_alarms,
        "person_summaries": deadlines.person_summaries,
        "team_summaries": deadlines.team_summaries,
        "department_summaries": deadlines.department_summaries,
        "org_summaries": deadlines.org_summaries,
    }
    if include_reports:
        assert active is not None and statistics is not None
        result.update(
            {
                "device_active": active.device_active,
                "person_active": active.person_active,
                "team_active": active.team_active,
                "department_active": active.department_active,
                "org_active": active.org_active,
                "person_statistics": statistics.person_statistics,
                "team_statistics": statistics.team_statistics,
                "department_statistics": statistics.department_statistics,
                "org_statistics": statistics.org_statistics,
            }
        )
    if include_quality:
        assert quality is not None
        result.update(
            {
                "reference_checks": quality.reference_checks,
                "reference_issues": quality.reference_issues,
                "reconciliation_checks": quality.reconciliation_checks,
                "reconciliation_issues": quality.reconciliation_issues,
            }
        )
    return result


def _inputs(spark, assets, events, organization, remediation, reporting, risk):
    software = _csv("software.csv")
    vulnerabilities = _vulnerabilities("vulnerabilities.csv")
    quality_vulnerabilities = vulnerabilities + _vulnerabilities("quality_vulnerabilities.csv")
    return {
        "device_types": spark.createDataFrame(_csv("device_types.csv"), assets.DEVICE_TYPE_SCHEMA),
        "software": spark.createDataFrame(software, assets.SOFTWARE_SCHEMA),
        "devices": spark.createDataFrame(_devices(), assets.DEVICE_SCHEMA),
        "vuln_types": spark.createDataFrame(_csv("vuln_types.csv"), risk.VULN_TYPE_SCHEMA),
        "remediation_policies": spark.createDataFrame(_policies(), risk.REMEDIATION_POLICY_SCHEMA),
        "people": spark.createDataFrame(_csv("people.csv"), organization.PERSON_SCHEMA),
        "teams": spark.createDataFrame(_csv("teams.csv"), organization.TEAM_SCHEMA),
        "departments": spark.createDataFrame(_csv("departments.csv"), organization.DEPARTMENT_SCHEMA),
        "orgs": spark.createDataFrame(_csv("orgs.csv"), organization.ORG_SCHEMA),
        "events": spark.createDataFrame(_events(), events.VULN_EVENT_SCHEMA),
        "periods": spark.createDataFrame(_periods(), reporting.REPORTING_PERIOD_SCHEMA),
        "vulnerabilities": spark.createDataFrame(vulnerabilities, risk.VULN_SCHEMA),
        "quality_vulnerabilities": spark.createDataFrame(quality_vulnerabilities, risk.VULN_SCHEMA),
        "evaluation": spark.createDataFrame([(date(2026, 1, 2),)], reporting.SECURITY_EVALUATION_SCHEMA),
        "receipts": spark.createDataFrame([], reporting.DELIVERY_RECEIPT_SCHEMA),
        "cases": spark.createDataFrame([], remediation.REMEDIATION_CASE_SCHEMA),
    }


def _with_cases(spark, inputs, remediation, cases):
    return {**inputs, "cases": spark.createDataFrame(cases, remediation.REMEDIATION_CASE_SCHEMA)}


def _with_evaluation(spark, inputs, reporting, as_of_date):
    return {**inputs, "evaluation": spark.createDataFrame([(as_of_date,)], reporting.SECURITY_EVALUATION_SCHEMA)}


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


def _policies():
    with (FIXTURES / "remediation_policies.csv").open(newline="", encoding="utf-8") as source:
        return [(row["severity"], int(row["target_days"])) for row in csv.DictReader(source)]


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


def _deadline_summary(frame, scope_id):
    matching = [row for row in rows(frame, "as_of_date") if row.get("person_id", row.get("team_id")) == scope_id]
    assert len(matching) == 1
    row = matching[0]
    return row["imminent_count"], row["overdue_count"]
