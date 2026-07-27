from typing import Any, cast

import pytest

from examples.security.transforms.alarms import VulnerabilityAlarms
from examples.security.transforms.deadlines import VulnerabilityDeadlineReports
from examples.security.transforms.events import EnrichAppEvents, EnrichVulnerabilityEvents
from examples.security.transforms.notify import VulnerabilityNotifications
from examples.security.transforms.posture import SecurityPosture
from examples.security.transforms.quality import SecurityInventoryQuality
from examples.security.transforms.reports import ActiveVulnerabilityReports, VulnerabilityStatistics
from examples.security.transforms.workflow import VulnerabilityRemediationWorkflow
from structure.core.compiler.api import Compiler


@pytest.mark.parametrize(
    "transform",
    [
        EnrichAppEvents,
        EnrichVulnerabilityEvents,
        SecurityPosture,
        VulnerabilityNotifications,
        VulnerabilityAlarms,
        VulnerabilityDeadlineReports,
        VulnerabilityRemediationWorkflow,
        ActiveVulnerabilityReports,
        VulnerabilityStatistics,
        SecurityInventoryQuality,
    ],
)
def test_security_transforms_compile(transform) -> None:
    Compiler.frontend.compile()(transform, materialize_schemas=False)


def test_security_transforms_expose_preparation_and_report_boundaries() -> None:
    plan = cast(Any, Compiler.frontend.compile()(SecurityPosture, materialize_schemas=False).analysis)
    active = cast(Any, Compiler.frontend.compile()(ActiveVulnerabilityReports, materialize_schemas=False).analysis)
    statistics = cast(Any, Compiler.frontend.compile()(VulnerabilityStatistics, materialize_schemas=False).analysis)
    notifications = cast(
        Any, Compiler.frontend.compile()(VulnerabilityNotifications, materialize_schemas=False).analysis
    )
    alarms = cast(Any, Compiler.frontend.compile()(VulnerabilityAlarms, materialize_schemas=False).analysis)
    deadlines = cast(Any, Compiler.frontend.compile()(VulnerabilityDeadlineReports, materialize_schemas=False).analysis)
    workflow = cast(
        Any, Compiler.frontend.compile()(VulnerabilityRemediationWorkflow, materialize_schemas=False).analysis
    )
    quality = cast(Any, Compiler.frontend.compile()(SecurityInventoryQuality, materialize_schemas=False).analysis)

    assert [item.name for item in plan.outputs] == ["exposures"]
    assert [item.name for item in active.outputs] == [
        "device_active",
        "person_active",
        "team_active",
        "department_active",
        "org_active",
    ]
    assert [item.name for item in statistics.outputs] == [
        "person_statistics",
        "team_statistics",
        "department_statistics",
        "org_statistics",
    ]
    assert [item.name for item in notifications.outputs] == [
        "discovery_notifications",
        "imminent_notifications",
        "overdue_notifications",
    ]
    assert [item.name for item in alarms.outputs] == ["overdue_alarms"]
    assert [item.name for item in deadlines.outputs] == [
        "person_summaries",
        "team_summaries",
        "department_summaries",
        "org_summaries",
    ]
    assert [item.name for item in workflow.outputs] == [
        "case_checks",
        "case_issues",
        "workflow_exposures",
        "unacknowledged",
        "pending_exceptions",
        "expiring_exceptions",
        "expired_exceptions",
        "person_summaries",
        "team_summaries",
        "department_summaries",
        "org_summaries",
    ]
    assert [item.name for item in quality.outputs] == [
        "reference_checks",
        "reference_issues",
        "reconciliation_checks",
        "reconciliation_issues",
    ]
