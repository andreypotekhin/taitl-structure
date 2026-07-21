from datetime import date, datetime


def test_security_reference_reports_lifecycle_metrics_period_end_activity_and_zero_scope_periods() -> None:
    findings = [
        {
            "id": "vuln-open",
            "person_id": "person-ava",
            "discovered": date(2026, 1, 1),
            "addressed": None,
            "active": True,
        },
        {
            "id": "vuln-fixed",
            "person_id": "person-ava",
            "discovered": date(2026, 1, 3),
            "addressed": date(2026, 1, 4),
            "active": False,
        },
    ]
    events = [
        ("event-open", "vuln-open", "Detected", datetime(2026, 1, 1, 9)),
        ("event-open", "vuln-open", "Detected", datetime(2026, 1, 1, 9)),
        ("event-open-repeat", "vuln-open", "Detected", datetime(2026, 1, 5, 9)),
        ("event-fixed-detected", "vuln-fixed", "Detected", datetime(2026, 1, 3, 9)),
        ("event-fixed-addressed", "vuln-fixed", "Addressed", datetime(2026, 1, 4, 9)),
    ]

    assert _active(findings) == ["vuln-open"]
    assert _statistics("person-ava", date(2026, 1, 1), date(2026, 1, 7), findings, events) == (2, 1, 1)
    assert _statistics("person-ava", date(2026, 2, 1), date(2026, 2, 7), findings, events) == (0, 0, 1)
    assert _statistics("person-ben", date(2026, 1, 1), date(2026, 1, 7), findings, events) == (0, 0, 0)


def test_security_reference_quality_identifies_invalid_state_and_device_inventory() -> None:
    device = {
        "id": "device-ava",
        "owner_id": "person-ava",
        "os_id": "macos",
        "apps": {"browser"},
        "vulns": {"vuln-open", "vuln-uninstalled"},
    }
    stale = {
        "id": "vuln-stale",
        "device_id": "device-ava",
        "owner_id": "person-ava",
        "software_id": "browser",
        "addressed": None,
        "active": False,
    }
    unlisted = {
        "id": "vuln-unlisted",
        "device_id": "device-ava",
        "owner_id": "person-ava",
        "software_id": "browser",
        "addressed": None,
        "active": True,
    }
    uninstalled = {
        "id": "vuln-uninstalled",
        "device_id": "device-ava",
        "owner_id": "person-ava",
        "software_id": "other",
        "addressed": None,
        "active": True,
    }
    unknown = {
        "id": "vuln-unknown",
        "device_id": "device-missing",
        "owner_id": "person-ava",
        "software_id": "browser",
        "addressed": None,
        "active": True,
    }

    assert _reference_issues(stale, device) == ["is_active disagrees with date_addressed"]
    assert _reconciliation_issues(unlisted, device) == ["device does not list vulnerability"]
    assert _reconciliation_issues(uninstalled, device) == ["device does not contain affected software"]
    assert _reference_issues(unknown, None) == ["missing device"]


def _active(findings):
    return [finding["id"] for finding in findings if finding["addressed"] is None]


def _statistics(person_id, start, end, findings, events):
    relevant = [finding for finding in findings if finding["person_id"] == person_id]
    by_id = {finding["id"]: finding for finding in relevant}
    delivered = {event[0]: event for event in events if event[1] in by_id}
    lifecycle: dict[tuple[str, str], datetime] = {}
    for _, vuln_id, action, occurred in delivered.values():
        key = (vuln_id, action)
        lifecycle[key] = min(lifecycle.get(key, occurred), occurred)
    discovered = sum(
        action == "Detected" and start <= occurred.date() <= end for (_, action), occurred in lifecycle.items()
    )
    addressed = sum(
        action == "Addressed" and start <= occurred.date() <= end for (_, action), occurred in lifecycle.items()
    )
    active = sum(
        finding["discovered"] <= end and (finding["addressed"] is None or finding["addressed"] > end)
        for finding in relevant
    )
    return discovered, addressed, active


def _reference_issues(vuln, device):
    if device is None:
        return ["missing device"]
    issues = []
    if vuln["owner_id"] != device["owner_id"]:
        issues.append("device owner disagrees with vulnerability owner")
    if vuln["active"] != (vuln["addressed"] is None):
        issues.append("is_active disagrees with date_addressed")
    return issues


def _reconciliation_issues(vuln, device):
    issues = []
    if vuln["id"] not in device["vulns"]:
        issues.append("device does not list vulnerability")
    if vuln["software_id"] != device["os_id"] and vuln["software_id"] not in device["apps"]:
        issues.append("device does not contain affected software")
    return issues
