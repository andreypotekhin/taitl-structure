# Security Example App

This example models a corporate security inventory. Model describes devices, software, people, and  organization hierarchy; application and vulnerability events arrive as streams. The application aggregates
security posture per device/user/team/org, as well as active vulnerability reports, historical statistics, and remediation queues.

| Concern | Transform | Result | Details |
| --- | --- | --- | --- |
| Software audit | `EnrichAppEvents` | `AppAuditEvent` | Streaming, ten-minute watermark, ID deduplication. |
| Vulnerability audit | `EnrichVulnerabilityEvents` | Audit rows | Streaming, watermark, ID deduplication. |
| Security posture | `SecurityPosture` | Reconciled exposures | Batch-only canonical dataset. |
| Active findings reports | `ActiveVulnerabilityReports` | Scope-specific finding views | Active exposures. |
| Historical reporting | `VulnerabilityStatistics` | Scope metrics | Aggregated reports. |
| Inventory quality | `SecurityInventoryQuality` | Checks and remediation queues | Checks and issues. |

## Event streams

`EnrichAppEvents` receives streamed application events like installing/uninstalling/upgrading of a mobile app, and static device, device-type, application, and scanner
references. `EnrichVulnerabilityEvents` receives streamed per-device vulnerability events from security scanners, and static vulnerability, device,
person, and scanner references.

Both transforms apply a ten-minute event-time watermark, deduplicate immutable event
IDs, and publish enriched audit rows.

Structure does not create sources or sinks. Callers choose the source, checkpoint, trigger, durable destination, and
start/stop lifecycle. 

```python
app_audits = EnrichAppEvents(
    events=app_events,
    devices=devices,
    device_types=device_types,
    apps=apps,
    scanners=scanners,
).run(session).audits

query = (
    app_audits.writeStream.outputMode("append")
    .option("checkpointLocation", checkpoint)
    .format("memory")
    .start()
)
```

Events outside Spark's watermark horizon may be discarded; retry-safe producers preserve IDs.

## Security posture

`SecurityPosture` joins vulnerability inventory to current device, software, person, team,
department, and organization context, then outputs reconciled `VulnerabilityExposure` rows. `is_active` is derived from
`date_addressed is null`; the source lifecycle flag is a quality check, not the posture truth.

```python
exposures = SecurityPosture(
    vulnerabilities=vulnerabilities,
    devices=devices,
    device_types=device_types,
    software=software,
    vuln_types=vuln_types,
    people=people,
    teams=teams,
    departments=departments,
    orgs=orgs,
).run(session).exposures

# Use as input for an operational remediation view.
active_exposures = exposures.where("is_active")
```

Rows failing reconciliation are excluded from posture rather than being treated as trustworthy findings.
Run inventory quality transform to obtain actionable explanations.

## Active findings and history reports

`ActiveVulnerabilityReports` publishes the active exposure set at device, person, team, department, and organization
levels. The views reflect the current context already captured in the posture rows.

`VulnerabilityStatistics` combines posture with vulnerability-event history and `ReportingPeriod` rows. It deduplicates by event ID, then uses the earliest
event timestamp for each `(vuln_id, action)` contribution, so repeat scanner observations do not inflate discovery or
counts. It emits scope × period rows even when discovered and addressed counts are zero.

```python
active = ActiveVulnerabilityReports(exposures=exposures).run(session).org_active

monthly = VulnerabilityStatistics(
    exposures=exposures,
    events=vulnerability_events,
    people=people,
    teams=teams,
    departments=departments,
    orgs=orgs,
    periods=periods,
).run(session).org_statistics

active_by_discovery = active.orderBy("date_discovered", "vuln_id")
monthly_history = monthly.orderBy("period_end", "org_id")
```

A vulnerability is active at a period end if it was discovered on or before that date and has no addressed date, or
was addressed later. This historical status calculation is distinct from the current posture's `is_active` field.

## Inventory quality

`SecurityInventoryQuality` works on the original snapshots, not only rows eligible for posture. It publishes
`reference_checks` and `reconciliation_checks` tables plus the filtered `reference_issues` and `reconciliation_issues`
remediation queues.

Reference checks cover device, device type, software, vulnerability type, person, current organization hierarchy,
device owner, and consistency between the source lifecycle flag and `date_addressed`. Reconciliation checks every
vulnerability with a resolvable device: the device must list the vulnerability and identify the affected software as
its OS or an installed application. A missing device remains a reference issue rather than disappearing silently.

```python
quality = SecurityInventoryQuality(
    vulnerabilities=vulnerabilities,
    devices=devices,
    device_types=device_types,
    software=software,
    vuln_types=vuln_types,
    people=people,
    teams=teams,
    departments=departments,
    orgs=orgs,
).run(session)

reference_queue = quality.reference_issues
reconciliation_queue = quality.reconciliation_issues
```
