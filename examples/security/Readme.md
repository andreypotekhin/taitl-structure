# Security Example

This example models a corporate security inventory. Static snapshots describe devices, software, people, and the
current organization hierarchy; application and vulnerability events arrive as streams. The model keeps canonical
security posture separate from active-finding reports, historical statistics, and remediation queues for broken or
stale inventory.

## Pipeline map

| Concern | Transform | Result | Boundary |
| --- | --- | --- | --- |
| Application audit | `EnrichAppEvents` | `AppAuditEvent` rows | Streaming, ten-minute watermark, ID deduplication. |
| Vulnerability audit | `EnrichVulnerabilityEvents` | Audit rows | Streaming, watermark, ID deduplication. |
| Current posture | `SecurityPosture` | Reconciled exposures | Batch-only canonical dataset. |
| Active reports | `ActiveVulnerabilityReports` | Scope-specific finding views | Batch views of active exposures. |
| Historical reporting | `VulnerabilityStatistics` | Scope metrics | Caller-supplied batch periods. |
| Inventory quality | `SecurityInventoryQuality` | Checks and remediation queues | Original-snapshot validation. |

## Enrich event streams

`EnrichAppEvents` receives streamed application events and static device, device-type, application, and scanner
references. `EnrichVulnerabilityEvents` receives streamed vulnerability events and static vulnerability, device,
person, and scanner references. Both transforms apply a ten-minute event-time watermark, deduplicate immutable event
IDs, and publish enriched audit rows.

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

Structure does not create sources or sinks. Callers choose the source, checkpoint, trigger, durable destination, and
start/stop lifecycle. Events outside Spark's watermark horizon may be discarded; retry-safe producers preserve IDs.

## Build the current security posture

`SecurityPosture` is batch-only. It joins vulnerability inventory to current device, software, person, team,
department, and organization context, then retains only reconciled `VulnerabilityExposure` rows. A retained row must
have a resolvable reference chain, the device owner must agree with the vulnerability owner, the device must list the
vulnerability ID, and the affected software must be its OS or an installed application. `is_active` is derived from
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
```

Rows failing reconciliation are intentionally excluded from posture rather than being treated as trustworthy findings.
Run inventory quality alongside it to obtain actionable explanations.

## Report active findings and history

`ActiveVulnerabilityReports` publishes the active exposure set at device, person, team, department, and organization
grains. The views retain the current context already captured in the posture rows.

`VulnerabilityStatistics` combines posture with vulnerability-event history and caller-provided weekly or monthly
`ReportingPeriod` rows. Period boundaries are inclusive. It deduplicates delivery by event ID, then uses the earliest
event timestamp for each `(vuln_id, action)` contribution, so repeat scanner observations do not inflate discovery or
address counts. It emits scope × period rows even when discovered and addressed counts are zero.

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
```

A vulnerability is active at a period end when it was discovered on or before that date and has no addressed date (or
was addressed later). This is a historical status calculation, distinct from the current posture's `is_active` field.

## Inspect inventory quality

`SecurityInventoryQuality` works on the original snapshots, not only rows eligible for posture. It publishes complete
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
