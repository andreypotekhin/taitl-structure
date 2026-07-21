# Security Example

This example models a corporate security inventory. Devices, software, people, and the current organization hierarchy
are described with static snapshots; application and vulnerability events arrive as streams. It separates the canonical
security posture dataset from the active-finding and historical-statistics reports built from it. It also makes
broken references and stale device inventory available as explicit remediation queues.

## Event streams
The system accepts events such as application install/uninstall/upgrade and vulnerability discovery.
`EnrichAppEvents` and `EnrichVulnerabilityEvents` are streaming-compatible.
They use an event-time watermark and remove duplicate event IDs, but leave source, sink, checkpoint, trigger, and query lifecycle to the caller.

```python
app_audits = EnrichAppEvents(
    events=app_events,
    devices=devices,
    device_types=device_types,
    apps=apps,
    scanners=scanners,
).run(session).audits

query = app_audits.writeStream.outputMode("append").option("checkpointLocation", checkpoint).format("memory").start()
```

Event transforms publish enriched `AppAuditEvent` and `VulnerabilityAuditEvent` rows.


## Posture, reports, and inventory quality

`SecurityPosture` is batch-only and creates `VulnerabilityExposure` rows with current device, software, person, team,
department, and organization context. Only fully resolvable, reconciled rows enter this posture: the device owner,
listed vulnerability ID, and affected OS or installed app must agree with the vulnerability. Its `is_active` field is
derived from `date_addressed is null`; the source `Vuln.is_active` is retained only as a quality check.
`ActiveVulnerabilityReports` produces the five active-finding views. 
`VulnerabilityStatistics` combines exposures with deduplicated vulnerability-event history and caller-supplied periods.
Its periods use `week` or `month` with inclusive start/end dates. It emits statistics for person, team, department, and
organization, retaining scope × period pairs with zero discovered or addressed events.

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

active = (
  ActiveVulnerabilityReports(exposures=exposures)
    .run(session).org_active
)

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

Delivery is deduplicated by event ID, then each `(vuln_id, action)` contributes at its earliest event timestamp. Thus
repeated scanner observations do not inflate discovery or address counts. A vulnerability is active at a period end
when it was discovered on or before that date and either has no addressed date or was addressed later.

`SecurityInventoryQuality` runs alongside posture with the original inventory snapshots. It publishes complete
`reference_checks` and `reconciliation_checks` tables plus filtered `reference_issues` and `reconciliation_issues`
remediation queues. Reference checks validate the device, device type, software, vulnerability type, person, current
organization hierarchy, device owner, and source lifecycle flag. Reconciliation checks every vulnerability with a
resolvable device: the device must list the vulnerability ID, and its affected software must be its OS or one of its
installed apps. A missing device stays a reference issue rather than being silently dropped.
