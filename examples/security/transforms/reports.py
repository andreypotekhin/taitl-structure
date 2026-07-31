from examples.security.schemas.events import VulnEvent
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
    VulnerabilityLifecycle,
    VulnerabilityPeriodActivity,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *


class ActiveVulnerabilityReports(Transform):
    """Publish active vulnerabilities at each requested organizational scope."""

    exposures = input(VulnerabilityExposure)
    device_active = output(DeviceActiveVulnerability)
    person_active = output(PersonActiveVulnerability)
    team_active = output(TeamActiveVulnerability)
    department_active = output(DepartmentActiveVulnerability)
    org_active = output(OrgActiveVulnerability)

    @step(output=device_active)
    def device_findings(self, finding: VulnerabilityExposure) -> DeviceActiveVulnerability:
        where(finding.is_active)
        return DeviceActiveVulnerability.base(finding)

    @step(input=exposures, output=person_active)
    def person_findings(self, finding: VulnerabilityExposure) -> PersonActiveVulnerability:
        where(finding.is_active)
        return PersonActiveVulnerability.base(finding)

    @step(input=exposures, output=team_active)
    def team_findings(self, finding: VulnerabilityExposure) -> TeamActiveVulnerability:
        where(finding.is_active)
        return TeamActiveVulnerability.base(finding)

    @step(input=exposures, output=department_active)
    def department_findings(self, finding: VulnerabilityExposure) -> DepartmentActiveVulnerability:
        where(finding.is_active)
        return DepartmentActiveVulnerability.base(finding)

    @step(input=exposures, output=org_active)
    def org_findings(self, finding: VulnerabilityExposure) -> OrgActiveVulnerability:
        where(finding.is_active)
        return OrgActiveVulnerability.base(finding)


class VulnerabilityStatistics(Transform):
    """Build zero-filled weekly and monthly vulnerability statistics by scope."""

    exposures = input(VulnerabilityExposure)
    events = input(VulnEvent)
    people = input(Person)
    teams = input(Team)
    departments = input(Department)
    orgs = input(Org)
    periods = input(ReportingPeriod)
    deduped_events = lane(VulnEvent)
    lifecycles = lane(VulnerabilityLifecycle)
    activities = lane(VulnerabilityPeriodActivity)
    person_statistics = output(PersonVulnerabilityStatistic)
    team_statistics = output(TeamVulnerabilityStatistic)
    department_statistics = output(DepartmentVulnerabilityStatistic)
    org_statistics = output(OrgVulnerabilityStatistic)

    @step(output=deduped_events)
    def dedupe_delivery(self, event: VulnEvent) -> VulnEvent:
        drop_duplicates(event.id)
        return VulnEvent.project(event)

    @step(output=lifecycles)
    def lifecycle(self, event: VulnEvent) -> VulnerabilityLifecycle:
        where((event.action == "Detected") | (event.action == "Addressed"))
        group_by(vuln_id=event.vuln_id)
        return VulnerabilityLifecycle.project(event)(
            detected_at=min(event.occurred_at, where=event.action == "Detected"),
            addressed_at=min(event.occurred_at, where=event.action == "Addressed"),
        )

    @step(input=[exposures, periods, lifecycles], output=activities)
    def activity(
        self, finding: VulnerabilityExposure, period: ReportingPeriod, lifecycle: VulnerabilityLifecycle
    ) -> VulnerabilityPeriodActivity:
        cross_join(period, allow_cartesian=True)
        left_join(lifecycle, on=lifecycle.vuln_id == finding.vuln_id)
        return VulnerabilityPeriodActivity.project(finding)(
            period_kind=period.kind,
            period_start=period.start,
            period_end=period.end,
            discovered_count=self._in_period(lifecycle.detected_at, period),
            addressed_count=self._in_period(lifecycle.addressed_at, period),
            active_count=when(
                (finding.date_discovered <= period.end)
                & (finding.date_addressed.is_null() | (finding.date_addressed > period.end)),
                1,
            ).otherwise(0),
        )

    @step(input=[people, periods, activities], output=person_statistics)
    def person_periods(
        self, person: Person, period: ReportingPeriod, activity: VulnerabilityPeriodActivity
    ) -> PersonVulnerabilityStatistic:
        cross_join(period, allow_cartesian=True)
        left_join(
            activity,
            on=(activity.person_id == person.id)
            & (activity.period_kind == period.kind)
            & (activity.period_start == period.start)
            & (activity.period_end == period.end),
        )
        group_by(
            person_id=person.id,
            person_name=person.name,
            period_kind=period.kind,
            period_start=period.start,
            period_end=period.end,
        )
        return PersonVulnerabilityStatistic(
            person_id=person.id, person_name=person.name, **self._totals(activity, period)
        )

    @step(input=[teams, periods, activities], output=team_statistics)
    def team_periods(
        self, team: Team, period: ReportingPeriod, activity: VulnerabilityPeriodActivity
    ) -> TeamVulnerabilityStatistic:
        cross_join(period, allow_cartesian=True)
        left_join(
            activity,
            on=(activity.team_id == team.id)
            & (activity.period_kind == period.kind)
            & (activity.period_start == period.start)
            & (activity.period_end == period.end),
        )
        group_by(
            team_id=team.id,
            team_name=team.name,
            period_kind=period.kind,
            period_start=period.start,
            period_end=period.end,
        )
        return TeamVulnerabilityStatistic(team_id=team.id, team_name=team.name, **self._totals(activity, period))

    @step(input=[departments, periods, activities], output=department_statistics)
    def department_periods(
        self, department: Department, period: ReportingPeriod, activity: VulnerabilityPeriodActivity
    ) -> DepartmentVulnerabilityStatistic:
        cross_join(period, allow_cartesian=True)
        left_join(
            activity,
            on=(activity.department_id == department.id)
            & (activity.period_kind == period.kind)
            & (activity.period_start == period.start)
            & (activity.period_end == period.end),
        )
        group_by(
            department_id=department.id,
            department_name=department.name,
            period_kind=period.kind,
            period_start=period.start,
            period_end=period.end,
        )
        return DepartmentVulnerabilityStatistic(
            department_id=department.id,
            department_name=department.name,
            **self._totals(activity, period),
        )

    @step(input=[orgs, periods, activities], output=org_statistics)
    def org_periods(
        self, org: Org, period: ReportingPeriod, activity: VulnerabilityPeriodActivity
    ) -> OrgVulnerabilityStatistic:
        cross_join(period, allow_cartesian=True)
        left_join(
            activity,
            on=(activity.org_id == org.id)
            & (activity.period_kind == period.kind)
            & (activity.period_start == period.start)
            & (activity.period_end == period.end),
        )
        group_by(
            org_id=org.id,
            org_name=org.name,
            period_kind=period.kind,
            period_start=period.start,
            period_end=period.end,
        )
        return OrgVulnerabilityStatistic(org_id=org.id, org_name=org.name, **self._totals(activity, period))

    @staticmethod
    def _in_period(at, period: ReportingPeriod):
        occurred_on = to_date(at)
        return when((occurred_on >= period.start) & (occurred_on < date_add(period.end, days=1)), 1).otherwise(0)

    @staticmethod
    def _totals(activity: VulnerabilityPeriodActivity, period: ReportingPeriod) -> dict[str, object]:
        return {
            "period_kind": period.kind,
            "period_start": period.start,
            "period_end": period.end,
            "discovered_count": sum(coalesce(activity.discovered_count, 0)),
            "addressed_count": sum(coalesce(activity.addressed_count, 0)),
            "active_count": sum(coalesce(activity.active_count, 0)),
        }
