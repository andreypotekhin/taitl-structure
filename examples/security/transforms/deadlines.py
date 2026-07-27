from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.reporting import (
    DepartmentVulnerabilityDeadlineSummary,
    OrgVulnerabilityDeadlineSummary,
    PersonVulnerabilityDeadlineSummary,
    SecurityEvaluation,
    TeamVulnerabilityDeadlineSummary,
    VulnerabilityDeadlineActivity,
    VulnerabilityWorkflowExposure,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *


class VulnerabilityDeadlineReports(Transform):
    """Publish current, zero-filled remediation deadline summaries."""

    exposures = input(VulnerabilityWorkflowExposure)
    people = input(Person)
    teams = input(Team)
    departments = input(Department)
    orgs = input(Org)
    evaluation = input(SecurityEvaluation)
    activities = lane(VulnerabilityDeadlineActivity)
    person_summaries = output(PersonVulnerabilityDeadlineSummary)
    team_summaries = output(TeamVulnerabilityDeadlineSummary)
    department_summaries = output(DepartmentVulnerabilityDeadlineSummary)
    org_summaries = output(OrgVulnerabilityDeadlineSummary)

    @step(input=[exposures, evaluation], output=activities)
    def assess(
        self, finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation
    ) -> VulnerabilityDeadlineActivity:
        cross_join(evaluation, allow_cartesian=True)
        return VulnerabilityDeadlineActivity(
            person_id=finding.person_id,
            team_id=finding.team_id,
            department_id=finding.department_id,
            org_id=finding.org_id,
            as_of_date=evaluation.as_of_date,
            imminent_count=when(self._imminent(finding, evaluation), 1).otherwise(0),
            overdue_count=when(self._overdue(finding, evaluation), 1).otherwise(0),
        )

    @step(input=[people, evaluation, activities], output=person_summaries)
    def summarize_people(
        self, person: Person, evaluation: SecurityEvaluation, activity: VulnerabilityDeadlineActivity
    ) -> PersonVulnerabilityDeadlineSummary:
        cross_join(evaluation, allow_cartesian=True)
        left_join(activity, on=(activity.person_id == person.id) & (activity.as_of_date == evaluation.as_of_date))
        group_by(person_id=person.id, person_name=person.name, as_of_date=evaluation.as_of_date)
        return PersonVulnerabilityDeadlineSummary(
            person_id=person.id, person_name=person.name, **self._totals(activity, evaluation)
        )

    @step(input=[teams, evaluation, activities], output=team_summaries)
    def summarize_teams(
        self, team: Team, evaluation: SecurityEvaluation, activity: VulnerabilityDeadlineActivity
    ) -> TeamVulnerabilityDeadlineSummary:
        cross_join(evaluation, allow_cartesian=True)
        left_join(activity, on=(activity.team_id == team.id) & (activity.as_of_date == evaluation.as_of_date))
        group_by(team_id=team.id, team_name=team.name, as_of_date=evaluation.as_of_date)
        return TeamVulnerabilityDeadlineSummary(
            team_id=team.id, team_name=team.name, **self._totals(activity, evaluation)
        )

    @step(input=[departments, evaluation, activities], output=department_summaries)
    def summarize_departments(
        self, department: Department, evaluation: SecurityEvaluation, activity: VulnerabilityDeadlineActivity
    ) -> DepartmentVulnerabilityDeadlineSummary:
        cross_join(evaluation, allow_cartesian=True)
        left_join(
            activity,
            on=(activity.department_id == department.id) & (activity.as_of_date == evaluation.as_of_date),
        )
        group_by(department_id=department.id, department_name=department.name, as_of_date=evaluation.as_of_date)
        return DepartmentVulnerabilityDeadlineSummary(
            department_id=department.id,
            department_name=department.name,
            **self._totals(activity, evaluation),
        )

    @step(input=[orgs, evaluation, activities], output=org_summaries)
    def summarize_orgs(
        self, org: Org, evaluation: SecurityEvaluation, activity: VulnerabilityDeadlineActivity
    ) -> OrgVulnerabilityDeadlineSummary:
        cross_join(evaluation, allow_cartesian=True)
        left_join(activity, on=(activity.org_id == org.id) & (activity.as_of_date == evaluation.as_of_date))
        group_by(org_id=org.id, org_name=org.name, as_of_date=evaluation.as_of_date)
        return OrgVulnerabilityDeadlineSummary(org_id=org.id, org_name=org.name, **self._totals(activity, evaluation))

    @staticmethod
    def _imminent(finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation):
        return (
            finding.is_active
            & ~finding.is_deadline_paused
            & (evaluation.as_of_date >= date_sub(finding.target_date, days=7))
            & (evaluation.as_of_date < finding.target_date)
        )

    @staticmethod
    def _overdue(finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation):
        return finding.is_active & ~finding.is_deadline_paused & (evaluation.as_of_date > finding.target_date)

    @staticmethod
    def _totals(activity: VulnerabilityDeadlineActivity, evaluation: SecurityEvaluation) -> dict[str, object]:
        return {
            "as_of_date": evaluation.as_of_date,
            "imminent_count": sum(coalesce(activity.imminent_count, 0)),
            "overdue_count": sum(coalesce(activity.overdue_count, 0)),
        }
