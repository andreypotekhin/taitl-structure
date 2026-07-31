from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.remediation import (
    DepartmentRemediationWorkflowSummary,
    ExpiredExceptionVulnerability,
    ExpiringExceptionVulnerability,
    OrgRemediationWorkflowSummary,
    PendingExceptionVulnerability,
    PersonRemediationWorkflowSummary,
    RemediationWorkflowActivity,
    TeamRemediationWorkflowSummary,
    UnacknowledgedVulnerability,
    VulnerabilityWorkflowExposure,
)
from examples.security.schemas.reporting import SecurityEvaluation
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *


class VulnerabilityRemediationSummaries(Transform):
    """Publish zero-filled remediate workflow summaries."""

    workflow_exposures = input(VulnerabilityWorkflowExposure)
    unacknowledged = input(UnacknowledgedVulnerability)
    pending_exceptions = input(PendingExceptionVulnerability)
    expiring_exceptions = input(ExpiringExceptionVulnerability)
    expired_exceptions = input(ExpiredExceptionVulnerability)
    people = input(Person)
    teams = input(Team)
    departments = input(Department)
    orgs = input(Org)
    evaluation = input(SecurityEvaluation)
    activities = lane(RemediationWorkflowActivity)
    person_summaries = output(PersonRemediationWorkflowSummary)
    team_summaries = output(TeamRemediationWorkflowSummary)
    department_summaries = output(DepartmentRemediationWorkflowSummary)
    org_summaries = output(OrgRemediationWorkflowSummary)

    @step(output=activities)
    def assess(
        self, finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation
    ) -> RemediationWorkflowActivity:
        cross_join(evaluation, allow_cartesian=True)
        return RemediationWorkflowActivity.project(finding, evaluation)(
            unacknowledged_count=when(finding.is_active & ~finding.is_acknowledged, 1).otherwise(0),
            pending_exception_count=when(
                finding.is_active
                & finding.exception_requested_at.is_not_null()
                & finding.exception_approved_at.is_null(),
                1,
            ).otherwise(0),
            expiring_exception_count=when(
                finding.is_active
                & finding.exception_approved_at.is_not_null()
                & finding.exception_expires_on.is_not_null()
                & (finding.exception_expires_on >= evaluation.as_of_date)
                & (finding.exception_expires_on <= date_add(evaluation.as_of_date, days=7)),
                1,
            ).otherwise(0),
            expired_exception_count=when(
                finding.is_active
                & finding.exception_approved_at.is_not_null()
                & finding.exception_expires_on.is_not_null()
                & (finding.exception_expires_on < evaluation.as_of_date),
                1,
            ).otherwise(0),
        )

    @step(input=[people, evaluation, activities], output=person_summaries)
    def summarize_people(
        self, person: Person, evaluation: SecurityEvaluation, activity: RemediationWorkflowActivity
    ) -> PersonRemediationWorkflowSummary:
        cross_join(evaluation, allow_cartesian=True)
        left_join(activity, on=(activity.person_id == person.id) & (activity.as_of_date == evaluation.as_of_date))
        group_by(person_id=person.id, person_name=person.name, as_of_date=evaluation.as_of_date)
        return PersonRemediationWorkflowSummary(
            person_id=person.id, person_name=person.name, **self._totals(activity, evaluation)
        )

    @step(input=[teams, evaluation, activities], output=team_summaries)
    def summarize_teams(
        self, team: Team, evaluation: SecurityEvaluation, activity: RemediationWorkflowActivity
    ) -> TeamRemediationWorkflowSummary:
        cross_join(evaluation, allow_cartesian=True)
        left_join(activity, on=(activity.team_id == team.id) & (activity.as_of_date == evaluation.as_of_date))
        group_by(team_id=team.id, team_name=team.name, as_of_date=evaluation.as_of_date)
        return TeamRemediationWorkflowSummary(
            team_id=team.id, team_name=team.name, **self._totals(activity, evaluation)
        )

    @step(input=[departments, evaluation, activities], output=department_summaries)
    def summarize_departments(
        self, department: Department, evaluation: SecurityEvaluation, activity: RemediationWorkflowActivity
    ) -> DepartmentRemediationWorkflowSummary:
        cross_join(evaluation, allow_cartesian=True)
        left_join(
            activity,
            on=(activity.department_id == department.id) & (activity.as_of_date == evaluation.as_of_date),
        )
        group_by(department_id=department.id, department_name=department.name, as_of_date=evaluation.as_of_date)
        return DepartmentRemediationWorkflowSummary(
            department_id=department.id,
            department_name=department.name,
            **self._totals(activity, evaluation),
        )

    @step(input=[orgs, evaluation, activities], output=org_summaries)
    def summarize_orgs(
        self, org: Org, evaluation: SecurityEvaluation, activity: RemediationWorkflowActivity
    ) -> OrgRemediationWorkflowSummary:
        cross_join(evaluation, allow_cartesian=True)
        left_join(activity, on=(activity.org_id == org.id) & (activity.as_of_date == evaluation.as_of_date))
        group_by(org_id=org.id, org_name=org.name, as_of_date=evaluation.as_of_date)
        return OrgRemediationWorkflowSummary(org_id=org.id, org_name=org.name, **self._totals(activity, evaluation))

    @staticmethod
    def _totals(activity: RemediationWorkflowActivity, evaluation: SecurityEvaluation) -> dict[str, object]:
        return {
            "as_of_date": evaluation.as_of_date,
            "unacknowledged_count": sum(coalesce(activity.unacknowledged_count, 0)),
            "pending_exception_count": sum(coalesce(activity.pending_exception_count, 0)),
            "expiring_exception_count": sum(coalesce(activity.expiring_exception_count, 0)),
            "expired_exception_count": sum(coalesce(activity.expired_exception_count, 0)),
        }
