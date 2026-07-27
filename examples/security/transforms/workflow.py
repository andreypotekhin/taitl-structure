from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.reporting import (
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
    SecurityEvaluation,
    TeamRemediationWorkflowSummary,
    UnacknowledgedVulnerability,
    VulnerabilityExposure,
    VulnerabilityWorkflowExposure,
)
from examples.security.schemas.risk import Vuln
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *


class VulnerabilityRemediationWorkflow(Transform):
    """Reconcile caller-owned remediation cases with current security exposures."""

    exposures = input(VulnerabilityExposure)
    vulnerabilities = input(Vuln)
    cases = input(RemediationCase)
    people = input(Person)
    teams = input(Team)
    departments = input(Department)
    orgs = input(Org)
    evaluation = input(SecurityEvaluation)
    case_aggregates = lane(RemediationCaseAggregate)
    case_lane = lane(RemediationCaseCheck)
    workflow_lane = lane(VulnerabilityWorkflowExposure)
    activities = lane(RemediationWorkflowActivity)
    case_checks = output(RemediationCaseCheck)
    case_issues = output(RemediationCaseIssue)
    workflow_exposures = output(VulnerabilityWorkflowExposure)
    unacknowledged = output(UnacknowledgedVulnerability)
    pending_exceptions = output(PendingExceptionVulnerability)
    expiring_exceptions = output(ExpiringExceptionVulnerability)
    expired_exceptions = output(ExpiredExceptionVulnerability)
    person_summaries = output(PersonRemediationWorkflowSummary)
    team_summaries = output(TeamRemediationWorkflowSummary)
    department_summaries = output(DepartmentRemediationWorkflowSummary)
    org_summaries = output(OrgRemediationWorkflowSummary)

    @step(input=[cases, vulnerabilities], output=case_aggregates)
    def aggregate_cases(self, case: RemediationCase, vuln: Vuln) -> RemediationCaseAggregate:
        left_join(vuln, on=vuln.id == case.vuln_id)
        group_by(vuln_id=case.vuln_id)
        acknowledged_at = min(case.acknowledged_at)
        requested_at = min(case.exception_requested_at)
        reason = min(case.exception_reason)
        approver = min(case.exception_approver)
        approved_at = min(case.exception_approved_at)
        expires_on = min(case.exception_expires_on)
        exists = bool_or(vuln.id.is_not_null())
        return RemediationCaseAggregate(
            vuln_id=case.vuln_id,
            acknowledged_at=acknowledged_at,
            exception_requested_at=requested_at,
            exception_reason=reason,
            exception_approver=approver,
            exception_approved_at=approved_at,
            exception_expires_on=expires_on,
            case_count=count(),
            vulnerability_exists=exists,
        )

    @step(input=case_aggregates, output=case_lane)
    def check_cases(self, case: RemediationCaseAggregate) -> RemediationCaseCheck:
        issues = arr_compact(
            array(
                when(case.case_count > 1, "duplicate current cases").otherwise(None),
                when(~case.vulnerability_exists, "unknown vulnerability").otherwise(None),
                when(
                    case.exception_approved_at.is_not_null() & case.exception_requested_at.is_null(),
                    "approved exception has no request",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null()
                    & case.exception_requested_at.is_not_null()
                    & (case.exception_approved_at < case.exception_requested_at),
                    "exception approval precedes request",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null() & case.exception_reason.is_null(),
                    "approved exception has no reason",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null() & case.exception_approver.is_null(),
                    "approved exception has no approver",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null() & case.exception_expires_on.is_null(),
                    "approved exception has no expiry",
                ).otherwise(None),
                when(
                    case.exception_approved_at.is_not_null()
                    & case.exception_expires_on.is_not_null()
                    & (case.exception_expires_on < to_date(case.exception_approved_at)),
                    "exception expires before approval",
                ).otherwise(None),
            )
        )
        return RemediationCaseCheck.base(case)(issues=issues, is_valid=size(issues) == 0)

    @step(input=case_lane, output=case_checks)
    def publish_case_checks(self, check: RemediationCaseCheck) -> RemediationCaseCheck:
        return RemediationCaseCheck.project(check)

    @step(input=case_lane, output=case_issues)
    def publish_case_issues(self, check: RemediationCaseCheck) -> RemediationCaseIssue:
        where(~check.is_valid)
        return RemediationCaseIssue.base(check)

    @step(input=[exposures, case_lane, evaluation], output=workflow_lane)
    def enrich_exposure(
        self, finding: VulnerabilityExposure, check: RemediationCaseCheck, evaluation: SecurityEvaluation
    ) -> VulnerabilityWorkflowExposure:
        left_join(check, on=(check.vuln_id == finding.vuln_id) & check.is_valid)
        cross_join(evaluation, allow_cartesian=True)
        approved = check.exception_approved_at.is_not_null() & check.exception_expires_on.is_not_null()
        return VulnerabilityWorkflowExposure.project(finding)(
            acknowledged_at=check.acknowledged_at,
            exception_requested_at=check.exception_requested_at,
            exception_reason=check.exception_reason,
            exception_approver=check.exception_approver,
            exception_approved_at=check.exception_approved_at,
            exception_expires_on=check.exception_expires_on,
            is_acknowledged=check.acknowledged_at.is_not_null(),
            is_deadline_paused=coalesce(approved & (check.exception_expires_on >= evaluation.as_of_date), False),
        )

    @step(input=workflow_lane, output=workflow_exposures)
    def publish_workflow_exposures(self, finding: VulnerabilityWorkflowExposure) -> VulnerabilityWorkflowExposure:
        return VulnerabilityWorkflowExposure.project(finding)

    @step(input=workflow_lane, output=unacknowledged)
    def publish_unacknowledged(self, finding: VulnerabilityWorkflowExposure) -> UnacknowledgedVulnerability:
        where(finding.is_active & ~finding.is_acknowledged)
        return UnacknowledgedVulnerability.base(finding)

    @step(input=workflow_lane, output=pending_exceptions)
    def publish_pending(self, finding: VulnerabilityWorkflowExposure) -> PendingExceptionVulnerability:
        where(
            finding.is_active & finding.exception_requested_at.is_not_null() & finding.exception_approved_at.is_null()
        )
        return PendingExceptionVulnerability.base(finding)

    @step(input=[workflow_lane, evaluation], output=expiring_exceptions)
    def publish_expiring(
        self, finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation
    ) -> ExpiringExceptionVulnerability:
        cross_join(evaluation, allow_cartesian=True)
        where(
            finding.is_active
            & finding.exception_approved_at.is_not_null()
            & finding.exception_expires_on.is_not_null()
            & (finding.exception_expires_on >= evaluation.as_of_date)
            & (finding.exception_expires_on <= date_add(evaluation.as_of_date, days=7))
        )
        return ExpiringExceptionVulnerability.base(finding)

    @step(input=[workflow_lane, evaluation], output=expired_exceptions)
    def publish_expired(
        self, finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation
    ) -> ExpiredExceptionVulnerability:
        cross_join(evaluation, allow_cartesian=True)
        where(
            finding.is_active
            & finding.exception_approved_at.is_not_null()
            & finding.exception_expires_on.is_not_null()
            & (finding.exception_expires_on < evaluation.as_of_date)
        )
        return ExpiredExceptionVulnerability.base(finding)

    @step(input=[workflow_lane, evaluation], output=activities)
    def assess(
        self, finding: VulnerabilityWorkflowExposure, evaluation: SecurityEvaluation
    ) -> RemediationWorkflowActivity:
        cross_join(evaluation, allow_cartesian=True)
        return RemediationWorkflowActivity(
            person_id=finding.person_id,
            team_id=finding.team_id,
            department_id=finding.department_id,
            org_id=finding.org_id,
            as_of_date=evaluation.as_of_date,
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
