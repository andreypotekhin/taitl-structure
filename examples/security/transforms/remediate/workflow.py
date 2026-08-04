from examples.security.schemas.organization import Department, Org, Person, Team
from examples.security.schemas.remediation import (
    DepartmentRemediationWorkflowSummary,
    ExpiredExceptionVulnerability,
    ExpiringExceptionVulnerability,
    OrgRemediationWorkflowSummary,
    PendingExceptionVulnerability,
    PersonRemediationWorkflowSummary,
    RemediationCase,
    RemediationCaseCheck,
    RemediationCaseIssue,
    TeamRemediationWorkflowSummary,
    UnacknowledgedVulnerability,
    VulnerabilityWorkflowExposure,
)
from examples.security.schemas.reporting import SecurityEvaluation, VulnerabilityExposure
from examples.security.schemas.risk import Vuln
from examples.security.transforms.remediate.check import VulnerabilityRemediationPrepare
from examples.security.transforms.remediate.enrich import VulnerabilityRemediationAccess
from examples.security.transforms.remediate.publish import VulnerabilityRemediationPublish
from examples.security.transforms.remediate.summarize import VulnerabilityRemediationSummaries
from structure import Transform, input, output, stage


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

    prepared = VulnerabilityRemediationPrepare(
        cases=cases, vulnerabilities=vulnerabilities
    )

    accessed = VulnerabilityRemediationAccess(
        exposures=exposures,
        case_checks=prepared.case_checks,
        evaluation=evaluation,
    )

    published = VulnerabilityRemediationPublish(
        workflow_exposures=accessed.workflow_exposures,
        evaluation=evaluation,
    )

    summarized = VulnerabilityRemediationSummaries(
        workflow_exposures=accessed.workflow_exposures,
        unacknowledged=published.unacknowledged,
        pending_exceptions=published.pending_exceptions,
        expiring_exceptions=published.expiring_exceptions,
        expired_exceptions=published.expired_exceptions,
        people=people,
        teams=teams,
        departments=departments,
        orgs=orgs,
        evaluation=evaluation,
    )
