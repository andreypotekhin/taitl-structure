"""Example of a non-PySpark transform. Powered by Iterable plugin."""

from structure_iterable import left_join

from examples.school.schemas.sequences import Student, StudentAudit, StudentAward, StudentProfile, StudentReport
from structure import Transform, input, output, step, transform


@transform(target="iterable")
class ProjectIterableScores(Transform):
    """Joins two lookup relations and emits report and audit outputs in one step."""

    students = input(Student)
    profiles = input(StudentProfile)
    awards = input(StudentAward)
    reports = output(StudentReport)
    audits = output(StudentAudit)

    @step(input=[students, profiles, awards], output=[reports, audits])
    def project_scores(
        self,
        student: Student,
        profile: StudentProfile,
        award: StudentAward,
    ) -> tuple[StudentReport, StudentAudit]:
        left_join(profile, on=profile.student == student.student)
        left_join(award, on=award.student == student.student)
        return (
            StudentReport(student=student.student, score=student.score, cohort=profile.cohort, award=award.award),
            StudentAudit(student=student.student, score=student.score),
        )
