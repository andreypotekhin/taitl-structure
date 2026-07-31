"""Example of a non-PySpark transform. Powered by Iterable plugin."""

from structure_iterable import left_join, scan, state

from examples.school.schemas.iterable import (
    IterableFibonacciRow,
    SequenceRow,
    Student,
    StudentAudit,
    StudentAward,
    StudentProfile,
    StudentReport,
)
from structure import Transform, input, output, transform


@transform(target="iterable")
class ProjectIterableScores(Transform):
    """Joins two lookup relations and emits report and audit outputs in one step."""

    students = input(Student)
    profiles = input(StudentProfile)
    awards = input(StudentAward)
    reports = output(StudentReport)
    audits = output(StudentAudit)

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


@transform(target="iterable")
class IterableFibonacci(Transform):
    """Emits Fibonacci values with the finite Iterable plugin."""

    rows = input(SequenceRow)
    result = output(IterableFibonacciRow)

    def generate(self, row: SequenceRow) -> IterableFibonacciRow:  # type: ignore[override]
        return scan(
            initial=(0, 1),
            output=IterableFibonacciRow(index=row.index, fibonacci=state[0]),
            next=lambda previous, current: (current, previous + current),
        )
