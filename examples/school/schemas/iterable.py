"""School schemas defined by the external Iterable plugin."""

from structure_iterable import field

from structure import Schema


class Student(Schema):
    """One student score supplied to the Iterable projection example."""

    student: str = field(nullable=False, description="Student identifier")
    score: int = field(nullable=False, description="Recorded score")


class StudentProfile(Schema):
    """A finite lookup relation carrying a student's cohort."""

    student: str = field(nullable=False)
    cohort: str = field(nullable=False, description="Student cohort")


class StudentAward(Schema):
    """A finite lookup relation carrying a student's award label."""

    student: str = field(nullable=False)
    award: str = field(nullable=False, description="Award label")


class StudentReport(Schema):
    """A score enriched from two Iterable lookup relations."""

    student: str = field(nullable=False, alias="student_name")
    score: int = field(nullable=False, alias="score_points")
    cohort: str = field(alias="cohort_name", description="Lookup cohort; absent when no profile matches")
    award: str = field(alias="award_name", description="Lookup award; absent when no award matches")


class StudentAudit(Schema):
    """A second output proving one Iterable step may publish several relations."""

    student: str = field(nullable=False)
    score: int = field(nullable=False)


class SequenceRow(Schema):
    """One caller-provided position in a finite sequence."""

    index: int = field(nullable=False)


class IterableFibonacciRow(SequenceRow):
    """A sequence position with its Iterable-generated Fibonacci value."""

    fibonacci: int = field(nullable=False, alias="fibonacci_value")
