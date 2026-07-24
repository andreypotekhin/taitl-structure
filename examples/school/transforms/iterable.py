"""Finite-row transform supplied by the external iterable example plugin."""

from structure_iterable import projection

from examples.school.schemas.sequences import Student
from structure import Transform, input, transform


@transform(target="iterable")
class ProjectIterableScores(Transform):
    """Projects caller-supplied finite student-score rows through the Iterable plugin."""

    students = input(Student)

    operation = projection(fields={"student": "student", "score": "score"}, input="students")
