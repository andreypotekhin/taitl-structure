from structure import Schema
from structure.plugin.pyspark import *


class Org(Schema):
    id = string(nullable=False)
    name = string(nullable=False)


class Department(Schema):
    id = string(nullable=False)
    org_id = string(nullable=False)
    name = string(nullable=False)


class Team(Schema):
    id = string(nullable=False)
    department_id = string(nullable=False)
    name = string(nullable=False)


class Person(Schema):
    id = string(nullable=False)
    team_id = string(nullable=False)
    name = string(nullable=False)
