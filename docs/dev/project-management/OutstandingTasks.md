# Outstanding

## v1

CI workflow evidence
Tracked in [Milestones.md (line 5)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/Milestones.md:5) and [Sprint00_Groundwork.md (line 135)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/sprints/Sprint00_Groundwork.md:135). This is mainly proof that lint/type/test/build run in CI before marking M0/Sprint 00 done.

Live PySpark integration matrix
Tracked in [Milestones.md (line 36)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/Milestones.md:36), [Sprint05_JoinsTraceabilityBuildIntegration.md (line 131)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/sprints/Sprint05_JoinsTraceabilityBuildIntegration.md:131), and [P06212604.Local-infrastructure-integration-testing.plan.md (line 42)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/planning/P06212604.Local-infrastructure-integration-testing.plan.md:42). The code path exists; final pass needs at least one real Spark/PySpark lane, ideally PySpark 3.5 and 4.0.

Broader negative schema-validation coverage against Spark DataFrames
Tracked in [Milestones.md (line 58)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/Milestones.md:58) and [Sprint02_SchemasAndValidation.md (line 102)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/sprints/Sprint02_SchemasAndValidation.md:102). This means live invalid DataFrame cases: missing columns, extra columns, type mismatch, nested mismatch, nullable/strict mode behavior.

Generated-code version headers
Still promised by compatibility policy in [Compatibility.md (line 90)](C:/Taitl/Code/taitl/taitl-structure/docs/Compatibility.md:90) and M6 in [Milestones.md (line 121)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/Milestones.md:121). Final pass should add or verify generator version and target PySpark range comments in generated artifacts.

Setup/configuration doctor checks
Still listed as v1 complete-phase work in [Implementation.md (line 54)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/Implementation.md:54) and as remaining M6 work in [Milestones.md (line 121)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/Milestones.md:121). This likely wants a lightweight structure doctor or equivalent check path for common adoption failures.

Multi-version PySpark evidence
Also part of M6 in [Milestones.md (line 129)](C:/Taitl/Code/taitl/taitl-structure/docs/dev/project-management/Milestones.md:129). This overlaps with the integration matrix but should produce release-ready evidence for the documented >=3.5,<4.1 target.

