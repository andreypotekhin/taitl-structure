from structure.app.target.pyspark.logic.actions.LowerPySparkPlan import LowerPySparkPlan


class PlanEndpoint:

    def lower(self) -> LowerPySparkPlan:
        return LowerPySparkPlan()
