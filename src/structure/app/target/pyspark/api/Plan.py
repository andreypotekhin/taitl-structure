from structure.app.target.pyspark.commands.LowerPySparkPlan import LowerPySparkPlan


class Plan:

    @staticmethod
    def lower() -> LowerPySparkPlan:
        return LowerPySparkPlan()
