from examples.store.schemas.experiment import RecommendationExperiment
from structure import Transform, input, output
from structure.plugin.pyspark import where


class SelectActiveRecommendationExperiments(Transform):
    experiments = input(RecommendationExperiment)
    active_experiments = output(RecommendationExperiment)

    def select(self, experiment: RecommendationExperiment) -> RecommendationExperiment:
        where(experiment.active)
        return RecommendationExperiment.project(experiment)
