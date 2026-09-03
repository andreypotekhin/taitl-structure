from app.math.model import Metric
from structure import Transform, input, output


class ScoreCorpus(Transform):
    metrics = input(Metric)
    scores = output(Metric)

    def score(self, metric: Metric) -> Metric:
        return Metric(id=metric.id)
