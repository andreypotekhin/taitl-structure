"""Experiment scoring001: slightly stronger BM25 term-frequency saturation."""

from examples.search.transforms.scoring.ScoreBm25 import ScoreBm25
from examples.search.transforms.scoring.Scoring import Scoring
from structure import stage


class Scoring001AdjustBm(Scoring):
    """Score with adjusted BM25 parameters under the ``Scoring001AdjustBm`` identity."""

    experiment_id = "Scoring001AdjustBm"

    bm25 = stage(
        ScoreBm25(
            queries=Scoring.queries,
            document_terms=Scoring.document_terms,
            section_terms=Scoring.section_terms,
            paragraph_terms=Scoring.paragraph_terms,
            sentence_terms=Scoring.sentence_terms,
            document_summary=Scoring.document_summary,
            section_summary=Scoring.section_summary,
            paragraph_summary=Scoring.paragraph_summary,
            sentence_summary=Scoring.sentence_summary,
            k1=1.35,
            b=0.70,
        )
    )
