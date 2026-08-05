"""Caller-owned SearchDocuments streaming adoption contracts."""

from dataclasses import dataclass
from typing import Literal

SearchOutputMode = Literal["append"]
SearchEventTimeField = Literal["requested_at"]
SearchFinalityPolicy = Literal["append_final_no_revisions"]
SearchRefreshRestartPolicy = Literal["new_run_on_snapshot_refresh"]
SearchTopKStage = Literal["candidate_admission", "overlap_narrowing"]
SearchTopKTiePolicy = Literal["score_desc_document_id_asc"]
SearchTopKRestartPolicy = Literal["same_checkpoint_same_snapshot"]

REQUIRED_SNAPSHOT_INPUTS = (
    "index",
    "score_cache",
    "feedback",
    "popularity",
    "policy",
)


@dataclass(frozen=True)
class SearchDocumentsRunContract:
    """Caller-owned run metadata for the design-gated SearchDocuments stream."""

    snapshot_id: str
    snapshot_inputs: tuple[str, ...]
    sink_identity: str
    checkpoint_identity: str
    trigger: str
    output_mode: SearchOutputMode
    event_time_field: SearchEventTimeField
    completion_window: str
    refresh_restart_policy: SearchRefreshRestartPolicy
    finality_policy: SearchFinalityPolicy
    downstream_materialization: str

    def validate(self) -> None:
        """Reject a run handoff that cannot be reviewed as one immutable serving run."""

        declarations = (
            ("snapshot_id", self.snapshot_id),
            ("sink_identity", self.sink_identity),
            ("checkpoint_identity", self.checkpoint_identity),
            ("trigger", self.trigger),
            ("completion_window", self.completion_window),
            ("downstream_materialization", self.downstream_materialization),
        )
        for name, value in declarations:
            if not value.strip():
                raise ValueError(
                    f"SEARCH-RUN-E1001: {name} must be a non-empty stable declaration; "
                    "see docs/api/Streaming.api.md#searchdocuments-caller-owned-run-handoff"
                )

        duplicates = tuple(
            name for index, name in enumerate(self.snapshot_inputs) if name in self.snapshot_inputs[:index]
        )
        if set(self.snapshot_inputs) != set(REQUIRED_SNAPSHOT_INPUTS) or duplicates:
            missing = tuple(name for name in REQUIRED_SNAPSHOT_INPUTS if name not in self.snapshot_inputs)
            extra = tuple(name for name in self.snapshot_inputs if name not in REQUIRED_SNAPSHOT_INPUTS)
            details = []
            if missing:
                details.append(f"missing {', '.join(missing)}")
            if extra:
                details.append(f"unexpected {', '.join(extra)}")
            if duplicates:
                details.append(f"duplicated {', '.join(duplicates)}")
            raise ValueError(
                "SEARCH-RUN-E1001: snapshot_inputs must bind index, score_cache, feedback, popularity, and policy; "
                f"{'; '.join(details)}; see docs/api/Streaming.api.md#searchdocuments-caller-owned-run-handoff"
            )

        if self.output_mode != "append" or self.event_time_field != "requested_at":
            raise ValueError(
                "SEARCH-RUN-E1002: SearchDocuments requires append output and requested_at event time; "
                "see docs/api/Streaming.api.md#searchdocuments-caller-owned-run-handoff"
            )
        if self.refresh_restart_policy != "new_run_on_snapshot_refresh":
            raise ValueError(
                "SEARCH-RUN-E1003: snapshot refresh must start a new caller-owned run; "
                "see docs/api/Streaming.api.md#searchdocuments-caller-owned-run-handoff"
            )
        if self.finality_policy != "append_final_no_revisions":
            raise ValueError(
                "SEARCH-RUN-E1003: emitted SearchDocuments results must be final and never revised; "
                "see docs/api/Streaming.api.md#searchdocuments-caller-owned-run-handoff"
            )


@dataclass(frozen=True)
class SearchFiniteTopKContract:
    """Metadata for one bounded SearchDocuments candidate-selection stage."""

    stage: SearchTopKStage
    retained_bound: int
    grouping_key: tuple[str, ...]
    order_keys: tuple[str, ...]
    tie_policy: SearchTopKTiePolicy
    event_time_field: SearchEventTimeField
    watermark_delay: str
    completion_window: str
    output_mode: SearchOutputMode
    snapshot_id: str
    state_identity: str
    restart_policy: SearchTopKRestartPolicy

    def validate(self) -> None:
        """Reject a top-K shape that is unbounded or can revise its tie order."""

        declarations = (
            ("watermark_delay", self.watermark_delay),
            ("completion_window", self.completion_window),
            ("snapshot_id", self.snapshot_id),
            ("state_identity", self.state_identity),
        )
        for name, value in declarations:
            if not value.strip():
                raise ValueError(
                    f"SEARCH-TOPK-E1010: {name} must be a non-empty stable declaration; "
                    "see docs/api/Streaming.api.md#searchdocuments-finite-window-top-k-contract"
                )

        if not self.grouping_key or any(not key.strip() for key in self.grouping_key):
            raise ValueError(
                "SEARCH-TOPK-E1011: grouping_key must name a finite query grouping field; "
                "see docs/api/Streaming.api.md#searchdocuments-finite-window-top-k-contract"
            )
        if self.order_keys != ("score desc", "document_id asc"):
            raise ValueError(
                "SEARCH-TOPK-E1011: order_keys must be score desc followed by document_id asc for deterministic ties; "
                "see docs/api/Streaming.api.md#searchdocuments-finite-window-top-k-contract"
            )

        expected_bound = {"candidate_admission": 1000, "overlap_narrowing": 100}.get(self.stage)
        if expected_bound is None or self.retained_bound != expected_bound:
            raise ValueError(
                "SEARCH-TOPK-E1011: candidate_admission retains 1000 rows and overlap_narrowing retains 100 rows; "
                "see docs/api/Streaming.api.md#searchdocuments-finite-window-top-k-contract"
            )
        if self.tie_policy != "score_desc_document_id_asc":
            raise ValueError(
                "SEARCH-TOPK-E1012: top-K tie policy must be score_desc_document_id_asc; "
                "see docs/api/Streaming.api.md#searchdocuments-finite-window-top-k-contract"
            )
        if self.event_time_field != "requested_at" or self.output_mode != "append":
            raise ValueError(
                "SEARCH-TOPK-E1012: top-K requires requested_at event time and append output; "
                "see docs/api/Streaming.api.md#searchdocuments-finite-window-top-k-contract"
            )
        if self.restart_policy != "same_checkpoint_same_snapshot":
            raise ValueError(
                "SEARCH-TOPK-E1012: restart must reuse the checkpoint only with the same snapshot; "
                "see docs/api/Streaming.api.md#searchdocuments-finite-window-top-k-contract"
            )
