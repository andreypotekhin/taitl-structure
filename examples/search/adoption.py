"""Caller-owned SearchDocuments streaming adoption contracts."""

from dataclasses import dataclass
from typing import Literal

SearchOutputMode = Literal["append"]
SearchEventTimeField = Literal["requested_at"]
SearchFinalityPolicy = Literal["append_final_no_revisions"]
SearchRefreshRestartPolicy = Literal["new_run_on_snapshot_refresh"]

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
