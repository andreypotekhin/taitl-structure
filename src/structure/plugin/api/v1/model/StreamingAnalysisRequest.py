from dataclasses import dataclass


@dataclass(frozen=True)
class StreamingAnalysisRequest:
    payload: object
    required: bool
    streaming_contract: bool = False
