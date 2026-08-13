"""Application-scoped Search inference adapter registry."""

from __future__ import annotations

from examples.search.inference.DefaultInferenceAdapter import DefaultInferenceAdapter
from examples.search.inference.InferenceAdapter import InferenceAdapter


class InferenceAdapterRegistry:
    """Resolve configured adapters while keeping calling transforms adapter-free."""

    _adapters: dict[str, InferenceAdapter] = {"default": DefaultInferenceAdapter()}

    @classmethod
    def register(cls, adapter: InferenceAdapter) -> None:
        cls._adapters[adapter.provider_id] = adapter

    @classmethod
    def resolve(cls, provider_id: str) -> InferenceAdapter:
        try:
            return cls._adapters[provider_id]
        except KeyError as error:
            raise ValueError(f"No Search inference adapter is registered for provider {provider_id!r}") from error

    @classmethod
    def default(cls) -> InferenceAdapter:
        return cls._adapters["default"]
