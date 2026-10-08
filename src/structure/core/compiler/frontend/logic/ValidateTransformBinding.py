from __future__ import annotations


def binding_provider(binding: str) -> str | None:
    if binding.startswith("delta"):
        return "delta"
    if binding.startswith("iceberg"):
        return "iceberg"
    return None


def compatible_bindings(producer: str, consumer: str) -> bool:
    """Return whether a composed output can satisfy a declared input role."""
    return binding_provider(producer) == binding_provider(consumer)


def input_role(binding: str) -> str:
    """Normalize a provider input declaration to its stable input role."""
    provider = binding_provider(binding)
    return "dataframe" if provider is None else f"{provider}_input"


def declaration_role(binding: str, *, output: bool) -> str:
    provider = binding_provider(binding)
    if provider is None:
        return "dataframe"
    if binding.endswith("_table"):
        return f"{provider}_table"
    return f"{provider}_{'output' if output else 'input'}"
