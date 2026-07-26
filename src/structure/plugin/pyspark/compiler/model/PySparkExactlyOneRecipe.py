from dataclasses import dataclass


@dataclass(frozen=True)
class PySparkExactlyOneRecipe:
    scope: str
