import re


class PySparkNameMapper:

    def alias(self, name: str) -> str:
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

    def join_alias(self, name: str, occurrence: int) -> str:
        if occurrence == 1:
            return name
        return f"{name}_{occurrence}"
