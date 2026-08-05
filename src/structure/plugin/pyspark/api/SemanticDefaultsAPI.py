from collections.abc import Mapping

from structure.plugin.api.v1 import SemanticDefaultsAPI as SemanticDefaultsAPIV1


class SemanticDefaultsAPI(SemanticDefaultsAPIV1):
    def resolve(self, *, options: Mapping[str, object]) -> Mapping[str, object]:
        return {
            "allow_output_to_input": True,
            "allow_to_reassign_output": True,
        }
