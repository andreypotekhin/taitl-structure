import json

from structure.plugin.api.v1 import SerializationAPI as SerializationAPIV1


class Serialization(SerializationAPIV1):
    """Encodes only the plugin-owned payload; Core owns the artifact envelope."""

    def encode(self, payload: object) -> bytes:
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()

    def decode(self, payload: bytes) -> object:
        return json.loads(payload.decode())
