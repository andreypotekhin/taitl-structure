import json


class Serialization:
    """Encodes only the plugin-owned payload; Core owns the artifact envelope."""

    def encode(self, payload: object) -> bytes:
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()

    def decode(self, payload: bytes) -> object:
        return json.loads(payload.decode())
