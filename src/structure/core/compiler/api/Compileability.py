from structure.core.compiler.compileability.streaming_compatibility.api import (
    ClassifyStreamingCompatibility,
    StreamingCompatibility,
)


class Compileability:
    streaming_compatibility = StreamingCompatibility()

    def streaming(self) -> ClassifyStreamingCompatibility:
        return self.streaming_compatibility.classify()
