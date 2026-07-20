from structure.core.compiler.compileability.streaming_compatibility.commands.ClassifyStreamingCompatibility import (
    ClassifyStreamingCompatibility,
)


class StreamingCompatibility:
    def classify(self) -> ClassifyStreamingCompatibility:
        return ClassifyStreamingCompatibility()
