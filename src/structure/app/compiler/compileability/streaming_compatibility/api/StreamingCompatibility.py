from structure.app.compiler.compileability.streaming_compatibility.commands.ClassifyStreamingCompatibility import (
    ClassifyStreamingCompatibility,
)


class StreamingCompatibility:

    @staticmethod
    def classify() -> ClassifyStreamingCompatibility:
        return ClassifyStreamingCompatibility()
