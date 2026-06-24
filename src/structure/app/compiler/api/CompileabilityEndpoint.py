from structure.app.compiler.compileability.streaming_compatibility.api import ClassifyStreamingCompatibility


class CompileabilityEndpoint:

    def streaming(self) -> ClassifyStreamingCompatibility:
        return ClassifyStreamingCompatibility()
