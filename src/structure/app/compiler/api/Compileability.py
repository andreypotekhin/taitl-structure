from structure.app.compiler.compileability.streaming_compatibility.api import ClassifyStreamingCompatibility


class Compileability:

    @staticmethod
    def streaming() -> ClassifyStreamingCompatibility:
        return ClassifyStreamingCompatibility()
