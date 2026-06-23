from structure.app.target.pyspark.logic.actions.CompareGeneratedFiles import CompareGeneratedFiles
from structure.app.target.pyspark.logic.actions.WriteGeneratedFiles import WriteGeneratedFiles


class FilesEndpoint:

    def compare(self) -> CompareGeneratedFiles:
        return CompareGeneratedFiles()

    def write(self) -> WriteGeneratedFiles:
        return WriteGeneratedFiles()
