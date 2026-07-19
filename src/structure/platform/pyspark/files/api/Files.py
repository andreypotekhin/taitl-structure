from structure.platform.pyspark.files.commands.CompareGeneratedFiles import CompareGeneratedFiles
from structure.platform.pyspark.files.commands.WriteGeneratedFiles import WriteGeneratedFiles


class Files:

    def compare(self) -> CompareGeneratedFiles:
        return CompareGeneratedFiles()

    def write(self) -> WriteGeneratedFiles:
        return WriteGeneratedFiles()
