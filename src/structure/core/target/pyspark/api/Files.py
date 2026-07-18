from structure.core.target.pyspark.commands.CompareGeneratedFiles import CompareGeneratedFiles
from structure.core.target.pyspark.commands.WriteGeneratedFiles import WriteGeneratedFiles


class Files:

    @staticmethod
    def compare() -> CompareGeneratedFiles:
        return CompareGeneratedFiles()

    @staticmethod
    def write() -> WriteGeneratedFiles:
        return WriteGeneratedFiles()
