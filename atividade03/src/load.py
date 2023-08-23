import pandas as pd

class UnsupportedFileType(Exception):
    def __init__(self, file_type):
        self.file_type = file_type
        self.message = f"File(s) of type {file_type} not supported"
        super().__init__(self.message)

class LoadData:
    def __init__(self, file_directories: list, file_type: str, separator: str = ';'):
        self.file_directories = file_directories
        self.file_type = file_type
        self.separator = separator

    def load(self) -> pd.DataFrame:
        dfs = []
        for file_directory in self.file_directories:
            if self.file_type in ('csv', 'tsv'):
                df = pd.read_csv(file_directory, sep=self.separator)
            else:
                raise UnsupportedFileType(self.file_type)
            dfs.append(df)
        return pd.concat(dfs, axis=0, ignore_index=True)