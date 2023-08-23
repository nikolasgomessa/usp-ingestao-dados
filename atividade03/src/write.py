import os

class DataWriter:
    def write_parquet_data(self, df, output_directory, file_name):
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        df.to_parquet(os.path.join(output_directory, file_name), engine='fastparquet')
