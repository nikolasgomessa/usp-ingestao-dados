import pandas as pd
import glob
import os
import logging
from load import LoadData
from write import DataWriter
from transform import BanksTransformation, EmployeesTransformation, ComplaintsTransformation

pd.options.display.float_format = '{:.2f}'.format  # Sets the display format for decimal data in pandas dataframes


def main():
    """
    Initiates the main execution where a logger is initialized, the current directory is captured, and various tables are loaded,
    transformed, concatenated into a single dataframe via join, and finally saved in a directory in the 'parquet' fixed format.
    """
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    current_directory = os.getcwd()

    load_banks = LoadData(
        file_directories=[os.path.join(current_directory, 'data/raw/Bancos/EnquadramentoInicia_v2.tsv')], 
        file_type='tsv',
        separator='\t'
    )
    df_raw_banks = load_banks.load()

    load_employees = LoadData(
        file_directories=glob.glob(os.path.join(current_directory, 'data/raw/Empregados', "*.csv")), 
        file_type='csv',
        separator='|'
    )
    df_raw_employees = load_employees.load()

    load_complaints = LoadData(
        file_directories=glob.glob(os.path.join(current_directory, 'data/raw/Reclamações', "*.csv")), 
        file_type='csv',
        separator=';'
    )
    df_raw_complaints = load_complaints.load()

    transform_banks = BanksTransformation(df_raw_banks)
    df_banks = transform_banks.transform()

    transform_employees = EmployeesTransformation(df_raw_employees)
    df_employees = transform_employees.transform()
    df_grouped_employees = transform_employees.calculate_aggregates(df_employees)

    transform_complaints = ComplaintsTransformation(df_raw_complaints)
    df_complaints = transform_complaints.transform()
    df_grouped_complaints = transform_complaints.calculate_aggregates(df_complaints)

    df_complaints_banks = df_banks.merge(df_grouped_complaints, on='nome', how='inner')
    logging.info(f'Count Reclamações x Bancos: \n{df_complaints_banks.count()}')

    df_complaints_banks_employees = df_complaints_banks.merge(df_grouped_employees, on='nome', how='inner')
    logging.info(f'Count Reclamações x Bancos x Empregados: \n{df_complaints_banks_employees.count()}')

    output_directory = os.path.join(current_directory, 'data/transformed')
    file_name = 'atividade03.parquet'
    save_data = DataWriter()
    save_data.write_parquet_data(df_complaints_banks_employees, output_directory, file_name)


if __name__ == "__main__":
    main()
