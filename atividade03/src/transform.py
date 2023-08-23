from abc import ABC, abstractmethod
import numpy as np
import pandas as pd
from json_loader import JSONLoader
import os

class TransformData(ABC):
    """
    Gathers general functions for all transformations.
    """
    def format_cnpj(self, value):
        """
        Receives an int or float and returns an 8-character string filled with leading '0's.
        """
        if pd.notna(value) and isinstance(value, (int, float)):
            return f'{str(int(value)).zfill(8)}'
        return ' '    


    def load_column_rename_mappings(self, transformation_name):
        json_file_path = os.path.join(os.path.dirname(__file__), 'column_rename.json')
        column_rename_mappings = JSONLoader.load_json(json_file_path)
        return column_rename_mappings.get(transformation_name, {})


    @abstractmethod
    def transform(self) -> pd.DataFrame:
        pass


class BanksTransformation(TransformData):
    """
    Functions for transforming the pandas dataframe for banks.
    """
    def __init__(self, df: pd.DataFrame):
        """
        Receives the dataframe.
        """
        self.df = df
        self.column_rename = self.load_column_rename_mappings('BanksTransformation')


    def transform(self) -> pd.DataFrame:
        """
        Function to rename (to snake_case), format, and adjust data in the 'Segment', 'CNPJ', and 'Name' columns.
        Returns a dataframe.
        """
        transformed_df = self.df.rename(columns=self.column_rename)
        transformed_df['cnpj'] = transformed_df['cnpj'].apply(self.format_cnpj)
        transformed_df['nome'] = transformed_df['nome'].astype('string').str.replace(' - PRUDENCIAL', '')
        return transformed_df


class EmployeesTransformation(TransformData):
    """
    Functions for transforming the pandas dataframe for employees.
    """
    def __init__(self, df: pd.DataFrame):
        """
        Receives the dataframe.
        """
        self.df = df
        self.column_rename = self.load_column_rename_mappings('EmployeesTransformation')


    def transform(self) -> pd.DataFrame:
        """
        Function to rename (to snake_case), format, and change data types.
        Returns a dataframe.
        """
        transformed_df = self.df.rename(columns=self.column_rename)

        transformed_df['employer_name'] = transformed_df['employer_name'].astype('string')
        transformed_df['reviews_count'] = pd.to_numeric(transformed_df['reviews_count'], errors='coerce')
        transformed_df['culture_count'] = pd.to_numeric(transformed_df['culture_count'], errors='coerce')
        transformed_df['salaries_count'] = pd.to_numeric(transformed_df['salaries_count'], errors='coerce')
        transformed_df['benefits_count'] = pd.to_numeric(transformed_df['benefits_count'], errors='coerce')
        transformed_df['employer_website'] = transformed_df['employer_website'].astype('string')
        transformed_df['employer_headquarters'] = transformed_df['employer_headquarters'].astype('string')
        transformed_df['employer_founded'] = pd.to_numeric(transformed_df['employer_founded'], errors='coerce')
        transformed_df['employer_industry'] = transformed_df['employer_industry'].astype('string')
        transformed_df['employer_revenue'] = transformed_df['employer_revenue'].astype('string')
        transformed_df['url'] = transformed_df['url'].astype('string')
        transformed_df['geral'] = pd.to_numeric(transformed_df['geral'], errors='coerce')
        transformed_df['cultura_valores'] = pd.to_numeric(transformed_df['cultura_valores'], errors='coerce')
        transformed_df['diversidade_inclusao'] = pd.to_numeric(transformed_df['diversidade_inclusao'], errors='coerce')
        transformed_df['qualidade_vida'] = pd.to_numeric(transformed_df['qualidade_vida'], errors='coerce')
        transformed_df['alta_lideranca'] = pd.to_numeric(transformed_df['alta_lideranca'], errors='coerce')
        transformed_df['remuneracao_beneficios'] = pd.to_numeric(transformed_df['remuneracao_beneficios'], errors='coerce')
        transformed_df['oportunidades_carreira'] = pd.to_numeric(transformed_df['oportunidades_carreira'], errors='coerce')
        transformed_df['percentual_recomendam_para_outras_pessoas'] = pd.to_numeric(transformed_df['percentual_recomendam_para_outras_pessoas'], errors='coerce')
        transformed_df['percentual_perspectiva_positiva_empresa'] = pd.to_numeric(transformed_df['percentual_perspectiva_positiva_empresa'], errors='coerce')
        transformed_df['cnpj'] = transformed_df['cnpj'].apply(self.format_cnpj)
        transformed_df['nome'] = transformed_df['nome'].astype('string')
        transformed_df['segmento'] = transformed_df['segmento'].astype('string')
        transformed_df['match_percent'] = pd.to_numeric(transformed_df['match_percent'], errors='coerce')

        return transformed_df


    def calculate_aggregates(self, df) -> pd.DataFrame:
        """
        Function to return a pandas dataframe of a pivot table grouped by the 'name' column,
        aggregating the 'geral' and 'remuneracao_beneficios' columns by mean.
        """
        aggregated_df  = df.groupby('nome', as_index=False).agg({
            'geral': 'mean',
            'remuneracao_beneficios': 'mean'
        })
        return aggregated_df 


class ComplaintsTransformation(TransformData):
    """
    Functions for transforming the pandas dataframe for complaints.
    """
    def __init__(self, df: pd.DataFrame):
        """
        Receives the dataframe.
        """
        self.df = df
        self.column_rename = self.load_column_rename_mappings('ComplaintsTransformation')


    def transform(self) -> pd.DataFrame:
        """
        Function to rename (to snake_case), format, and change data types.
        Returns a dataframe.
        """
        transformed_df = self.df.rename(columns=self.column_rename)

        transformed_df['ano'] = pd.to_numeric(transformed_df['ano'], errors='coerce')
        transformed_df['trimestre'] = transformed_df['trimestre'].astype('string')
        transformed_df['categoria'] = transformed_df['categoria'].astype('string')
        transformed_df['tipo'] = transformed_df['tipo'].astype('string')
        transformed_df['cnpj'] = np.where(transformed_df['cnpj_if'] == ' ', ' ', transformed_df['cnpj_if'].astype('string').str.zfill(8))
        transformed_df['nome'] = transformed_df['instituicao_financeira'].astype('string').str.replace(' (conglomerado)', '')
        transformed_df['indice'] = pd.to_numeric(transformed_df['indice'].astype(str).str.replace(',', '.'), errors='coerce')
        transformed_df['qtd_reclamacoes_reguladas_procedentes'] = pd.to_numeric(transformed_df['qtd_reclamacoes_reguladas_procedentes'], errors='coerce')
        transformed_df['qtd_reclamacoes_reguladas_outras'] = pd.to_numeric(transformed_df['qtd_reclamacoes_reguladas_outras'], errors='coerce')
        transformed_df['qtd_reclamacoes_nao_reguladas'] = pd.to_numeric(transformed_df['qtd_reclamacoes_nao_reguladas'], errors='coerce')
        transformed_df['qtd_total_reclamacoes'] = pd.to_numeric(transformed_df['qtd_total_reclamacoes'], errors='coerce')
        transformed_df['qtd_total_clientes_ccs_scr'] = pd.to_numeric(transformed_df['qtd_total_clientes_ccs_scr'], errors='coerce')
        transformed_df['qtd_clientes_ccs'] = pd.to_numeric(transformed_df['qtd_clientes_ccs'], errors='coerce')
        transformed_df['qtd_clientes_scr'] = pd.to_numeric(transformed_df['qtd_clientes_scr'], errors='coerce')

        return transformed_df


    def calculate_aggregates(self, df) -> pd.DataFrame:
        """
        Function to return a pandas dataframe of a pivot table grouped by the 'name' column,
        aggregating columns like 'indice', 'qtd_total_reclamacoes', and 'qtd_total_clientes_ccs_scr' by mean.
        """
        aggregated_df  = df.groupby('nome', as_index=False).agg({
            'indice': 'mean',
            'qtd_total_reclamacoes': 'mean',
            'qtd_total_clientes_ccs_scr': 'mean'
        })
        return aggregated_df 
