from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class TransformarDados(ABC):
    def formatar_cnpj(self, valor):
        if pd.notna(valor) and isinstance(valor, (int, float)):
            return f'{str(int(valor)).zfill(8)}'
        return ' '

    @abstractmethod
    def transformar(self) -> pd.DataFrame:
        pass


class TransformacaoBancos(TransformarDados):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transformar(self) -> pd.DataFrame:
        df_transformado = self.df.rename(columns={
            'Segmento' : 'segmento',
            'CNPJ': 'cnpj',
            'Nome': 'nome'
        })
        df_transformado['cnpj'] = df_transformado['cnpj'].apply(self.formatar_cnpj)
        df_transformado['nome'] = df_transformado['nome'].astype('string').str.replace(' - PRUDENCIAL', '')
        return df_transformado


class TransformacaoEmpregados(TransformarDados):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transformar(self) -> pd.DataFrame:
        df_transformado = self.df.rename(columns={
            'employer-website' : 'employer_website',
            'employer-headquarters': 'employer_headquarters',
            'employer-founded': 'employer_founded',
            'employer-industry': 'employer_industry',
            'employer-revenue': 'employer_revenue',
            'Geral': 'geral',
            'Cultura e valores': 'cultura_valores',
            'Diversidade e inclusão': 'diversidade_inclusao',
            'Qualidade de vida': 'qualidade_vida',
            'Alta liderança': 'alta_lideranca',
            'Remuneração e benefícios': 'remuneracao_beneficios',
            'Oportunidades de carreira': 'oportunidades_carreira',
            'Recomendam para outras pessoas(%)': 'percentual_recomendam_para_outras_pessoas',
            'Perspectiva positiva da empresa(%)': 'percentual_perspectiva_positiva_empresa',
            'CNPJ': 'cnpj',
            'Nome': 'nome',
            'Segmento': 'segmento'
        })

        df_transformado['employer_name'] = df_transformado['employer_name'].astype('string')
        df_transformado['reviews_count'] = pd.to_numeric(df_transformado['reviews_count'], errors='coerce')
        df_transformado['culture_count'] = pd.to_numeric(df_transformado['culture_count'], errors='coerce')
        df_transformado['salaries_count'] = pd.to_numeric(df_transformado['salaries_count'], errors='coerce')
        df_transformado['benefits_count'] = pd.to_numeric(df_transformado['benefits_count'], errors='coerce')
        df_transformado['employer_website'] = df_transformado['employer_website'].astype('string')
        df_transformado['employer_headquarters'] = df_transformado['employer_headquarters'].astype('string')
        df_transformado['employer_founded'] = pd.to_numeric(df_transformado['employer_founded'], errors='coerce')
        df_transformado['employer_industry'] = df_transformado['employer_industry'].astype('string')
        df_transformado['employer_revenue'] = df_transformado['employer_revenue'].astype('string')
        df_transformado['url'] = df_transformado['url'].astype('string')
        df_transformado['geral'] = pd.to_numeric(df_transformado['geral'], errors='coerce')
        df_transformado['cultura_valores'] = pd.to_numeric(df_transformado['cultura_valores'], errors='coerce')
        df_transformado['diversidade_inclusao'] = pd.to_numeric(df_transformado['diversidade_inclusao'], errors='coerce')
        df_transformado['qualidade_vida'] = pd.to_numeric(df_transformado['qualidade_vida'], errors='coerce')
        df_transformado['alta_lideranca'] = pd.to_numeric(df_transformado['alta_lideranca'], errors='coerce')
        df_transformado['remuneracao_beneficios'] = pd.to_numeric(df_transformado['remuneracao_beneficios'], errors='coerce')
        df_transformado['oportunidades_carreira'] = pd.to_numeric(df_transformado['oportunidades_carreira'], errors='coerce')
        df_transformado['percentual_recomendam_para_outras_pessoas'] = pd.to_numeric(df_transformado['percentual_recomendam_para_outras_pessoas'], errors='coerce')
        df_transformado['percentual_perspectiva_positiva_empresa'] = pd.to_numeric(df_transformado['percentual_perspectiva_positiva_empresa'], errors='coerce')
        df_transformado['cnpj'] = df_transformado['cnpj'].apply(self.formatar_cnpj)
        df_transformado['nome'] = df_transformado['nome'].astype('string')
        df_transformado['segmento'] = df_transformado['segmento'].astype('string')
        df_transformado['match_percent'] = pd.to_numeric(df_transformado['match_percent'], errors='coerce')

        return df_transformado

    def analisar(self, df) -> pd.DataFrame:
        df_empregados_agrupado = df.groupby('nome', as_index=False).agg({
            'geral': 'mean',
            'remuneracao_beneficios': 'mean'
        })
        return df_empregados_agrupado


class TransformacaoReclamacoes(TransformarDados):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transformar(self) -> pd.DataFrame:
        df_transformado = self.df.rename(columns={
            'Ano' : 'ano',
            'Trimestre': 'trimestre',
            'Categoria': 'categoria',
            'Tipo': 'tipo',
            'CNPJ IF': 'cnpj_if',
            'Instituição financeira': 'instituicao_financeira',
            'Índice': 'indice',
            'Quantidade de reclamações reguladas procedentes': 'qtd_reclamacoes_reguladas_procedentes',
            'Quantidade de reclamações reguladas - outras': 'qtd_reclamacoes_reguladas_outras',
            'Quantidade de reclamações não reguladas': 'qtd_reclamacoes_nao_reguladas',
            'Quantidade total de reclamações': 'qtd_total_reclamacoes',
            'Quantidade total de clientes - CCS e SCR': 'qtd_total_clientes_ccs_scr',
            'Quantidade de clientes - CCS': 'qtd_clientes_ccs',
            'Quantidade de clientes - SCR': 'qtd_clientes_scr'
        })

        df_transformado['ano'] = pd.to_numeric(df_transformado['ano'], errors='coerce')
        df_transformado['trimestre'] = df_transformado['trimestre'].astype('string')
        df_transformado['categoria'] = df_transformado['categoria'].astype('string')
        df_transformado['tipo'] = df_transformado['tipo'].astype('string')
        df_transformado['cnpj'] = np.where(df_transformado['cnpj_if'] == ' ', ' ', df_transformado['cnpj_if'].astype('string').str.zfill(8))
        df_transformado['nome'] = df_transformado['instituicao_financeira'].astype('string').str.replace(' (conglomerado)', '')
        df_transformado['indice'] = pd.to_numeric(df_transformado['indice'].astype(str).str.replace(',', '.'), errors='coerce')
        df_transformado['qtd_reclamacoes_reguladas_procedentes'] = pd.to_numeric(df_transformado['qtd_reclamacoes_reguladas_procedentes'], errors='coerce')
        df_transformado['qtd_reclamacoes_reguladas_outras'] = pd.to_numeric(df_transformado['qtd_reclamacoes_reguladas_outras'], errors='coerce')
        df_transformado['qtd_reclamacoes_nao_reguladas'] = pd.to_numeric(df_transformado['qtd_reclamacoes_nao_reguladas'], errors='coerce')
        df_transformado['qtd_total_reclamacoes'] = pd.to_numeric(df_transformado['qtd_total_reclamacoes'], errors='coerce')
        df_transformado['qtd_total_clientes_ccs_scr'] = pd.to_numeric(df_transformado['qtd_total_clientes_ccs_scr'], errors='coerce')
        df_transformado['qtd_clientes_ccs'] = pd.to_numeric(df_transformado['qtd_clientes_ccs'], errors='coerce')
        df_transformado['qtd_clientes_scr'] = pd.to_numeric(df_transformado['qtd_clientes_scr'], errors='coerce')

        return df_transformado

    def analisar(self, df) -> pd.DataFrame:
        df_reclamacoes_agrupado = df.groupby('nome', as_index=False).agg({
            'indice': 'mean',
            'qtd_total_reclamacoes': 'mean',
            'qtd_total_clientes_ccs_scr': 'mean'
        })
        return df_reclamacoes_agrupado
