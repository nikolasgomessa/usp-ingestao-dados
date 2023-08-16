import pandas as pd
import glob
import os
import logging
from carregar import CarregarArquivo
from gravar import GravarDados
from transformar import TransformacaoBancos, TransformacaoEmpregados, TransformacaoReclamacoes

pd.options.display.float_format = '{:.2f}'.format

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    diretorio_atual = os.getcwd()

    carregar_bancos = CarregarArquivo(
        diretorio_arquivos=[os.path.join(diretorio_atual, 'raw/Bancos/EnquadramentoInicia_v2.tsv')], tipo_arquivo='tsv',
        separador='\t')
    df_bancos_raw = carregar_bancos.carregar()

    carregar_empregados = CarregarArquivo(
        diretorio_arquivos=glob.glob(os.path.join(diretorio_atual, 'raw/Empregados', "*.csv")), tipo_arquivo='csv',
        separador='|')
    df_empregados_raw = carregar_empregados.carregar()

    carregar_reclamacoes = CarregarArquivo(
        diretorio_arquivos=glob.glob(os.path.join(diretorio_atual, 'raw/Reclamações', "*.csv")), tipo_arquivo='csv',
        separador=';')
    df_reclamacoes_raw = carregar_reclamacoes.carregar()

    transforma_bancos = TransformacaoBancos(df_bancos_raw)
    df_bancos = transforma_bancos.transformar()

    transforma_empregados = TransformacaoEmpregados(df_empregados_raw)
    df_empregados = transforma_empregados.transformar()
    df_empregados_agrupado = transforma_empregados.analisar(df_empregados)

    transforma_reclamacoes = TransformacaoReclamacoes(df_reclamacoes_raw)
    df_reclamacoes = transforma_reclamacoes.transformar()
    df_reclamacoes_agrupado = transforma_reclamacoes.analisar(df_reclamacoes)

    df_reclamacoes_bancos = df_bancos.merge(df_reclamacoes_agrupado, on='nome', how='inner')
    logging.info(f'Count Reclamacoes x Bancos: \n{df_reclamacoes_bancos.count()}')

    df_reclamacoes_bancos_empregados = df_reclamacoes_bancos.merge(df_empregados_agrupado, on='nome', how='inner')
    logging.info(f'Count Reclamacoes x Bancos x Empregados: \n{df_reclamacoes_bancos_empregados.count()}')

    diretorio_saida = os.path.join(diretorio_atual, 'transformed')
    nome_arquivo = 'atividade02.parquet'
    gravar_dados = GravarDados()
    gravar_dados.gravar_dados_parquet(df_reclamacoes_bancos_empregados, diretorio_saida, nome_arquivo)


