import os


class GravarDados:
    """
    Grava os dados de um pandas dataframe em um diretório enviado à função junto ao seu nome de arquivo.
    O tipo de arquivo de saída é fixo em parquet.
    """
    def gravar_dados_parquet(self, df, diretorio_saida, nome_arquivo):
        if not os.path.exists(diretorio_saida):
            os.makedirs(diretorio_saida)
        df.to_parquet(os.path.join(diretorio_saida, nome_arquivo), engine='fastparquet')
