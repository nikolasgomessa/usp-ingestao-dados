import os


class GravarDados:
    def gravar_dados_parquet(self, df, diretorio_saida, nome_arquivo):
        if not os.path.exists(diretorio_saida):
            os.makedirs(diretorio_saida)
        df.to_parquet(os.path.join(diretorio_saida, nome_arquivo), engine='fastparquet')
