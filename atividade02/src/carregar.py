import pandas as pd


class TipoNaoSuportado(Exception):
    """
    Retorna uma mensagem de erro quando o arquivo lido não corresponde aos listados na função 'carregar()'
    """
    def __init__(self, tipo):
        """
        Define a mensagem conforme o tipo de arquivo enviado como errado.
        """
        self.tipo = tipo
        self.mensagem = f"Arquivo(s) do tipo {tipo} não suportado(s)"
        super().__init__(self.mensagem)


class CarregarDados():
    def __init__(self, diretorio_arquivos: list, tipo_arquivo: str, separador: str = ';'):
        """
        Carrega arquivos CSV ou TSV de um ou mais diretório(s) especifico(s), sendo esses obrigatoriamente de um tipo e separador único.
        """
        self.diretorio_arquivos = diretorio_arquivos
        self.tipo_arquivo = tipo_arquivo
        self.separador = separador

    def carregar(self) -> pd.DataFrame:
        """
        Concatena, via pandas, os dataframes da lista de paths e retorna um único concatenado.
        """
        dfs = []
        for diretorio_arquivo in self.diretorio_arquivos:
            if self.tipo_arquivo in ('csv', 'tsv'):
                df = pd.read_csv(diretorio_arquivo, sep=self.separador)
            else:
                raise TipoNaoSuportado(self.tipo_arquivo)
            dfs.append(df)
        return pd.concat(dfs, axis=0, ignore_index=True)
