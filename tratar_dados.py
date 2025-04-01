import pandas as pd
import html
import numpy as np

def tratamento_movimentos(lista):
    """
    Processa os dados consultados da API, transformando-os em um DataFrame 
    e aplicando os tratamentos necessários para inserção no banco de dados.

    Parâmetros:
        lista (list): Lista de dados brutos retornados pela API.

    Retorna:
        pandas.DataFrame: DataFrame tratado e pronto para ser armazenado no banco.
    """
    if lista:
        print(f"\nTotal de movimentos obtidos: {len(lista)}")
        # Normalizando os dados
        df = pd.json_normalize(lista,sep="_")
        # Renomeia as colunas para remover os prefixos 'resumo_' e 'detalhes_'
        df.columns = df.columns.str.replace('detalhes_', '', regex=False)
        
        numerical_columns = [
            'nCodTitulo', 'nCodCliente', 'nCodCtr', 'nCodOS', 'nCodCC', 'nValorTitulo', 
            'nValorPIS', 'nValorCOFINS', 'nValorCSLL', 'nValorIR', 'nValorISS', 'nValorINSS', 
            'nCodProjeto', 'cCodVendedor', 'nCodComprador', 'nCodNF', 'nCodTitRepet', 'nCodMovCC', 
            'nValorMovCC', 'nCodMovCCRepet', 'nDesconto', 'nJuros', 'nMulta', 'nCodBaixa', 
            'resumo_nValPago', 'resumo_nValAberto', 'resumo_nDesconto', 'resumo_nJuros', 'resumo_nMulta', 
            'resumo_nValLiquido'
        ]

        # Converte para numérico, forçando nulos (erro='coerce')
        for col in numerical_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Converte colunas de data com pd.to_datetime(), forçando nulos (erro='coerce')
        date_columns = [
            'dDtEmissao', 'dDtVenc', 'dDtPrevisao', 'dDtPagamento', 'dDtRegistro', 
            'dDtCredito', 'dDtConcilia', 'dDtInc', 'dDtAlt'
        ]

        # Converte para datetime, forçando nulos (erro='coerce')
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

        tipos_colunas = {
            'ID': 'Int64',
            'nCodTitulo': 'Int64',
            'empresa': 'string',
            'cCodIntTitulo': 'string',
            'cNumTitulo': 'string',
            'dDtEmissao': 'datetime64[ns]',
            'dDtVenc': 'datetime64[ns]',
            'dDtPrevisao': 'datetime64[ns]',
            'dDtPagamento': 'datetime64[ns]',
            'nCodCliente': 'Int64',
            'cCPFCNPJCliente': 'string',
            'nCodCtr': 'Int64',
            'cNumCtr': 'string',
            'nCodOS': 'Int64',
            'cNumOS': 'string',
            'nCodCC': 'Int64',
            'cStatus': 'string',
            'cNatureza': 'string',
            'cTipo': 'string',
            'cOperacao': 'string',
            'cNumDocFiscal': 'string',
            'cCodCateg': 'string',
            'cNumParcela': 'string',
            'nValorTitulo': 'float64',  # Alterado de decimal para float64
            'nValorPIS': 'float64',
            'cRetPIS': 'string',
            'nValorCOFINS': 'float64',
            'cRetCOFINS': 'string',
            'nValorCSLL': 'float64',
            'cRetCSLL': 'string',
            'nValorIR': 'float64',
            'cRetIR': 'string',
            'nValorISS': 'float64',
            'cRetISS': 'string',
            'nValorINSS': 'float64',
            'cRetINSS': 'string',
            'cCodProjeto': 'Int64',
            'observacao': 'string',
            'cCodVendedor': 'Int64',
            'nCodComprador': 'Int64',
            'cCodigoBarras': 'string',
            'cNSU': 'string',
            'nCodNF': 'Int64',
            'dDtRegistro': 'datetime64[ns]',
            'cNumBoleto': 'string',
            'cChaveNFe': 'string',
            'cOrigem': 'string',
            'nCodTitRepet': 'Int64',
            'cGrupo': 'string',
            'nCodMovCC': 'Int64',
            'nValorMovCC': 'float64',
            'nCodMovCCRepet': 'Int64',
            'nDesconto': 'float64',
            'nJuros': 'float64',
            'nMulta': 'float64',
            'nCodBaixa': 'Int64',
            'dDtCredito': 'datetime64[ns]',
            'dDtConcilia': 'datetime64[ns]',
            'cHrConcilia': 'string',
            'cUsConcilia': 'string',
            'dDtInc': 'datetime64[ns]',
            'cHrInc': 'string',
            'cUsInc': 'string',
            'dDtAlt': 'datetime64[ns]',
            'cHrAlt': 'string',
            'cUsAlt': 'string',
            'resumo_cLiquidado': 'string',
            'resumo_nValPago': 'float64',
            'resumo_nValAberto': 'float64',
            'resumo_nDesconto': 'float64',
            'resumo_nJuros': 'float64',
            'resumo_nMulta': 'float64',
            'resumo_nValLiquido': 'float64'
        }
        # Verifica quais colunas existem no DataFrame antes de aplicar os tipos
        # colunas_existentes = {col: tipos_colunas[col] for col in df.columns if col in tipos_colunas}
        
        # Adicionando colunas ausentes com valores padrão
        for col, tipo in tipos_colunas.items():
            if col not in df.columns:
                if tipo.startswith('datetime'):
                    df[col] = pd.NaT  # Data em branco
                elif tipo == 'Int64' or tipo == 'float64':
                    df[col] = np.nan  # Número em branco
                else:
                    df[col] = ''  # String em branco
        # Alterando a tipagem das colunas presentes no DataFrame
        df = df.astype(tipos_colunas)

        return df
    else:
        print("Nenhum dado encontrado.")
        
def tratamento_categorias(lista):
    """
    Processa os dados consultados da API, transformando-os em um DataFrame 
    e aplicando os tratamentos necessários para inserção no banco de dados.

    Parâmetros:
        lista (list): Lista de dados brutos retornados pela API.

    Retorna:
        pandas.DataFrame: DataFrame tratado e pronto para ser armazenado no banco.
    """
    if lista:
        print(f"\nTotal de movimentos obtidos: {len(lista)}")
        # Criando DataFrame
        df = pd.json_normalize(lista)
        # Corrige caracteres HTML escapados
        for col in df.select_dtypes(include=[object]):  # Selecione apenas colunas de tipo string
            df[col] = df[col].map(lambda x: html.unescape(x) if isinstance(x, str) else x)
        # criando coluna ID
        df['ID'] = df['codigo'].astype(str) + "_" + df['empresa'].astype(str)
        colunas_renomear = {
            'dadosDRE.codigoDRE': 'dre_codigoDRE',
            'dadosDRE.descricaoDRE': 'dre_descricaoDRE',
            'dadosDRE.naoExibirDRE': 'dre_naoExibirDRE',
            'dadosDRE.nivelDRE': 'dre_nivelDRE',
            'dadosDRE.sinalDRE': 'dre_sinalDRE',
            'dadosDRE.totalizaDRE': 'dre_totalizaDRE'
        }
        df.rename(columns= colunas_renomear, inplace=True)
        # Substituir valores vazios por NaN para colunas numéricas antes da conversão
        df['id_conta_contabil'] = pd.to_numeric(df['id_conta_contabil'], errors='coerce')
        df['dre_nivelDRE'] = pd.to_numeric(df['dre_nivelDRE'], errors='coerce')

        tipos_colunas = {
            'ID': 'string',
            'codigo': 'string',
            'empresa': 'string',
            'descricao': 'string',
            'descricao_padrao': 'string',
            'tipo_categoria': 'string',
            'conta_inativa': 'string',
            'definida_pelo_usuario': 'string',
            'id_conta_contabil': 'Int64',  # Pandas usa 'Int64' para suportar valores nulos
            'tag_conta_contabil': 'string',
            'conta_despesa': 'string',
            'conta_receita': 'string',
            'nao_exibir': 'string',
            'natureza': 'string',
            'totalizadora': 'string',
            'transferencia': 'string',
            'codigo_dre': 'string',
            'categoria_superior': 'string',  # Pandas não tem tipo Text, 'string' equivale
            'dre_codigoDRE': 'string',
            'dre_descricaoDRE': 'string',
            'dre_naoExibirDRE': 'string',
            'dre_nivelDRE': 'Int64',
            'dre_sinalDRE': 'string',
            'dre_totalizaDRE': 'string'
        }
        df = df.astype(tipos_colunas)
        return df
    else:
        print("Nenhum dado encontrado.")

def tratamento_orcamentos(lista):
    """
    Processa os dados consultados da API, transformando-os em um DataFrame 
    e aplicando os tratamentos necessários para inserção no banco de dados.

    Parâmetros:
        lista (list): Lista de dados brutos retornados pela API.

    Retorna:
        pandas.DataFrame: DataFrame tratado e pronto para ser armazenado no banco.
    """
    if lista:
        # Criando DataFrame
        df = pd.json_normalize(data=lista)
        # criando coluna ID
        df['ID'] = df['cCodCateg'].astype(str) + "_" + df['empresa'].astype(str)
        tipos_colunas = {
            'ID': 'string',
            'cCodCateg': 'string',
            'cDesCateg': 'string',
            'nValorPrevisto': 'float64',
            'nValorRealizado': 'float64',
            'nAno': 'Int64',
            'nMes': 'Int64',
            'empresa': 'string'
        }
        df = df.astype(tipos_colunas)
        return df
    else:
        print("Nenhum dado encontrado.")


def tratamento_dre(lista):
    """
    Processa os dados consultados da API, transformando-os em um DataFrame 
    e aplicando os tratamentos necessários para inserção no banco de dados.

    Parâmetros:
        lista (list): Lista de dados brutos retornados pela API.

    Retorna:
        pandas.DataFrame: DataFrame tratado e pronto para ser armazenado no banco.
    """
    if lista:
        # Criando DataFrame
        df = pd.json_normalize(data=lista)
        # criando coluna ID
        df['ID'] = df['codigoDRE'].astype(str) + "_" + df['empresa'].astype(str)
        tipos_colunas = {
            'ID': 'string',
            'codigoDRE': 'string',
            'descricaoDRE': 'string',
            'naoExibirDRE': 'string',
            'nivelDRE': 'Int64',
            'sinalDRE': 'string',
            'totalizaDRE': 'string',
            'empresa': 'string'
        }
        df = df.astype(tipos_colunas)
        return df
    else:
        print("Nenhum dado encontrado.")