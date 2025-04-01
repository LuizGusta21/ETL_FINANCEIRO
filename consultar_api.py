import requests # type: ignore
import math
import logging

# Configuração do Logger
logging.basicConfig(
    filename='log_apis.txt',  # Arquivo onde os logs serão armazenados
    level=logging.INFO,  # Nível de log: INFO, DEBUG, ERROR, etc.
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato da mensagem
)

def log_message(message: str):
    """Função auxiliar para registrar as mensagens de log."""
    logging.info(message)

def log_error(message: str, exc: Exception, payload=None, response=None):
    """Função auxiliar para registrar mensagens de erro com exceção."""
    logging.error(f"{message} - Exception: {exc}", exc_info=True)
    if payload:
        logging.error(f"Payload: {payload}")
    if response:
        logging.error(f"Response: {response.text if response else 'No response'}")

# fazer requisicao e caso de erro, repetir até 5x
def fazer_requisicao(url, payload, headers):
    tentativas = 0
    while tentativas < 5:
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=120)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:  # noqa: F841
            tentativas += 1
    return None


def consultar_movimentos(app_key: str, app_secret: str, empresa: str, dtinicio: str = None, dtfim: str = None) -> list:
    """
    Consulta os dados financeiros na API da Omie, focando em movimentos de contas.

    Parâmetros:
        app_key (str): Chave de acesso da API da Omie.
        app_secret (str): Segredo de acesso da API da Omie.
        empresa (str): Nome da empresa de cada API consultada.
        dtinicio (str, opcional): Data de início no formato "DD/MM/AAAA".
        dtfim (str, opcional): Data de fim no formato "DD/MM/AAAA".

    Retorno:
        list: Lista contendo todas as movimentações feitas.
    """
    log_message("Iniciando consulta de movimentos...")
    
    url = "https://app.omie.com.br/api/v1/financas/mf/"

    payload = {
        "call": "ListarMovimentos",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [{"nPagina": 1, "nRegPorPagina": 1}]
    }

    # Adicionar filtros de data ao payload se fornecidos
    if dtinicio or dtfim:
        payload["param"][0]["dDtEmisDe"] = dtinicio if dtinicio else ""
        payload["param"][0]["dDtEmisAte"] = dtfim if dtfim else ""

    headers = {"Content-Type": "application/json"}

    all_data = []
    try:
        log_message(f"Realizando a primeira requisição para obter o total de registros da {empresa}")
        dados = fazer_requisicao(url,payload, headers)
        
        total_registros = dados.get("nTotRegistros", 0)
        n_reg_por_pagina = 500
        total_paginas = math.ceil(total_registros / n_reg_por_pagina)

        log_message(f"Total de registros: {total_registros}. Total de páginas: {total_paginas}.")

        for pagina in range(1, total_paginas + 1):
            payload["param"][0]["nPagina"] = pagina
            payload["param"][0]["nRegPorPagina"] = n_reg_por_pagina

            dados = fazer_requisicao(url,payload, headers)
            
            movimentos = dados.get("movimentos", [])
            for movimento in movimentos:
                movimento["empresa"] = empresa
            all_data.extend(movimentos)

        log_message(f"Consulta de movimentos finalizada. Total de movimentos encontrados: {len(all_data)}.")

    except requests.exceptions.RequestException as e:
        log_error("Erro na requisição durante a consulta de movimentos", e, payload, dados)

    return all_data

    
    
def consultar_categorias(app_key: str, app_secret: str, empresa: str) -> list:
    """
    Consulta os dados categoricos na API da Omie.

    Parâmetros:
        app_key (str): Chave de acesso da API da Omie.
        app_secret (str): Segredo de acesso da API da Omie.
        empresa: (str): Nome da empresa de cada API consultada.

    Retorno:
        list: Lista contendo todas as categorias cadastradas.
    """
    log_message("Iniciando consulta de categorias...")

    url = "https://app.omie.com.br/api/v1/geral/categorias/"
    
    payload = {
        "call": "ListarCategorias",
        "app_key": app_key,
        "app_secret": app_secret,
    }
    headers = {"Content-Type": "application/json"}

    all_data = []
    try:
        log_message(f"Realizando a primeira requisição para obter o total de registros da {empresa}")
        # Enviar a requisição para a primeira página para saber o total de registros
        payload['param'] = [{"pagina": 1, "registros_por_pagina": 1}]  # 1 registro por página
        dados = fazer_requisicao(url,payload, headers)
        
        # Obter o total de registros e calcular o total de páginas
        total_registros = dados.get("total_de_registros", 0)
        n_reg_por_pagina = 500  # Ajuste o número de registros por página conforme necessário
        total_paginas = math.ceil(total_registros / n_reg_por_pagina)

        log_message(f"Total de registros: {total_registros}. Total de páginas: {total_paginas}.")
        
        # Iterar pelas páginas e coletar os dados
        for pagina in range(1, total_paginas + 1):
            # print(f"Requisitando a página {pagina}/{total_paginas}...")
            payload['param'] = [{"pagina": pagina, "registros_por_pagina": n_reg_por_pagina}]
            dados = fazer_requisicao(url,payload, headers)
            
            # Extrair dados da página
            categorias = dados.get('categoria_cadastro', [])
            for movimento in categorias:
                movimento['empresa'] = empresa
            all_data.extend(categorias)

        log_message(f"Consulta de categorias finalizada. Total de categorias encontradas: {len(all_data)}.")

    except requests.exceptions.RequestException as e:
        log_error("Erro na requisição durante a consulta de categorias", e, payload, dados)
    return all_data

def consultar_orcamentos(app_key: str, app_secret: str, empresa: str, ano: int, mes: int) -> list:
    """
    Consulta os dados de orçamentos na API da Omie.

    Parâmetros:
        app_key (str): Chave de acesso da API da Omie.
        app_secret (str): Segredo de acesso da API da Omie.
        empresa: (str): Nome da empresa de cada API consultada.
        ano: (int): Ano que irá puxar a resposta da API.
        mes: (int): Mês que irá puxar a resposta da API.

    Retorno:
        list: Lista contendo todas as categorias cadastradas.
    """
    log_message("Iniciando consulta de orçamentos...")

    url = "https://app.omie.com.br/api/v1/financas/caixa/"
    
    payload = {
        "call": "ListarOrcamentos",
        "app_key": app_key,
        "app_secret": app_secret,
    }
    headers = {"Content-Type": "application/json"}

    try:
        log_message(f"Realizando a carga total dos orcamentos da {empresa}")
        # por no payload o ano e mes que irá puxar da API
        payload['param'] = [{"nAno": ano, "nMes": mes}]
        dados = fazer_requisicao(url,payload, headers)
        # Obtém a lista de orçamentos ou uma lista vazia caso a chave não exista
        orcamentos = dados.get("ListaOrcamentos", [])

        # Adiciona empresa, ano e mês a cada orçamento individualmente
        for orcamento in orcamentos:
            orcamento["empresa"] = empresa
            orcamento["nAno"] = ano
            orcamento["nMes"] = mes
        
        log_message(f"Consulta de orcamento finalizada. Total de orcamentos encontrados: {len(orcamentos)}.")
        return orcamentos
    except requests.exceptions.RequestException as e:
        log_error("Erro na requisição durante a consulta de orcamentos", e, payload, dados)
        return []  # Retorna uma lista vazia em caso de erro
    

def consultar_dre(app_key: str, app_secret: str, empresa: str) -> list:
    """
    Consulta os dados de orçamentos na API da Omie.

    Parâmetros:
        app_key (str): Chave de acesso da API da Omie.
        app_secret (str): Segredo de acesso da API da Omie.
        empresa: (str): Nome da empresa de cada API consultada.

    Retorno:
        list: Lista contendo todas as categorias cadastradas.
    """


    url = "https://app.omie.com.br/api/v1/geral/dre/"
    
    payload = {
        "call": "ListarCadastroDRE",
        "app_key": app_key,
        "app_secret": app_secret,
    }
    headers = {"Content-Type": "application/json"}

    try:
     
        # por no payload o ano e mes que irá puxar da API
        payload['param'] = [{"apenasContasAtivas": "N"}]
        dados = fazer_requisicao(url,payload, headers)
        # Obtém a lista de orçamentos ou uma lista vazia caso a chave não exista
        lista_dre = dados.get("dreLista", [])

        # Adiciona empresa, ano e mês a cada orçamento individualmente
        for dre in lista_dre:
            dre["empresa"] = empresa


        return lista_dre

    except requests.exceptions.RequestException as e:
        print(e, empresa)
        return []  # Retorna uma lista vazia em caso de erro