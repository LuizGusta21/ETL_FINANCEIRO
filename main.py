from consultar_api import consultar_movimentos , consultar_categorias, consultar_orcamentos, consultar_dre
from tratar_dados import tratamento_movimentos, tratamento_categorias, tratamento_orcamentos, tratamento_dre
from post_banco import carregar_dados, conectar_banco
import config
import pandas as pd  # noqa: F401
from datetime import datetime, timedelta

# Obtém o dia da semana (0 = Segunda-feira, 6 = Domingo)
hoje = datetime.today()
mes = hoje.month
ano = hoje.year
dias_anteriores =  hoje - timedelta(days=45)

# Convertendo para string no formato "DD/MM/AAAA"
hoje_str = hoje.strftime("%d/%m/%Y")
dias_anteriores_str = dias_anteriores.strftime("%d/%m/%Y")
hoje_sql = hoje.strftime("%Y-%m-%d")
dias_anteriores_sql = dias_anteriores.strftime("%Y-%m-%d")

if hoje.weekday() == 0: # Segunda-feira
    print()

def main():
    todos_movimentos = []
    todas_categorias = []
    todos_orcamentos = []
    todas_dres = []
    try:
        for empresa, credenciais in config.dados_empresas.items():
            app_key = credenciais.get("APP_KEY")
            app_secret = credenciais.get("APP_SECRET")

            if not app_key or not app_secret:
                print(f"[ERRO] Credenciais ausentes para a empresa: {empresa}")
                continue

            try:
                # Coletando os dados
                todos_movimentos.extend(consultar_movimentos(app_key, app_secret, empresa, dtinicio=dias_anteriores_str, dtfim=hoje_str))
                if hoje.weekday() == 0:
                    todas_categorias.extend(consultar_categorias(app_key, app_secret, empresa))
                    todas_dres.extend(consultar_dre(app_key, app_secret, empresa))

                    for ano in range(2024, 2026):
                        for mes in range(1, 13):
                            if ano == 2025 and mes > 3:
                                continue  # Evita meses além de março de 2025
                            todos_orcamentos.extend(consultar_orcamentos(app_key, app_secret, empresa, ano, mes))

                print(f"[SUCESSO] {empresa} teve todos os dados processados com sucesso.")

            except Exception as e:
                print(f"[ERRO] Falha ao consultar dados para {empresa}: {e}")

        # Verifica se há dados antes de processá-los
        if not any([todos_movimentos, todas_categorias, todos_orcamentos, todas_dres]):
            print("[AVISO] Nenhum dado coletado. Processo encerrado.")
            return

        # Tratamento dos dados
        df_movimentos = tratamento_movimentos(todos_movimentos) if todos_movimentos else None
        df_categorias = tratamento_categorias(todas_categorias) if todas_categorias else None
        df_orcamentos = tratamento_orcamentos(todos_orcamentos) if todos_orcamentos else None
        df_dre = tratamento_dre(todas_dres) if todas_dres else None

        # Conectando ao banco e carregando os dados
        conexao_banco = conectar_banco(user='', password='', host='', name='')

        try:
            if df_categorias is not None:
                carregar_dados(df=df_categorias, tabela='categorias', engine=conexao_banco)
                print("[SUCESSO] categorias carregados no banco com sucesso.")
            if df_movimentos is not None:
                carregar_dados(df=df_movimentos, tabela='movimentacoes', engine=conexao_banco,dtinicio= dias_anteriores_sql, dtfim= hoje_sql)
                print("[SUCESSO] movimentacoes carregados no banco com sucesso.")
            if df_orcamentos is not None:
                carregar_dados(df=df_orcamentos, tabela='orcamentos', engine=conexao_banco)
                print("[SUCESSO] orçamentos carregados no banco com sucesso.")
            if df_dre is not None:
                carregar_dados(df=df_dre, tabela='dre', engine=conexao_banco)
                print("[SUCESSO] dre carregados no banco com sucesso.")

            print("[SUCESSO] Todos os dados carregados no banco com sucesso.")

        except Exception as e:
            print(f"[ERRO] Falha ao carregar dados no banco: {e}")

    except Exception as e:
        print(f"[ERRO CRÍTICO] Erro inesperado: {e}")

    finally:
        # Garante que a conexão com o banco seja fechada
        if 'conexao_banco' in locals():
            conexao_banco.dispose()
            print("[INFO] Conexão com o banco fechada.")


if __name__ == "__main__":
    main()