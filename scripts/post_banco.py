from sqlalchemy import create_engine, Column, String, Integer, Boolean, ForeignKey, DECIMAL, Date, Text, text, BigInteger  # noqa: F401
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd  # noqa: F401


def conectar_banco(user, password, host, name):
    """
    Estabelece uma conexão com um banco de dados MySQL usando SQLAlchemy.

    Parâmetros:
    - user (str): Nome de usuário do banco de dados.
    - password (str): Senha do banco de dados.
    - host (str): Endereço IP ou hostname do servidor MySQL.
    - name (str): Nome do banco de dados.

    Retorna:
    - sqlalchemy.engine.base.Engine: Objeto de conexão com o banco de dados.

    Exemplo de uso:
    python
    engine = conectar_banco("usuario", "senha123", "localhost", "meu_banco")
    
    """
    db_user = user
    db_password = password
    db_host = host
    db_name = name

    engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}')
    return engine

def criar_tabelas():
    db = conectar_banco(user="aizen", password="aizenmito%4036", host="192.168.100.162", name="teste")

    Session = sessionmaker(bind=db)
    session = Session()  # noqa: F841
    Base = declarative_base()

    class Movimentacao(Base):
        __tablename__ = 'movimentacoes'
        ID = Column(Integer, primary_key=True, autoincrement=True)
        nCodTitulo = Column(BigInteger)  # Alterado para BigInteger
        empresa = Column(String(50))
        cCodIntTitulo = Column(String(60))
        cNumTitulo = Column(String(20))
        dDtEmissao = Column(Date)
        dDtVenc = Column(Date)
        dDtPrevisao = Column(Date)
        dDtPagamento = Column(Date)
        nCodCliente = Column(BigInteger)  # Alterado para BigInteger
        cCPFCNPJCliente = Column(String(20))
        nCodCtr = Column(BigInteger)  # Alterado para BigInteger
        cNumCtr = Column(String(20))
        nCodOS = Column(BigInteger)  # Alterado para BigInteger
        cNumOS = Column(String(15))
        nCodCC = Column(BigInteger)  # Alterado para BigInteger
        cStatus = Column(String(100))
        cNatureza = Column(String(1))
        cTipo = Column(String(5))
        cOperacao = Column(String(2))
        cNumDocFiscal = Column(String(20))
        cCodCateg = Column(String(20))
        cNumParcela = Column(String(7))
        nValorTitulo = Column(DECIMAL)
        nValorPIS = Column(DECIMAL)
        cRetPIS = Column(String(1))
        nValorCOFINS = Column(DECIMAL)
        cRetCOFINS = Column(String(1))
        nValorCSLL = Column(DECIMAL)
        cRetCSLL = Column(String(1))
        nValorIR = Column(DECIMAL)
        cRetIR = Column(String(1))
        nValorISS = Column(DECIMAL)
        cRetISS = Column(String(1))
        nValorINSS = Column(DECIMAL)
        cRetINSS = Column(String(1))
        cCodProjeto = Column(BigInteger)  # Alterado para BigInteger
        observacao = Column(Text)
        cCodVendedor = Column(BigInteger)  # Alterado para BigInteger
        nCodComprador = Column(BigInteger)  # Alterado para BigInteger
        cCodigoBarras = Column(String(70))
        cNSU = Column(String(100))
        nCodNF = Column(BigInteger)  # Alterado para BigInteger
        dDtRegistro = Column(Date)
        cNumBoleto = Column(String(30))
        cChaveNFe = Column(String(44))
        cOrigem = Column(String(4))
        nCodTitRepet = Column(BigInteger)  # Alterado para BigInteger
        cGrupo = Column(String(20))
        nCodMovCC = Column(BigInteger)  # Alterado para BigInteger
        nValorMovCC = Column(DECIMAL)
        nCodMovCCRepet = Column(BigInteger)  # Alterado para BigInteger
        nDesconto = Column(DECIMAL)
        nJuros = Column(DECIMAL)
        nMulta = Column(DECIMAL)
        nCodBaixa = Column(BigInteger)  # Alterado para BigInteger
        dDtCredito = Column(Date)
        dDtConcilia = Column(Date)
        cHrConcilia = Column(String(8))
        cUsConcilia = Column(String(10))
        dDtInc = Column(Date)
        cHrInc = Column(String(8))
        cUsInc = Column(String(10))
        dDtAlt = Column(Date)
        cHrAlt = Column(String(8))
        cUsAlt = Column(String(10))
        resumo_cLiquidado = Column(String(1))
        resumo_nValPago = Column(DECIMAL)
        resumo_nValAberto = Column(DECIMAL)
        resumo_nDesconto = Column(DECIMAL)
        resumo_nJuros = Column(DECIMAL)
        resumo_nMulta = Column(DECIMAL)
        resumo_nValLiquido = Column(DECIMAL)

        def _init_(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)


    class Categoria(Base):
        __tablename__ = 'categorias'

        ID = Column(String(50), primary_key=True)
        codigo = Column(String(20))
        empresa = Column(String(50))
        descricao = Column(String(100))
        descricao_padrao = Column(String(100))
        tipo_categoria = Column(String(3))
        conta_inativa = Column(String(1))
        definida_pelo_usuario = Column(String(1))
        id_conta_contabil = Column(Integer)
        tag_conta_contabil = Column(String(20))
        conta_despesa = Column(String(1))
        conta_receita = Column(String(1))
        nao_exibir = Column(String(1))
        natureza = Column(String(500))
        totalizadora = Column(String(1))
        transferencia = Column(String(1))
        codigo_dre = Column(String(10))
        categoria_superior = Column(Text)
        dre_codigoDRE = Column(String(10))
        dre_descricaoDRE = Column(String(40))
        dre_naoExibirDRE = Column(String(1))
        dre_nivelDRE = Column(Integer)
        dre_sinalDRE = Column(String(1))
        dre_totalizaDRE = Column(String(1))

        def _init_(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    class Orcamento(Base):
        __tablename__ = 'orcamentos'

        ID = Column(String(50), primary_key=True)
        cCodCateg = Column(String(20))
        cDesCateg = Column(String(500))
        nValorPrevisto = Column(DECIMAL)
        nValorRealizado = Column(DECIMAL)
        nAno = Column(Integer)
        nMes = Column(Integer)
        empresa = Column(String(50))
        
        def _init_(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
                
    class Dre(Base):
        __tablename__ = 'dre'

        ID = Column(String(50), primary_key=True)
        codigoDRE = Column(String(20))
        descricaoDRE = Column(String(500))
        naoExibirDRE = Column(String(10))
        nivelDRE = Column(Integer)
        sinalDRE = Column(String(10))
        totalizaDRE = Column(String(10))
        empresa = Column(String(50))
        
        def _init_(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
    
    Base.metadata.create_all(bind=db)
criar_tabelas()
def carregar_dados(df, tabela, engine, **kwargs):
    """
    Carrega um DataFrame para uma tabela no banco de dados MySQL.

    Parâmetros:
    - df (pandas.DataFrame): DataFrame contendo os dados a serem inseridos.
    - tabela (str): Nome da tabela no banco de dados.
    - engine (sqlalchemy.engine.base.Engine): Objeto de conexão com o banco de dados.
    - **kwargs: Parâmetros opcionais (ex: dtinicio, dtfim, ano, mes para filtragem).

    Exemplo de uso:
    python
    carregar_dados(df, "movimentacoes", engine, dtinicio="2024-01-01", dtfim="2024-12-31")
    
    """

    dtinicio = kwargs.get("dtinicio")
    dtfim = kwargs.get("dtfim")
    ano = kwargs.get("ano")
    mes = kwargs.get("mes")
 
    # with engine.begin() as conn:
    #     conn.execute(text(f"DELETE FROM {tabela}"))

    # df.to_sql(name=tabela, con=engine, if_exists='append', index=False, chunksize=10000)

    with engine.begin() as conn:
        if tabela == 'movimentacoes' and dtinicio and dtfim:
            if dtinicio and dtfim:
                # Usa placeholders para evitar SQL Injection
                conn.execute(
                    text(f"DELETE FROM {tabela} WHERE dDtEmissao BETWEEN :dtinicio AND :dtfim"),
                    {"dtinicio": dtinicio, "dtfim": dtfim})
            else:
                conn.execute(text(f"DELETE FROM {tabela}"))    
            
        elif tabela == 'orcamentos' and ano and mes:
            # Usa placeholders para evitar SQL Injection
            conn.execute(
                 text(f"DELETE FROM {tabela} WHERE nAno = :ano AND nMes = :mes"),
                {"ano": ano, "mes": mes}
            )
        else:
            conn.execute(text(f"DELETE FROM {tabela}"))

    df.to_sql(name=tabela, con=engine, if_exists='append', index=False, chunksize=10000)