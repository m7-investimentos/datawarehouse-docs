"""
Processador para dados históricos do CDI do Banco Central.
Busca dados diretamente da API do BCB para cálculos de rentabilidade.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime, timedelta
import pandas as pd
import requests
from typing import Optional, Dict, Any
from sqlalchemy import text

# Configurações específicas do processador
NOME_TABELA = "bc_cdi_historico"
LOAD_STRATEGY = LoadStrategy.APPEND
BATCH_SIZE = 5000

# Mapeamento de colunas da API para o banco
COLUMN_MAPPING = {
    "data": "data_ref",
    "valor": "taxa_cdi"
}

# Colunas numéricas
NUMERIC_COLUMNS = ["taxa_cdi"]

# Ordem final das colunas
COLUMN_ORDER = ["data_ref", "taxa_cdi"]


def consulta_bc(data_ini: str = "01/07/2023", data_fin: Optional[str] = None, base: int = 12) -> Optional[pd.DataFrame]:
    """
    Consulta API do Banco Central para obter dados do CDI.
    
    Args:
        data_ini: Data inicial no formato DD/MM/AAAA (padrão: 01/07/2023)
        data_fin: Data final no formato DD/MM/AAAA (padrão: ontem)
        base: Código da série no BCB (12 = CDI)
    
    Returns:
        DataFrame com os dados ou None se houver erro
    """
    # Se não especificar data final, usa ontem
    if data_fin is None:
        ontem = datetime.now() - timedelta(days=1)
        data_fin = ontem.strftime('%d/%m/%Y')
    try:
        url = f'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{base}/dados?formato=json&dataInicial={data_ini}&dataFinal={data_fin}'
        
        print(f"   🌐 Consultando API do BCB: {data_ini} até {data_fin}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        df = pd.read_json(response.text)
        
        if df.empty:
            print("   ⚠️ Nenhum dado retornado pela API")
            return None
            
        print(f"   ✅ {len(df)} registros obtidos da API")
        return df
        
    except Exception as e:
        print(f"   ❌ Erro ao consultar API do BCB: {e}")
        return None


def obter_periodo_consulta(engine) -> tuple[str, str]:
    """
    Determina o período de consulta: últimos 30 dias.
    
    Returns:
        Tupla com (data_inicial, data_final) no formato DD/MM/AAAA
    """
    hoje = datetime.now()
    
    # Data inicial: 30 dias antes de hoje
    data_ini = hoje - timedelta(days=30)
    
    # Data final: ontem
    data_fin = hoje - timedelta(days=1)
    
    # Formata para o padrão da API
    return (
        data_ini.strftime('%d/%m/%Y'),
        data_fin.strftime('%d/%m/%Y')
    )


# Registrar função de pré-processamento
# Esta função será chamada ANTES da inserção para filtrar apenas datas novas
PRE_LOAD_FUNCTION = lambda df, table, engine: filter_existing_dates(df, table, engine)


def filter_existing_dates(df: pd.DataFrame, table_name: str, engine) -> pd.DataFrame:
    """
    Filtra DataFrame para conter apenas datas que não existem no banco.
    
    Args:
        df: DataFrame a ser filtrado
        table_name: Nome completo da tabela no banco
        engine: Engine do SQLAlchemy
        
    Returns:
        DataFrame filtrado com apenas datas novas
    """
    try:
        from sqlalchemy import text
        
        if 'data_ref' not in df.columns:
            print("⚠️ Coluna data_ref não encontrada. Retornando todos os registros.")
            return df
            
        # Busca todas as datas existentes no banco
        with engine.connect() as conn:
            query = text(f"SELECT DISTINCT data_ref FROM {table_name}")
            result = conn.execute(query)
            existing_dates = {pd.to_datetime(row[0]) for row in result if row[0] is not None}
        
        print(f"\n🔍 Verificando datas existentes...")
        print(f"   Datas no banco: {len(existing_dates)}")
        print(f"   Datas no DataFrame: {len(df)}")
        
        # Converte data_ref para datetime para comparação
        df['data_ref'] = pd.to_datetime(df['data_ref'])
        
        # Filtra apenas as datas novas
        df_filtered = df[~df['data_ref'].isin(existing_dates)]
        
        print(f"   Novas datas a inserir: {len(df_filtered)}")
        
        if len(df_filtered) == 0:
            print("   ℹ️ Nenhuma data nova encontrada.")
        else:
            # Mostra range de datas novas
            print(f"   📅 Período novo: {df_filtered['data_ref'].min().strftime('%d/%m/%Y')} até {df_filtered['data_ref'].max().strftime('%d/%m/%Y')}")
        
        return df_filtered
        
    except Exception as e:
        # Se a tabela não existir, retorna todos os dados
        if "Invalid object name" in str(e) or "does not exist" in str(e):
            print("   ℹ️ Tabela não existe ainda. Inserindo todos os registros.")
            return df
        else:
            print(f"⚠️ Erro ao filtrar datas: {str(e)}")
            print("   Continuando com todos os registros...")
            return df


def processar_bc_cdi_historico(file_path: Optional[str] = None) -> Optional[Tabela]:
    """
    Processa dados históricos do CDI direto da API do Banco Central.
    
    Args:
        file_path: Ignorado - mantido para compatibilidade com outros processors
        
    Returns:
        Objeto Tabela processado ou None se não houver dados novos
    """
    print("📈 Processando dados do CDI (Banco Central)")
    
    # Para determinar o período, precisamos do engine
    try:
        from loaders.sql_server_loader import SQLServerLoader
        loader = SQLServerLoader()
        engine = loader._get_engine()
        
        # Determina período de consulta
        data_ini, data_fin = obter_periodo_consulta(engine)
        print(f"   📅 Período: {data_ini} até {data_fin}")
        
    except Exception as e:
        print(f"   ⚠️ Erro ao conectar no banco, buscando últimos 30 dias: {e}")
        hoje = datetime.now()
        data_ini = (hoje - timedelta(days=30)).strftime('%d/%m/%Y')
        data_fin = (hoje - timedelta(days=1)).strftime('%d/%m/%Y')
    
    # Consulta API
    df = consulta_bc(data_ini, data_fin)
    
    if df is None or df.empty:
        print("   ℹ️ Nenhum dado novo para processar")
        return None
    
    # Cria tabela com DataFrame
    tabela = Tabela(dataframe=df)
    
    # Aplica transformações
    tabela.rename_columns(COLUMN_MAPPING)
    
    # Converte data de string para datetime
    if "data_ref" in tabela.df.columns:
        tabela.df["data_ref"] = pd.to_datetime(tabela.df["data_ref"], format="%d/%m/%Y")
    
    # Formata coluna numérica (garante que taxa está como float)
    tabela.format_numeric_columns(NUMERIC_COLUMNS)
    
    # Converte taxa de porcentagem para decimal se necessário
    # (API retorna 0.045513 que já está em formato decimal correto)
    
    # Reordena colunas
    tabela.reorder_columns(COLUMN_ORDER)
    
    # Remove duplicatas por data (caso existam)
    tabela.df = tabela.df.drop_duplicates(subset=['data_ref'], keep='last')
    
    print(f"   📊 Total de registros a inserir: {len(tabela.df)}")
    
    return tabela


def processar_periodo_especifico(data_inicial: str, data_final: str) -> Optional[Tabela]:
    """
    Função auxiliar para processar um período específico.
    Útil para backfill ou correções.
    
    Args:
        data_inicial: Data inicial no formato DD/MM/AAAA
        data_final: Data final no formato DD/MM/AAAA
    """
    print(f"📈 Processando CDI para período específico: {data_inicial} até {data_final}")
    
    df = consulta_bc(data_inicial, data_final)
    
    if df is None or df.empty:
        return None
        
    tabela = Tabela(dataframe=df)
    
    # Mesmas transformações do processamento normal
    tabela.rename_columns(COLUMN_MAPPING)
    
    if "data_ref" in tabela.df.columns:
        tabela.df["data_ref"] = pd.to_datetime(tabela.df["data_ref"], format="%d/%m/%Y")
    
    tabela.format_numeric_columns(NUMERIC_COLUMNS)
    tabela.reorder_columns(COLUMN_ORDER)
    tabela.df = tabela.df.drop_duplicates(subset=['data_ref'], keep='last')
    
    return tabela


if __name__ == "__main__":
    # Teste local do processador
    import sys
    
    if len(sys.argv) > 2:
        # Modo período específico
        data_ini = sys.argv[1]
        data_fin = sys.argv[2]
        print(f"Buscando CDI de {data_ini} até {data_fin}")
        
        tabela = processar_periodo_especifico(data_ini, data_fin)
    else:
        # Modo normal (incremental)
        print("Buscando CDI incremental...")
        tabela = processar_bc_cdi_historico()
    
    if tabela:
        print(f"\n✅ Dados processados com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head(10))
        
        # Estatísticas
        print(f"\n📈 Estatísticas:")
        print(f"   Período: {tabela.df['data_ref'].min()} até {tabela.df['data_ref'].max()}")
        print(f"   Taxa média: {tabela.df['taxa_cdi'].mean():.6f}")
        print(f"   Taxa mínima: {tabela.df['taxa_cdi'].min():.6f}")
        print(f"   Taxa máxima: {tabela.df['taxa_cdi'].max():.6f}")
    else:
        print("\n⚠️ Nenhum dado para processar")
