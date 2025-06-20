"""
Processador para arquivos de NPS Envios.
Processa dados de envios de pesquisas NPS (Net Promoter Score).
"""
from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
from typing import Dict, Any

# Configurações específicas do processador
NOME_TABELA = "xp_nps_envios"
LOAD_STRATEGY = LoadStrategy.APPEND  # Estratégia padrão - ajustar conforme necessário
BATCH_SIZE = 5000

# Mapeamento de colunas
COLUMN_MAPPING = {
    'Survey ID': 'survey_id',
    'Assessor': 'cod_assessor',
    'Customer ID': 'customer_id',
    'Código Escritório': 'codigo_escritorio',
    'Data de Entrega': 'data_entrega',
    'Data da Resposta': 'data_resposta',
    'Resposta via APP – NPS': 'resposta_app_nps',
    'Survey status': 'survey_status',
    'Pesquisa Relacionamento': 'pesquisa_relacionamento',
    'Email': 'email',
    'Invitation opened': 'invitation_opened',
    'Invitation opened date': 'invitation_opened_date',
    'Sampling result exclusion cause': 'sampling_exclusion_cause',
    'Survey start date': 'survey_start_date',
    'Last page seen': 'last_page_seen',
    'NPS in APP Survey ID Original': 'nps_app_survey_id_original'
}

# Colunas de texto a normalizar
TEXT_COLUMNS = [
    'survey_id',
    'cod_assessor',
    'customer_id',
    'codigo_escritorio',
    'resposta_app_nps',
    'survey_status',
    'pesquisa_relacionamento',
    'email',
    'invitation_opened',
    'sampling_exclusion_cause',
    'last_page_seen',
    'nps_app_survey_id_original'
]

# Colunas numéricas
NUMERIC_COLUMNS = []

# Colunas de data
DATE_COLUMNS = [
    'data_entrega',
    'data_resposta',
    'invitation_opened_date',
    'survey_start_date'
]

# Ordem final das colunas
COLUMN_ORDER = [
    'survey_id',
    'cod_assessor',
    'customer_id',
    'codigo_escritorio',
    'data_entrega',
    'data_resposta',
    'resposta_app_nps',
    'survey_status',
    'pesquisa_relacionamento',
    'email',
    'invitation_opened',
    'invitation_opened_date',
    'sampling_exclusion_cause',
    'survey_start_date',
    'last_page_seen',
    'nps_app_survey_id_original'
]


def processar_nps_envios(file_path: str) -> Tabela:
    """
    Processa arquivo de NPS Envios.
    
    Args:
        file_path: Caminho do arquivo Excel
        
    Returns:
        Objeto Tabela processado
    """
    print(f"📋 Processando NPS Envios: {file_path}")
    
    # Carrega o arquivo pulando as 2 primeiras linhas
    tabela = Tabela.__new__(Tabela)
    tabela.file_path = file_path
    tabela.original_columns = None
    
    # Lê o arquivo pulando as 2 primeiras linhas
    try:
        df = pd.read_excel(file_path, dtype=str, engine="openpyxl", skiprows=2)
        if df.empty:
            raise ValueError(f"Excel file is empty: {file_path}")
        tabela.df = df
        tabela.original_columns = list(df.columns)
    except Exception as e:
        raise ValueError(f"Error loading Excel file: {file_path} - {str(e)}")
    
    # Valida colunas obrigatórias
    tabela.validate_required_columns(list(COLUMN_MAPPING.keys()))
    
    # Remove linhas vazias
    tabela.remove_empty_rows()
    
    # Remove espaços em branco
    tabela.trim_text_columns()
    
    # Renomeia colunas conforme mapeamento
    tabela.rename_columns(COLUMN_MAPPING)
    
    # Remove 'A' do início do cod_assessor se existir
    if 'cod_assessor' in tabela.df.columns:
        tabela.clean_column_code('cod_assessor', 'A')
    
    # Normaliza colunas de texto
    tabela.normalize_text_columns(TEXT_COLUMNS)
    
    # Formata colunas de data
    tabela.format_date_columns(DATE_COLUMNS)
    
    # Reordena colunas
    tabela.reorder_columns(COLUMN_ORDER)
    
    print(f"✅ NPS Envios processado: {len(tabela.get_data())} linhas")
    return tabela


# Função de pré-processamento (executada ANTES da inserção)
# Descomentar e ajustar se necessário
# PRE_LOAD_FUNCTION = lambda df, table, engine: Tabela.filter_new_records_by_key(df, table, engine, 'chave_unica')


# Função de pós-processamento (executada APÓS a inserção)
# Descomentar e ajustar se necessário
# def post_load_cleanup(df: pd.DataFrame, table_name: str, engine) -> Dict[str, Any]:
#     """
#     Limpeza pós-carga para NPS Envios.
#     """
#     return Tabela.post_load_cleanup_by_period(df, table_name, engine, 'data_envio')
# 
# POST_LOAD_FUNCTION = post_load_cleanup


if __name__ == "__main__":
    # Teste local do processador
    import sys
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")
        
        tabela = processar_nps_envios(file_path)
        
        print(f"✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head())
        
        # Informações sobre o DataFrame
        print("\n📊 Informações do DataFrame:")
        print(tabela.df.info())
    else:
        print("Uso: python nps_envios.py <caminho_do_arquivo>")
