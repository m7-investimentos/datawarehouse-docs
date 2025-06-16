"""
Processador para arquivos de Open Investment Extrato do Hub XP.
Processa dados de extrato de investimentos com informações de produtos e valores.
"""
from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
from typing import Dict, Any

# Configurações específicas do processador
NOME_TABELA = "xp_open_investment_extrato"
LOAD_STRATEGY = LoadStrategy.APPEND  # Apenas adiciona novos dados
BATCH_SIZE = 5000

# Mapeamento de colunas
COLUMN_MAPPING = {
    'Segmento': 'segmento',
    'Cód. Assessor': 'cod_assessor',
    'Cód. Conta': 'cod_conta',
    'Cód. Matriz': 'cod_matriz',
    'Instituição Bancária': 'instituicao_bancaria',
    'Produtos': 'produtos',
    'Sub Produtos': 'sub_produtos',
    'Ativo': 'ativo',
    'Valor Bruto': 'valor_bruto',
    'Valor Líquido': 'valor_liquido'
}

# Colunas de texto a normalizar
TEXT_COLUMNS = [
    'segmento', 'cod_assessor', 'cod_conta', 'cod_matriz',
    'instituicao_bancaria', 'produtos', 'sub_produtos', 'ativo'
]

# Colunas numéricas
NUMERIC_COLUMNS = ['valor_bruto', 'valor_liquido']

# Ordem final das colunas
COLUMN_ORDER = list(COLUMN_MAPPING.values()) + ['data_carga']

def processar_oi_extrato(file_path: str) -> Tabela:
    """
    Processa arquivo de Open Investment Extrato do Hub XP.
    
    Args:
        file_path: Caminho do arquivo Excel
        
    Returns:
        Objeto Tabela processado
    """
    # Cria instância da tabela
    tabela = Tabela(file_path)
    
    # Remove as últimas 2 linhas (geralmente contém totais ou filtros)
    tabela.remove_last_rows(2)
    
    # Renomeia colunas conforme mapeamento
    tabela.rename_columns(COLUMN_MAPPING)
    
    # Remove 'A' do início do cod_assessor se existir
    if 'cod_assessor' in tabela.df.columns:
        tabela.clean_column_code('cod_assessor', 'A')
    
    # Normaliza colunas de texto
    tabela.normalize_text_columns(TEXT_COLUMNS)
    
    # Formata colunas numéricas
    tabela.format_numeric_columns(NUMERIC_COLUMNS)
    
    # Adiciona data de carga
    tabela.add_processing_date()
    
    # Reordena colunas
    tabela.reorder_columns(COLUMN_ORDER)
    
    return tabela


if __name__ == "__main__":
    # Teste local do processador
    import sys
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")
        
        tabela = processar_oi_extrato(file_path)
        
        print(f"✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head())
    else:
        print("Uso: python oi_extrato.py <caminho_do_arquivo>")
