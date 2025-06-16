"""
Processador para arquivos de RPA Clientes do Hub XP.
Processa dados de clientes com informações de patrimônio e elegibilidade.
"""
from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
from typing import Dict, Any

# Configurações específicas do processador
NOME_TABELA = "xp_rpa_clientes"
LOAD_STRATEGY = LoadStrategy.APPEND  # Usa APPEND mas com filtragem customizada
BATCH_SIZE = 5000

# Colunas de texto a normalizar (assumindo que existem no arquivo)
TEXT_COLUMNS = ["elegibilidade_cartao", "status_conta_digital", "produto", "email_cliente"]

# Colunas monetárias que precisam limpeza especial
MONETARY_COLUMNS = ["patrimonio"]

# Coluna chave para verificar duplicatas
KEY_COLUMN = "cod_xp"

def processar_rpa_clientes(file_path: str) -> Tabela:
    """
    Processa arquivo de RPA Clientes do Hub XP.
    
    Args:
        file_path: Caminho do arquivo Excel
        
    Returns:
        Objeto Tabela processado
    """
    # Cria instância da tabela
    tabela = Tabela(file_path)
    
    # Limpa e formata colunas monetárias usando método da classe Tabela
    tabela.clean_monetary_columns(MONETARY_COLUMNS)
    
    # Normaliza colunas de texto
    tabela.normalize_text_columns(TEXT_COLUMNS)
    
    # Adiciona apenas data de carga (não adiciona data_ref)
    tabela.add_processing_date()
    
    return tabela



# Registrar função de pré-processamento diretamente
# Esta função será chamada ANTES da inserção
PRE_LOAD_FUNCTION = lambda df, table, engine: Tabela.filter_new_records_by_key(df, table, engine, KEY_COLUMN)


if __name__ == "__main__":
    # Teste local do processador
    import sys
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")
        
        tabela = processar_rpa_clientes(file_path)
        
        print(f"✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head())
        
        # Mostra estatísticas da coluna patrimônio se existir
        if 'patrimonio' in tabela.df.columns:
            print(f"\n💰 Estatísticas de patrimônio:")
            print(f"   Soma total: R$ {tabela.df['patrimonio'].sum():,.2f}")
            print(f"   Média: R$ {tabela.df['patrimonio'].mean():,.2f}")
            print(f"   Mediana: R$ {tabela.df['patrimonio'].median():,.2f}")
    else:
        print("Uso: python rpa_clientes.py <caminho_do_arquivo>")
