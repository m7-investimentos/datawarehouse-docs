"""
Processador para arquivos de Transferência de Clientes do Hub XP.
Processa dados de transferências de clientes entre assessores.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
from typing import Dict, Any

# Configurações específicas do processador
NOME_TABELA = "xp_transferencia_clientes"
LOAD_STRATEGY = LoadStrategy.APPEND  # Apenas adiciona novos dados
BATCH_SIZE = 5000

# Mapeamento de colunas
COLUMN_MAPPING = {
    "Status": "status",
    "Código Assessor Origem": "cod_aai_origem",
    "Código Assessor Destino": "cod_aai_destino",
    "Data Solicitação": "data_solicitacao",
    "Data Transferência": "data_transferencia",
    "Código do Cliente": "cod_xp",
}

# Colunas a remover
COLUMNS_TO_DROP = ["Código Solicitação"]

# Colunas de texto a normalizar
TEXT_COLUMNS = ["status", "cod_aai_origem", "cod_aai_destino", "cod_xp"]

# Colunas de data
DATE_COLUMNS = ["data_solicitacao", "data_transferencia"]

# Ordem final das colunas
COLUMN_ORDER = [
    "status",
    "cod_aai_origem",
    "cod_aai_destino",
    "data_solicitacao",
    "data_transferencia",
    "cod_xp",
    "data_carga",
]


def processar_transferencia_clientes(file_path: str) -> Tabela:
    """
    Processa arquivo de transferência de clientes do Hub XP.

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Objeto Tabela processado
    """
    # Cria instância da tabela
    tabela = Tabela(file_path)

    # Remove colunas desnecessárias
    for col in COLUMNS_TO_DROP:
        if col in tabela.df.columns:
            tabela.df = tabela.df.drop(col, axis=1)

    # Renomeia colunas conforme mapeamento
    tabela.rename_columns(COLUMN_MAPPING)

    # Remove 'A' do início dos códigos de assessor
    if "cod_aai_origem" in tabela.df.columns:
        tabela.clean_column_code("cod_aai_origem", "A")

    if "cod_aai_destino" in tabela.df.columns:
        tabela.clean_column_code("cod_aai_destino", "A")

    # Remove caracteres não alfanuméricos dos códigos
    if "cod_aai_origem" in tabela.df.columns:
        tabela.df["cod_aai_origem"] = (
            tabela.df["cod_aai_origem"]
            .astype(str)
            .str.replace(r"[^a-zA-Z0-9]", "", regex=True)
        )

    if "cod_aai_destino" in tabela.df.columns:
        tabela.df["cod_aai_destino"] = (
            tabela.df["cod_aai_destino"]
            .astype(str)
            .str.replace(r"[^a-zA-Z0-9]", "", regex=True)
        )

    # Formata colunas de data
    tabela.format_date_columns(DATE_COLUMNS)

    # Normaliza colunas de texto
    tabela.normalize_text_columns(TEXT_COLUMNS)

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

        tabela = processar_transferencia_clientes(file_path)

        print(f"✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head())
    else:
        print("Uso: python transferencia_clientes.py <caminho_do_arquivo>")
