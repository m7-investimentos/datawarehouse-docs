"""
Processador para arquivos de Captação do Hub XP.
Trata dados acumulados com lógica especial para substituir dados do mesmo período.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
from sqlalchemy import text
from typing import Optional, Dict, Any

# Configurações específicas do processador
NOME_TABELA = "xp_captacao"
LOAD_STRATEGY = LoadStrategy.APPEND  # Usamos APPEND pois temos lógica customizada
BATCH_SIZE = 5000

# Mapeamento de colunas
COLUMN_MAPPING = {
    "Data": "data_ref",
    "Assessor": "cod_aai",
    "Cód do Cliente": "cod_xp",
    "Tipo de Captação": "tipo_de_captacao",
    "Aux": "sinal_captacao",
    "Captação": "valor_captacao",
}

# Colunas a normalizar (texto)
TEXT_COLUMNS = ["cod_xp", "cod_aai", "tipo_de_captacao"]

# Colunas numéricas
NUMERIC_COLUMNS = ["valor_captacao"]

# Ordem final das colunas
COLUMN_ORDER = [
    "data_ref",
    "cod_xp",
    "cod_aai",
    "tipo_de_captacao",
    "sinal_captacao",
    "valor_captacao",
    "data_carga",
]


def processar_captacao(file_path: str) -> Tabela:
    """
    Processa arquivo de captação do Hub XP.

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Objeto Tabela processado
    """
    # Cria instância da tabela
    tabela = Tabela(file_path)

    # Remove coluna Escritório se existir
    if "Escritório" in tabela.df.columns:
        tabela.df = tabela.df.drop("Escritório", axis=1)

    # Renomeia colunas
    tabela.rename_columns(COLUMN_MAPPING)

    # Normaliza colunas de texto (lowercase)
    tabela.normalize_text_columns(TEXT_COLUMNS)

    # Formata data_ref
    if "data_ref" in tabela.df.columns:
        tabela.format_date_columns(["data_ref"])

    # Processa sinal_captacao (C = 1, D = -1)
    if "sinal_captacao" in tabela.df.columns:
        tabela.df["sinal_captacao"] = tabela.df["sinal_captacao"].apply(
            process_sinal_captacao
        )

    # Formata colunas numéricas
    tabela.format_numeric_columns(NUMERIC_COLUMNS)

    # Adiciona data de processamento
    tabela.add_processing_date()

    # Reordena colunas
    tabela.reorder_columns(COLUMN_ORDER)

    return tabela


def process_sinal_captacao(valor: Any) -> int:
    """
    Converte sinal de captação: C = 1, D = -1, outros = 0
    """
    if pd.isna(valor):
        return 0

    valor_upper = str(valor).strip().upper()
    if valor_upper == "C":
        return 1
    elif valor_upper == "D":
        return -1
    else:
        return 0


# Registrar função de pós-processamento diretamente
# Esta função será chamada automaticamente pelo loader após inserção bem-sucedida
POST_LOAD_FUNCTION = Tabela.post_load_cleanup_by_period


if __name__ == "__main__":
    # Teste local do processador
    import sys

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")

        tabela = processar_captacao(file_path)

        print(f"✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head())

        # Mostrar períodos únicos
        if "data_ref" in tabela.df.columns:
            periodos = pd.to_datetime(tabela.df["data_ref"]).dt.to_period("M").unique()
            print(f"\n📅 Períodos no arquivo: {[str(p) for p in periodos]}")
    else:
        print("Uso: python captacao.py <caminho_do_arquivo>")
