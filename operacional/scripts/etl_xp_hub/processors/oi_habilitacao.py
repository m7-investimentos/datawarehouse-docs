"""
Processador para arquivos de Open Finance (OI Habilitação) do Hub XP.
Trata dados acumulados de habilitação Open Finance com lógica de substituição por período.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
from typing import Dict, Any

# Configurações específicas do processador
NOME_TABELA = "xp_open_investment_habilitacao"
LOAD_STRATEGY = LoadStrategy.APPEND  # Usamos APPEND pois temos lógica customizada
BATCH_SIZE = 5000

# Mapeamento de colunas
COLUMN_MAPPING = {
    "Mês": "ano_mes",
    "Data Permissão": "data_permissao",
    "Cód. Conta": "cod_xp",
    "Tipo Pessoa": "tipo_conta",
    "Cód. Assessor": "cod_aai",
    "Status Termo": "status_termo",
    "Instituição Bancária": "instituicao",
    "SoW": "sow",
    "AuC": "auc",
    "AuC Atual": "auc_atual",
    "Grupo Clientes": "grupo_clientes",
    "Sugestão Estratégia": "sugestao_estrategia",
}

# Colunas a remover
COLUMNS_TO_DROP = ["Cód. Matriz", "Segmento", "Descrição Estratégia"]

# Colunas de texto a normalizar
TEXT_COLUMNS = [
    "cod_xp",
    "tipo_conta",
    "cod_aai",
    "status_termo",
    "instituicao",
    "grupo_clientes",
    "sugestao_estrategia",
]

# Colunas numéricas
NUMERIC_COLUMNS = ["sow", "auc", "auc_atual"]

# Ordem final das colunas
COLUMN_ORDER = [
    "ano_mes",
    "data_permissao",
    "cod_xp",
    "tipo_conta",
    "cod_aai",
    "status_termo",
    "instituicao",
    "sow",
    "auc",
    "auc_atual",
    "grupo_clientes",
    "sugestao_estrategia",
    "data_carga",
]


def processar_oi_habilitacao(file_path: str) -> Tabela:
    """
    Processa arquivo de Open Finance (OI Habilitação) do Hub XP.

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Objeto Tabela processado
    """
    # Cria instância da tabela
    tabela = Tabela(file_path)

    # Remove as últimas 2 linhas (conforme código original)
    if len(tabela.df) >= 2:
        tabela.df = tabela.df.iloc[:-2]

    # Remove colunas desnecessárias
    for col in COLUMNS_TO_DROP:
        if col in tabela.df.columns:
            tabela.df = tabela.df.drop(col, axis=1)

    # Renomeia colunas
    tabela.rename_columns(COLUMN_MAPPING)

    # Remove linhas onde data_permissao é nula
    if "data_permissao" in tabela.df.columns:
        tabela.df = tabela.df[tabela.df["data_permissao"].notna()]

    # Processa data_permissao (formato YYYYMMDD para DATE)
    if "data_permissao" in tabela.df.columns:
        tabela.df["data_permissao"] = pd.to_datetime(
            tabela.df["data_permissao"].astype(str), format="%Y%m%d", errors="coerce"
        )

    # Remove 'A' do início do cod_aai
    if "cod_aai" in tabela.df.columns:
        tabela.clean_column_code("cod_aai", "A")

    # Extrai apenas números do cod_xp
    if "cod_xp" in tabela.df.columns:
        tabela.df["cod_xp"] = tabela.df["cod_xp"].astype(str).str.extract(r"(\d+)")[0]

    # Formata colunas numéricas e preenche nulos com 0
    for col in NUMERIC_COLUMNS:
        if col in tabela.df.columns:
            tabela.df[col] = pd.to_numeric(tabela.df[col], errors="coerce").fillna(0)
            # Aplicar precisões específicas
            if col == "sow":
                tabela.df[col] = tabela.df[col].round(6)
            else:  # auc e auc_atual
                tabela.df[col] = tabela.df[col].round(2)

    # Preenche valores nulos em colunas VARCHAR com string vazia
    text_fill_columns = ["instituicao", "grupo_clientes", "sugestao_estrategia"]
    for col in text_fill_columns:
        if col in tabela.df.columns:
            tabela.df[col] = tabela.df[col].fillna("")

    # Normaliza colunas de texto
    tabela.normalize_text_columns(TEXT_COLUMNS)

    # Adiciona data de processamento
    tabela.add_processing_date()

    # CORREÇÃO: Garantir que ano_mes permaneça como string no formato YYYYMM
    if "ano_mes" in tabela.df.columns:
        # Se a coluna já existe, garantir que está no formato string YYYYMM
        tabela.df["ano_mes"] = tabela.df["ano_mes"].astype(str).str.strip()

        # Validar formato (deve ter 6 dígitos)
        invalid_mask = ~tabela.df["ano_mes"].str.match(r"^\d{6}$")
        if invalid_mask.any():
            print(
                f"⚠️ Aviso: {invalid_mask.sum()} registros com ano_mes em formato inválido"
            )
            # Tentar corrigir formatos comuns
            # Ex: 2025-06 -> 202506
            tabela.df.loc[invalid_mask, "ano_mes"] = (
                tabela.df.loc[invalid_mask, "ano_mes"]
                .str.replace("-", "")
                .str.replace("/", "")
            )
    else:
        # Se não existir ano_mes, criar baseado na data atual no formato YYYYMM
        current_date = datetime.now()
        tabela.df["ano_mes"] = current_date.strftime("%Y%m")

    # Reordena colunas
    tabela.reorder_columns(COLUMN_ORDER)

    return tabela


# Registrar função de pós-processamento
# Usa um lambda para ajustar a ordem dos parâmetros
POST_LOAD_FUNCTION = lambda engine, table_name, df: Tabela.post_load_cleanup_by_period(
    df, table_name, engine, date_column="ano_mes"
)


if __name__ == "__main__":
    # Teste local do processador
    import sys

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")

        tabela = processar_oi_habilitacao(file_path)

        print(f"✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head())

        # Mostrar períodos únicos
        if "ano_mes" in tabela.df.columns:
            periodos = tabela.df["ano_mes"].unique()
            print(f"\n📅 Períodos no arquivo: {list(periodos)}")

            # Validar formato
            for periodo in periodos:
                if not str(periodo).isdigit() or len(str(periodo)) != 6:
                    print(f"   ⚠️ Período em formato inválido: {periodo}")
    else:
        print("Uso: python oi_habilitacao.py <caminho_do_arquivo>")
