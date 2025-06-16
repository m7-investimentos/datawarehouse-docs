"""
Processador para arquivos de Positivador do Hub XP.
Processa dados de clientes com informações de cadastro, operações e receitas.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
import numpy as np

# Configurações específicas do processador
NOME_TABELA = "xp_positivador"
LOAD_STRATEGY = LoadStrategy.APPEND
BATCH_SIZE = 5000

# Mapeamento de colunas do Excel para o banco
COLUMN_MAPPING = {
    "Assessor": "cod_aai",
    "Cliente": "cod_xp",
    "Profissão": "profissao",
    "Sexo": "sexo",
    "Segmento": "segmento",
    "Data de Cadastro": "data_cadastro",
    "Fez Segundo Aporte?": "fez_segundo_aporte",
    "Data de Nascimento": "data_nascimento",
    "Status": "status_cliente",
    "Ativou em M?": "ativou_em_M",
    "Evadiu em M?": "evadiu_em_M",
    "Operou Bolsa?": "operou_bolsa",
    "Operou Fundo?": "operou_fundo",
    "Operou Renda Fixa?": "operou_renda_fixa",
    "Aplicação Financeira Declarada": "aplicacao_financeira_declarada",
    "Receita no Mês": "receita_mes",
    "Receita Bovespa": "receita_bovespa",
    "Receita Futuros": "receita_futuros",
    "Receita RF Bancários": "receita_rf_bancarios",
    "Receita RF Privados": "receita_rf_privados",
    "Receita RF Públicos": "receita_rf_publicos",
    "Captação Bruta em M": "captacao_bruta_em_M",
    "Resgate em M": "resgate_em_M",
    "Captação Líquida em M": "captacao_liquida_em_M",
    "Captação TED": "captacao_TED",
    "Captação ST": "captacao_ST",
    "Captação OTA": "captacao_OTA",
    "Captação RF": "captacao_RF",
    "Captação TD": "captacao_TD",
    "Captação PREV": "captacao_PREV",
    "Net em M-1": "net_em_M_1",
    "Net Em M": "net_em_M",
    "Renda Fixa": "net_renda_fixa",
    "Fundos Imobiliários": "net_fundos_imobiliarios",
    "Renda Variável": "net_renda_variavel",
    "Fundos": "net_fundos",
    "Financeiro": "net_financeiro",
    "Previdência": "net_previdencia",
    "Outros": "net_outros",
    "Valor Receita Aluguel": "receita_aluguel",
}

# Colunas por tipo de tratamento
TEXT_COLUMNS = ["cod_aai", "cod_xp", "profissao", "sexo", "segmento"]
BOOLEAN_COLUMNS = [
    "fez_segundo_aporte",
    "ativou_em_M",
    "evadiu_em_M",
    "operou_bolsa",
    "operou_fundo",
    "operou_renda_fixa",
    "status_cliente",
]
DATE_COLUMNS = ["data_cadastro", "data_nascimento"]
NUMERIC_COLUMNS = [
    "aplicacao_financeira_declarada",
    "receita_mes",
    "receita_bovespa",
    "receita_futuros",
    "receita_rf_bancarios",
    "receita_rf_privados",
    "receita_rf_publicos",
    "captacao_bruta_em_M",
    "resgate_em_M",
    "captacao_liquida_em_M",
    "captacao_TED",
    "captacao_ST",
    "captacao_OTA",
    "captacao_RF",
    "captacao_TD",
    "captacao_PREV",
    "net_em_M_1",
    "net_em_M",
    "net_renda_fixa",
    "net_fundos_imobiliarios",
    "net_renda_variavel",
    "net_fundos",
    "net_financeiro",
    "net_previdencia",
    "net_outros",
    "receita_aluguel",
]


def processar_positivador(file_path: str) -> Tabela:
    """
    Processa arquivo de positivador do Hub XP.

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Objeto Tabela processado
    """
    # Cria instância da tabela e aplica transformações encadeadas
    tabela = (
        Tabela(file_path)
        .rename_columns(COLUMN_MAPPING)
        .normalize_text_columns(TEXT_COLUMNS)
        .format_date_columns(DATE_COLUMNS)
        .format_numeric_columns(NUMERIC_COLUMNS)
        .convert_boolean_columns(BOOLEAN_COLUMNS)
        .add_reference_date()
        .add_processing_date()
    )

    # Reordena colunas (data_ref primeiro)
    column_order = ["data_ref"] + list(COLUMN_MAPPING.values()) + ["data_carga"]
    tabela.reorder_columns(column_order)

    return tabela


if __name__ == "__main__":
    # Teste local do processador
    import sys

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")

        tabela = processar_positivador(file_path)

        print(f"✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head())
    else:
        print("Uso: python positivador.py <caminho_do_arquivo>")
