"""
Processamento de Diversificação usando classe Tabela

Este arquivo demonstra como usar a classe Tabela genérica
para processar arquivos de diversificação.
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from utils.helpers import executar_etl_completo, validar_arquivo_existe

# Configurações específicas para diversificação
MAPEAMENTO_COLUNAS = {
    "Data": "data_ref",
    "Cliente": "cod_xp",
    "Assessor": "cod_aai",
    "Produto": "produto",
    "Sub Produto": "sub_produto",
    "Produto em Garantia": "produto_em_garantia",
    "CNPJ Fundo": "CNPJ_fundo",
    "Ativo": "ativo",
    "Emissor": "emissor",
    "Data de Vencimento": "data_de_vencimento",
    "Quantidade": "quantidade",
    "NET": "NET",
}

COLUNAS_TEXTO = [
    "cod_xp",
    "cod_aai",
    "produto",
    "sub_produto",
    "produto_em_garantia",
    "ativo",
    "emissor",
]
COLUNAS_DATA = ["data_ref", "data_de_vencimento"]
COLUNAS_NUMERICAS = ["quantidade", "NET"]
ORDEM_COLUNAS = list(MAPEAMENTO_COLUNAS.values()) + ["data_carga"]

# Configurações específicas do processador
NOME_TABELA = "xp_diversificacao"
LOAD_STRATEGY = LoadStrategy.APPEND
BATCH_SIZE = 5000


def processar_diversificacao(file_path: str) -> Tabela:
    """
    Processa arquivo de diversificação usando classe Tabela.

    Esta função contém as transformações específicas para diversificação,
    mas usa a classe Tabela genérica para executá-las.

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Instância de Tabela processada
    """
    print(f"📋 Processando diversificação: {os.path.basename(file_path)}")

    # Cria instância da classe Tabela e aplica transformações específicas
    diversificacao = (
        Tabela(file_path)
        .validate_required_columns(list(MAPEAMENTO_COLUNAS.keys()))
        .remove_empty_rows()
        .trim_text_columns()
        .rename_columns(MAPEAMENTO_COLUNAS)
        .normalize_text_columns(COLUNAS_TEXTO)
        .format_date_columns(COLUNAS_DATA)
        .format_numeric_columns(COLUNAS_NUMERICAS)
        .clean_cnpj_column("CNPJ_fundo")
        .clean_column_code("cod_aai", "A")
        .add_processing_date()
        .reorder_columns(ORDEM_COLUNAS)
    )

    print(f"✅ Diversificação processada: {len(diversificacao.get_data())} linhas")
    return diversificacao


def executar_diversificacao_completo(file_path: str) -> dict:
    """
    Executa processamento completo de diversificação usando helper genérico.

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Relatório de execução
    """
    # Valida arquivo
    validar_arquivo_existe(file_path)

    # Usa helper genérico para ETL completo
    return executar_etl_completo(
        file_path=file_path,
        funcao_processamento=processar_diversificacao,
        nome_tabela=NOME_TABELA,
        load_strategy=LOAD_STRATEGY,
        batch_size=BATCH_SIZE,
    )


# Execução direta
if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1].strip('"').strip("'")
    else:
        file_path = input("📁 Caminho do arquivo Excel: ").strip().strip('"').strip("'")

    try:
        relatorio = executar_diversificacao_completo(file_path)
        print(f"\n🎉 Processamento concluído com sucesso!")

    except Exception as e:
        print(f"\n💥 ERRO: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
