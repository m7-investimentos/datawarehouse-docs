"""
Processador para arquivos de XPerformance Rentabilidade Cliente.
Processa dados de rentabilidade de clientes já formatados do Hub XP.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
from typing import Dict, Any

# Configurações específicas do processador
NOME_TABELA = "xperformance_rentabilidade_cliente"
LOAD_STRATEGY = LoadStrategy.APPEND
BATCH_SIZE = 5000


def processar_xperformance_rentabilidade_cliente(file_path: str) -> Tabela:
    """
    Processa arquivo de XPerformance Rentabilidade Cliente.

    Como o arquivo já vem formatado corretamente do S3, apenas:
    - Remove linhas vazias
    - Remove espaços extras (trim)
    - Remove linhas onde portfolio_rentabilidade é vazio/nulo
    - Retorna para inserção direta

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Objeto Tabela processado
    """
    # Cria instância da tabela
    tabela = Tabela(file_path)

    # Remove linhas completamente vazias (se houver)
    tabela.remove_empty_rows()

    # Remove espaços em branco extras das colunas de texto
    tabela.trim_text_columns()

    # Remove linhas onde portfolio_rentabilidade está vazio ou nulo
    if "portfolio_rentabilidade" in tabela.df.columns:
        registros_antes = len(tabela.df)

        # Remove linhas onde portfolio_rentabilidade é NaN, None, string vazia ou 'nan'
        tabela.df = tabela.df[
            ~tabela.df["portfolio_rentabilidade"].isna()
            & (tabela.df["portfolio_rentabilidade"] != "")
            & (tabela.df["portfolio_rentabilidade"].astype(str).str.lower() != "nan")
            & (tabela.df["portfolio_rentabilidade"].astype(str).str.lower() != "none")
        ]

        registros_removidos = registros_antes - len(tabela.df)
        if registros_removidos > 0:
            print(
                f"   🗑️ Removidas {registros_removidos} linhas com portfolio_rentabilidade vazio/nulo"
            )
    else:
        print(f"   ⚠️ Coluna 'portfolio_rentabilidade' não encontrada no arquivo")

    # Como o arquivo já está formatado, não precisamos de transformações adicionais
    print(f"   📊 Colunas encontradas: {list(tabela.df.columns)}")
    print(f"   📊 Total de registros finais: {len(tabela.df)}")

    return tabela


# Se houver necessidade de limpar dados antigos do mesmo período,
# descomente e ajuste a função abaixo:
"""
def post_load_cleanup(df: pd.DataFrame, table_name: str, engine) -> Dict[str, Any]:
    # Implementar lógica de limpeza se necessário
    # Por exemplo: remover dados do mesmo período antes de inserir novos
    return {"status": "success", "message": "Sem limpeza necessária"}

POST_LOAD_FUNCTION = post_load_cleanup
"""


if __name__ == "__main__":
    # Teste local do processador
    import sys

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")

        tabela = processar_xperformance_rentabilidade_cliente(file_path)

        print(f"\n✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros finais: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print(f"📋 Tipos de dados:")
        print(tabela.df.dtypes)
        print("\n🔍 Amostra dos dados (primeiras 5 linhas):")
        print(tabela.df.head())

        # Mostra informações adicionais se disponíveis
        info = tabela.info()
        print(f"\n📊 Informações do DataFrame:")
        print(f"   Shape: {info['shape']}")
        print(f"   Valores nulos por coluna:")
        for col, nulls in info["null_counts"].items():
            if nulls > 0:
                print(f"   - {col}: {nulls} valores nulos")

        # Verifica especificamente a coluna portfolio_rentabilidade
        if "portfolio_rentabilidade" in tabela.df.columns:
            print(f"\n📊 Estatísticas de portfolio_rentabilidade:")
            print(
                f"   - Valores não-nulos: {tabela.df['portfolio_rentabilidade'].notna().sum()}"
            )
            print(
                f"   - Valores únicos: {tabela.df['portfolio_rentabilidade'].nunique()}"
            )
    else:
        print("Uso: python xperformance_rentabilidade_cliente.py <caminho_do_arquivo>")
