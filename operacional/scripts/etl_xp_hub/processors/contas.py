"""
Processador para arquivos de Contas (Ativações, Habilitações e Evasões) do Hub XP.
Processa 3 tipos de arquivos diferentes e empilha em uma única tabela.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
import os
from typing import Dict, Any, List
from sqlalchemy import text

# Configurações específicas do processador
NOME_TABELA = "xp_ativacoes_habilitacoes_evasoes"
LOAD_STRATEGY = LoadStrategy.APPEND
BATCH_SIZE = 5000

# Mapeamento de colunas
COLUMN_MAPPING = {
    "Conta": "cod_xp",
    "Assessor": "cod_aai",
    "Data": "data_ref",
    "Faixa": "faixa_pl",
}

# Colunas de texto a normalizar
TEXT_COLUMNS = ["cod_xp", "faixa_pl", "tipo_movimentacao"]

# Ordem final das colunas
COLUMN_ORDER = [
    "data_ref",
    "cod_xp",
    "cod_aai",
    "faixa_pl",
    "tipo_movimentacao",
    "data_carga",
]


def processar_contas(file_path: str) -> Tabela:
    """
    Processa arquivo de contas (ativações, habilitações ou evasões).

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Objeto Tabela processado
    """
    # Determina o tipo de movimentação baseado no nome do arquivo
    filename = os.path.basename(file_path).lower()

    if "contas_a_" in filename:
        tipo_movimentacao = "ativacao"
    elif "contas_h_" in filename:
        tipo_movimentacao = "habilitacao"
    elif "contas_e_" in filename:
        tipo_movimentacao = "evasao"
    else:
        tipo_movimentacao = "desconhecido"

    print(f"   📁 Tipo de movimentação detectado: {tipo_movimentacao}")

    # Cria instância da tabela
    tabela = Tabela(file_path)

    # Remove linhas com "Filtros aplicados:"
    if len(tabela.df) > 0:
        # Remove linhas que contêm "Filtros aplicados:" em qualquer coluna
        mask = ~tabela.df.apply(
            lambda row: row.astype(str)
            .str.contains("Filtros aplicados:", case=False)
            .any(),
            axis=1,
        )
        tabela.df = tabela.df[mask]

    # Remove coluna 'conta ativada' se for arquivo de habilitação
    if tipo_movimentacao == "habilitacao" and "conta ativada" in tabela.df.columns:
        tabela.df = tabela.df.drop("conta ativada", axis=1)

    # Renomeia colunas
    tabela.rename_columns(COLUMN_MAPPING)

    tabela.remove_last_rows(2)

    # Remove 'A' do início do cod_aai
    if "cod_aai" in tabela.df.columns:
        tabela.clean_column_code("cod_aai", "A")

    # Formata data_ref
    if "data_ref" in tabela.df.columns:
        tabela.format_date_columns(["data_ref"])

    # Adiciona coluna tipo_movimentacao
    tabela.df["tipo_movimentacao"] = tipo_movimentacao

    # Normaliza colunas de texto
    tabela.normalize_text_columns(TEXT_COLUMNS)

    # Adiciona data de carga
    tabela.add_processing_date()

    # Reordena colunas
    tabela.reorder_columns(COLUMN_ORDER)

    return tabela



def post_load_cleanup(df: pd.DataFrame, table_name: str, engine) -> Dict[str, Any]:
    """
    Remove registros do mesmo período (mês/ano) e tipo que tenham data_carga mais antiga.
    
    Args:
        df: DataFrame que foi inserido
        table_name: Nome completo da tabela
        engine: Engine do SQLAlchemy
        
    Returns:
        Dict com informações sobre a limpeza
    """
    try:
        # Verificar colunas necessárias
        if 'data_ref' not in df.columns or 'tipo_movimentacao' not in df.columns or 'data_carga' not in df.columns:
            print("⚠️ Colunas necessárias não encontradas. Pulando limpeza.")
            return {"status": "skipped", "reason": "missing required columns"}
        
        # Converter data_ref para datetime
        df['data_ref'] = pd.to_datetime(df['data_ref'])
        
        # Pegar a data_carga atual (deve ser a mesma para todos os registros inseridos)
        data_carga_atual = df['data_carga'].iloc[0]
        
        # Agrupar por período e tipo
        grupos = df.groupby(['tipo_movimentacao', df['data_ref'].dt.to_period('M')]).size()
        
        print(f"\n🧹 Removendo registros com data_carga anterior a {data_carga_atual}...")
        total_deleted = 0
        
        with engine.connect() as conn:
            with conn.begin():
                for (tipo, periodo), count in grupos.items():
                    ano = periodo.year
                    mes = periodo.month
                    
                    # Query para deletar registros do mesmo período/tipo com data_carga mais antiga
                    delete_query = text(f"""
                        DELETE FROM {table_name}
                        WHERE YEAR(data_ref) = :ano 
                        AND MONTH(data_ref) = :mes
                        AND tipo_movimentacao = :tipo
                        AND (data_carga < :data_carga_atual OR data_carga IS NULL)
                    """)
                    
                    result = conn.execute(delete_query, {
                        "ano": ano, 
                        "mes": mes, 
                        "tipo": tipo,
                        "data_carga_atual": data_carga_atual
                    })
                    deleted = result.rowcount
                    
                    if deleted > 0:
                        total_deleted += deleted
                        print(f"   🗑️ {tipo} - {ano}-{mes:02d}: {deleted} registros antigos removidos")
        
        print(f"   ✅ Limpeza concluída! Total removido: {total_deleted} registros\n")
        
        return {
            "status": "success",
            "groups_cleaned": len(grupos),
            "total_deleted": total_deleted
        }
        
    except Exception as e:
        print(f"❌ Erro na limpeza pós-carga: {str(e)}")
        return {"status": "error", "error": str(e)}


# Registrar função de pós-processamento
POST_LOAD_FUNCTION = post_load_cleanup


def processar_multiplos_contas(file_paths: List[str]) -> pd.DataFrame:
    """
    Processa múltiplos arquivos de contas e combina em um único DataFrame.

    Args:
        file_paths: Lista de caminhos dos arquivos

    Returns:
        DataFrame combinado
    """
    dfs = []

    for file_path in file_paths:
        try:
            tabela = processar_contas(file_path)
            dfs.append(tabela.get_data())
        except Exception as e:
            print(f"⚠️ Erro processando {os.path.basename(file_path)}: {e}")
            continue

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


# Função especial para processar os 3 arquivos juntos se necessário
def processar_contas_batch(file_info_list) -> pd.DataFrame:
    """
    Processa um batch de arquivos de contas (a, h, e) do mesmo período.
    Esta função pode ser usada pelo main.py para processar os 3 arquivos juntos.
    """
    file_paths = [fi.local_path for fi in file_info_list]
    return processar_multiplos_contas(file_paths)


if __name__ == "__main__":
    # Teste local do processador
    import sys

    if len(sys.argv) > 1:
        if len(sys.argv) == 2:
            # Processa um único arquivo
            file_path = sys.argv[1]
            print(f"Processando arquivo único: {file_path}")

            tabela = processar_contas(file_path)

            print(f"✅ Arquivo processado com sucesso!")
            print(f"📊 Total de registros: {len(tabela.df)}")
            print(f"📋 Colunas: {list(tabela.df.columns)}")
            print("\n🔍 Amostra dos dados:")
            print(tabela.df.head())
        else:
            # Processa múltiplos arquivos
            file_paths = sys.argv[1:]
            print(f"Processando {len(file_paths)} arquivos...")

            df_combined = processar_multiplos_contas(file_paths)

            if not df_combined.empty:
                print(f"✅ Arquivos processados e combinados!")
                print(f"📊 Total de registros: {len(df_combined)}")
                print(f"📋 Colunas: {list(df_combined.columns)}")
                print("\n📊 Contagem por tipo:")
                print(df_combined["tipo_movimentacao"].value_counts())
            else:
                print("❌ Nenhum dado processado")
    else:
        print("Uso: python contas.py <arquivo> [arquivo2] [arquivo3]")
