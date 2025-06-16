"""
Processador para arquivos de IEA (Índice de Eficiência do Assessor) do Hub XP.
Processa dados mensais com métricas de performance dos assessores.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
import os

# Configurações específicas do processador
NOME_TABELA = "xp_iea"
LOAD_STRATEGY = LoadStrategy.APPEND
BATCH_SIZE = 5000

# Mapeamento de colunas
COLUMN_MAPPING = {
    "Ano/Mês": "ano_mes",
    "Código AAI": "cod_assessor",
    "IEA final": "iea_final",
    "Captação Líquida": "captacao_liquida",
    "Esforco Prospecção": "esforco_prospeccao",
    "Captação de Novos Clientes por AAI": "captacao_de_novos_clientes_por_aai",
    "Atingimento Lead Starts": "atingimento_lead_starts",
    "Atingimento Habilitações": "atingimento_habilitacoes",
    "Atingimento Conversão": "atingimento_conversao",
    "Atingimento Carteiras Simuladas Novos": "atingimento_carteiras_simuladas_novos",
    "Esforco Relacionamento": "esforco_relacionamento",
    "Captação da Base por AAI": "captacao_da_base",
    "Atingimento Contas Aportaram": "atingimento_contas_aportarem",
    "Atingimento Ordens Enviadas": "atingimento_ordens_enviadas",
    "Atingimento Contas Acessadas Hub": "atingimento_contas_acessadas_hub"
}

# Colunas a remover
COLUMNS_TO_DROP = ["Código Matriz", "Matriz"]

# Colunas numéricas
NUMERIC_COLUMNS = [
    "iea_final",
    "captacao_liquida",
    "esforco_prospeccao",
    "captacao_de_novos_clientes_por_aai",
    "atingimento_lead_starts",
    "atingimento_habilitacoes",
    "atingimento_conversao",
    "atingimento_carteiras_simuladas_novos",
    "esforco_relacionamento",
    "captacao_da_base",
    "atingimento_contas_aportarem",
    "atingimento_ordens_enviadas",
    "atingimento_contas_acessadas_hub"
]

# Ordem final das colunas
COLUMN_ORDER = list(COLUMN_MAPPING.values())


def processar_iea(file_path: str) -> Tabela:
    """
    Processa arquivo de IEA do Hub XP.
    
    Args:
        file_path: Caminho do arquivo Excel
        
    Returns:
        Objeto Tabela processado
    """
    print(f"   📁 Processando IEA: {os.path.basename(file_path)}")
    
    # Cria instância da tabela
    tabela = Tabela(file_path)
    
    # Remove as últimas 2 linhas (geralmente totais)
    tabela.remove_last_rows(2)
    
    # Remove colunas desnecessárias
    for col in COLUMNS_TO_DROP:
        if col in tabela.df.columns:
            tabela.df = tabela.df.drop(col, axis=1)
    
    # Renomeia colunas
    tabela.rename_columns(COLUMN_MAPPING)
    
    # Remove 'A' do início do cod_assessor
    if "cod_assessor" in tabela.df.columns:
        tabela.clean_column_code("cod_assessor", "A")
    
    # Formata colunas numéricas
    tabela.format_numeric_columns(NUMERIC_COLUMNS)
    
    # Reordena colunas
    tabela.reorder_columns(COLUMN_ORDER)
    
    print(f"   ✅ Total de registros: {len(tabela.df)}")
    
    return tabela


if __name__ == "__main__":
    # Teste local do processador
    import sys
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")
        
        try:
            tabela = processar_iea(file_path)
            
            print(f"✅ Arquivo processado com sucesso!")
            print(f"📊 Total de registros: {len(tabela.df)}")
            print(f"📋 Colunas: {list(tabela.df.columns)}")
            print("\n🔍 Amostra dos dados:")
            print(tabela.df.head())
            
            # Mostra estatísticas
            if "ano_mes" in tabela.df.columns:
                print(f"\n📈 Estatísticas:")
                print(f"   Períodos únicos: {tabela.df['ano_mes'].nunique()}")
                print(f"   Assessores únicos: {tabela.df['cod_assessor'].nunique()}")
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("Uso: python iea.py <caminho_do_arquivo>")
