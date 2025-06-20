"""
Processador para arquivos de Positivador do Hub XP.
Processa dados de clientes com informações de cadastro, operações e receitas.
"""

from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
import numpy as np
import os

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


def add_custom_reference_date(tabela: Tabela, column_name: str = "data_ref") -> Tabela:
    """
    Adiciona coluna de data de referência customizada para positivador.
    
    - Se o arquivo termina com '_acumulado', mantém a coluna data_ref existente
    - Caso contrário, extrai a data do nome do arquivo
    
    Args:
        tabela: Objeto Tabela a ser processado
        column_name: Nome da coluna de data de referência
        
    Returns:
        Tabela com data de referência adicionada/processada
    """
    import os
    from datetime import datetime
    
    # Extrai nome do arquivo
    filename = os.path.basename(tabela.file_path)
    filename_no_ext = os.path.splitext(filename)[0]
    
    # Verifica se é arquivo acumulado
    if filename_no_ext.endswith("_acumulado"):
        # Para arquivos acumulados, a coluna data_ref deve existir no arquivo
        if column_name not in tabela.df.columns:
            # Se não existir, cria com data de hoje como fallback
            print(f"⚠️ Arquivo acumulado sem coluna {column_name}. Usando data atual.")
            tabela.df[column_name] = datetime.today().strftime("%Y-%m-%d")
        else:
            # Formata a coluna data_ref existente para garantir formato correto
            print(f"✅ Usando coluna {column_name} existente no arquivo acumulado.")
            # Mantém a coluna data_ref original do arquivo, mas garante que está formatada
            # Não renomeia, pois o mapeamento já foi aplicado antes
            pass
    else:
        # Para arquivos com data no nome, extrai do nome do arquivo
        try:
            # Tenta extrair data do final do nome (formato YYYY-MM-DD)
            date_str = filename_no_ext[-10:]  # Últimos 10 caracteres
            # Valida se é uma data válida
            pd.to_datetime(date_str, format="%Y-%m-%d")
            tabela.df[column_name] = date_str
            print(f"✅ Data de referência extraída do nome do arquivo: {date_str}")
        except:
            # Se falhar, usa data de hoje
            tabela.df[column_name] = datetime.today().strftime("%Y-%m-%d")
            print(f"⚠️ Não foi possível extrair data do nome. Usando data atual.")
    
    return tabela


def processar_positivador(file_path: str) -> Tabela:
    """
    Processa arquivo de positivador do Hub XP.

    Args:
        file_path: Caminho do arquivo Excel

    Returns:
        Objeto Tabela processado
    """
    # Cria instância da tabela
    tabela = Tabela(file_path)
    
    # Verifica se é arquivo acumulado e tem coluna data_ref
    filename = os.path.basename(file_path)
    filename_no_ext = os.path.splitext(filename)[0]
    is_acumulado = filename_no_ext.endswith("_acumulado")
    
    # Se for acumulado e tiver data_ref, adiciona ela na lista de colunas de data
    date_columns_to_format = DATE_COLUMNS.copy()
    if is_acumulado and "data_ref" in tabela.df.columns:
        date_columns_to_format.append("data_ref")
    
    # Aplica transformações
    tabela = (
        tabela
        .rename_columns(COLUMN_MAPPING)
        .normalize_text_columns(TEXT_COLUMNS)
        .format_date_columns(date_columns_to_format)
        .format_numeric_columns(NUMERIC_COLUMNS)
        .convert_boolean_columns(BOOLEAN_COLUMNS)
    )
    
    # IMPORTANTE: Valida limites decimais para evitar overflow no SQL Server
    # Especialmente importante para arquivos acumulados que podem ter valores muito grandes
    tabela = tabela.validate_decimal_limits(NUMERIC_COLUMNS, precision=16, scale=4)
    
    # Aplica método customizado de data de referência
    tabela = add_custom_reference_date(tabela)
    
    # Adiciona data de processamento
    tabela = tabela.add_processing_date()

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
