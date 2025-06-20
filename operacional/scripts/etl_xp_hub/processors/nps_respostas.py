"""
Processador para arquivos de NPS Respostas.
Processa dados de respostas de pesquisas NPS (Net Promoter Score).
"""
from processors.tabela import Tabela
from config.database_config import LoadStrategy
from datetime import datetime
import pandas as pd
from typing import Dict, Any

# Configurações específicas do processador
NOME_TABELA = "xp_nps_respostas"
LOAD_STRATEGY = LoadStrategy.APPEND  # Estratégia padrão - ajustar conforme necessário
BATCH_SIZE = 5000

# Mapeamento de colunas principais (as mais importantes)
COLUMN_MAPPING = {
    'Survey ID': 'survey_id',
    'Id do Usuário': 'id_usuario',
    'Customer ID': 'customer_id',
    'Data da Resposta': 'data_resposta',
    'Código Escritório': 'codigo_escritorio',
    'Faixa Net': 'faixa_net',
    'Area': 'area',
    'Canal': 'canal',
    'Suitability': 'suitability',
    'Area Transacional': 'area_transacional',
    'XP Assessores Survey Type': 'xp_assessores_survey_type',
    'Pesquisa Relacionamento': 'pesquisa_relacionamento',
    'XP - Relacionamento - Aniversário - NPS Assessor': 'xp_aniversario_nps_assessor',
    'XP - Relacionamento - Onboarding - NPS': 'xp_onboarding_nps',
    'Clear - Relacionamento - Onboarding - NPS': 'clear_onboarding_nps',
    'Rico - Relacionamento - Onboarding - NPS': 'rico_onboarding_nps',
    'XP - Relacionamento - Aniversário - NPS XP': 'xp_aniversario_nps_xp',
    'Clear - Relacionamento - Aniversário - NPS': 'clear_aniversario_nps',
    'Rico - Relacionamento - Aniversário - NPS': 'rico_aniversario_nps',
    'Transacional - Satisfação do Assessor': 'transacional_satisfacao_assessor',
    'Transacional - Comentário da Nota de Satisfação': 'transacional_comentario_satisfacao',
    'XP - Relacionamento - Onboarding - NPS Comentário': 'xp_onboarding_comentario',
    'Clear - Relacionamento - Onboarding - NPS Comentário': 'clear_onboarding_comentario',
    'Rico - Relacionamento - Onboarding - Comentário': 'rico_onboarding_comentario',
    'XP - Relacionamento - Aniversário - Comentário NPS Assessor': 'xp_aniversario_comentario_assessor',
    'XP - Relacionamento - Aniversário - Comentário XP': 'xp_aniversario_comentario_xp',
    'Clear - Relacionamento - Aniversário - Comentário': 'clear_aniversario_comentario',
    'Rico - Relacionamento - Aniversário - Comentário': 'rico_aniversario_comentario',
    'PLD': 'pld',
    'Faixa PLD': 'faixa_pld',
    'Delivered on date': 'delivered_on_date',
    'Gênero': 'genero',
    'Usuários Ativos': 'usuarios_ativos',
    'Cliente Max': 'cliente_max',
    'Cliente Max Simulação': 'cliente_max_simulacao',
    'Persona': 'persona',
    'Data de cadastro': 'data_cadastro',
    'Open Comment': 'open_comment',
    'Faixa de Atendimento': 'faixa_atendimento',
    'Grupo de Atendimento': 'grupo_atendimento',
    'Topics Tagged Original': 'topics_tagged_original',
    'Assessor': 'cod_assessor',
    'Unit Secundária Digital': 'unit_secundaria_digital',
    'First name': 'first_name',
    'Email': 'email',
    'Previdência Atual': 'previdencia_atual',
    'Receber Comparativo/Proposta': 'receber_comparativo_proposta',
    'Valor Previdência': 'valor_previdencia',
    # Colunas de Razão NPS
    'Clear - Relacionamento - Razão NPS': 'clear_razao_nps',
    'Rico - Relacionamento - Razão NPS': 'rico_razao_nps',
    'XP - Relacionamento - Razão NPS': 'xp_razao_nps',
    'Clear - Relacionamento - Aniversário - Razão NPS': 'clear_aniversario_razao_nps',
    'Clear - Relacionamento - Onboarding - Razão NPS': 'clear_onboarding_razao_nps',
    'Rico - Relacionamento - Aniversário - Razão NPS': 'rico_aniversario_razao_nps',
    'Rico - Relacionamento - Onboarding - Razão NPS': 'rico_onboarding_razao_nps',
    'XP - Relacionamento - Aniversário - Razão NPS Assessor': 'xp_aniversario_razao_nps_assessor',
    'XP - Relacionamento - Aniversário - Razão NPS XP': 'xp_aniversario_razao_nps_xp',
    'XP - Relacionamento - Onboarding - Razão NPS': 'xp_onboarding_razao_nps',
    # Colunas de Email e Recomendação
    'XP - Relacionamento - Aniversário - Email amigo/familiar': 'xp_aniversario_email_amigo_familiar',
    'XP - Relacionamento - Aniversário - Recomendaria Assessor': 'xp_aniversario_recomendaria_assessor',
    # Links de resposta
    'Link to Response': 'link_to_response',
    # Subatributos
    'Global - Relacionamento - Subatributo': 'global_relacionamento_subatributo',
    'Rico - Relacionamento - Subatributo': 'rico_relacionamento_subatributo'
}

# Colunas de texto a normalizar
TEXT_COLUMNS = [
    'survey_id', 'id_usuario', 'customer_id', 'codigo_escritorio',
    'faixa_net', 'area', 'canal', 'suitability', 'area_transacional',
    'xp_assessores_survey_type', 'pesquisa_relacionamento',
    'transacional_comentario_satisfacao', 'xp_onboarding_comentario',
    'clear_onboarding_comentario', 'rico_onboarding_comentario',
    'xp_aniversario_comentario_assessor', 'xp_aniversario_comentario_xp',
    'clear_aniversario_comentario', 'rico_aniversario_comentario',
    'pld', 'faixa_pld', 'genero', 'usuarios_ativos', 'cliente_max',
    'cliente_max_simulacao', 'persona', 'open_comment',
    'faixa_atendimento', 'grupo_atendimento', 'topics_tagged_original',
    'cod_assessor', 'unit_secundaria_digital', 'first_name', 'email',
    'previdencia_atual', 'receber_comparativo_proposta',
    # Novas colunas adicionadas
    'clear_razao_nps', 'rico_razao_nps', 'xp_razao_nps',
    'clear_aniversario_razao_nps', 'clear_onboarding_razao_nps',
    'rico_aniversario_razao_nps', 'rico_onboarding_razao_nps',
    'xp_aniversario_razao_nps_assessor', 'xp_aniversario_razao_nps_xp',
    'xp_onboarding_razao_nps', 'xp_aniversario_email_amigo_familiar',
    'xp_aniversario_recomendaria_assessor', 'link_to_response',
    'global_relacionamento_subatributo', 'rico_relacionamento_subatributo'
]

# Colunas numéricas (notas NPS)
NUMERIC_COLUMNS = [
    'xp_aniversario_nps_assessor',
    'xp_onboarding_nps',
    'clear_onboarding_nps',
    'rico_onboarding_nps',
    'xp_aniversario_nps_xp',
    'clear_aniversario_nps',
    'rico_aniversario_nps',
    'transacional_satisfacao_assessor'
]

# Colunas de data
DATE_COLUMNS = [
    'data_resposta',
    'delivered_on_date',
    'data_cadastro'
]

# Colunas monetárias
MONETARY_COLUMNS = [
    'valor_previdencia'
]

# Ordem final das colunas principais
COLUMN_ORDER = [
    'survey_id',
    'id_usuario',
    'customer_id',
    'cod_assessor',
    'codigo_escritorio',
    'data_resposta',
    'faixa_net',
    'faixa_pld',
    'area',
    'canal',
    'pesquisa_relacionamento',
    'xp_aniversario_nps_assessor',
    'xp_aniversario_recomendaria_assessor',  # Coluna importante adicionada
    'xp_onboarding_nps',
    'clear_onboarding_nps',
    'rico_onboarding_nps',
    'xp_aniversario_nps_xp',
    'clear_aniversario_nps',
    'rico_aniversario_nps',
    'transacional_satisfacao_assessor',
    'xp_aniversario_email_amigo_familiar',
    'link_to_response',
    'valor_previdencia'
]


def processar_nps_respostas(file_path: str) -> Tabela:
    """
    Processa arquivo de NPS Respostas.
    
    Args:
        file_path: Caminho do arquivo Excel
        
    Returns:
        Objeto Tabela processado
    """
    print(f"📋 Processando NPS Respostas: {file_path}")
    
    # Carrega o arquivo pulando as 2 primeiras linhas
    tabela = Tabela.__new__(Tabela)
    tabela.file_path = file_path
    tabela.original_columns = None
    
    # Lê o arquivo pulando as 2 primeiras linhas
    try:
        df = pd.read_excel(file_path, dtype=str, engine="openpyxl", skiprows=2)
        if df.empty:
            raise ValueError(f"Excel file is empty: {file_path}")
        tabela.df = df
        tabela.original_columns = list(df.columns)
    except Exception as e:
        raise ValueError(f"Error loading Excel file: {file_path} - {str(e)}")
    
    # Valida se pelo menos algumas colunas existem (não exige todas)
    colunas_encontradas = [col for col in COLUMN_MAPPING.keys() if col in tabela.df.columns]
    if len(colunas_encontradas) < 10:  # Pelo menos 10 colunas conhecidas
        raise ValueError(f"Arquivo não parece ser de NPS Respostas. Apenas {len(colunas_encontradas)} colunas reconhecidas.")
    
    # Remove linhas vazias
    tabela.remove_empty_rows()
    
    # Remove espaços em branco
    tabela.trim_text_columns()
    
    # Remove a coluna problemática com nome muito longo
    coluna_problema = 'Você gostaria responder mais duas questões para nos ajudar sobre o produto\nPrevidência? (Mesmo você não tendo previdência, poderá responder)'
    
    # Procura por colunas que começam com "Você gostaria responder" (para pegar com ou sem quebra de linha)
    colunas_remover = [col for col in tabela.df.columns if col.startswith('Você gostaria responder')]
    if colunas_remover:
        print(f"   🗑️ Removendo {len(colunas_remover)} coluna(s) problemática(s)")
        tabela.df = tabela.df.drop(columns=colunas_remover)
    
    # Tratamento especial para colunas duplicadas como 'Link to Response'
    # Se houver colunas duplicadas, o pandas adiciona .1, .2, etc.
    if 'Link to Response.1' in tabela.df.columns:
        tabela.df = tabela.df.rename(columns={'Link to Response.1': 'Link to Response 2'})
        COLUMN_MAPPING['Link to Response 2'] = 'link_to_response_2'
    
    # Renomeia apenas as colunas que existem no arquivo
    mapeamento_valido = {k: v for k, v in COLUMN_MAPPING.items() if k in tabela.df.columns}
    tabela.rename_columns(mapeamento_valido)
    
    # Remove colunas que não foram mapeadas (extras)
    colunas_mapeadas = list(mapeamento_valido.values())
    colunas_extras = [col for col in tabela.df.columns if col not in colunas_mapeadas]
    if colunas_extras:
        print(f"   ⚠️ Removendo {len(colunas_extras)} coluna(s) não mapeada(s): {colunas_extras[:3]}..." if len(colunas_extras) > 3 else f"   ⚠️ Removendo coluna(s) não mapeada(s): {colunas_extras}")
        tabela.df = tabela.df.drop(columns=colunas_extras)
    
    # Remove 'A' do início do cod_assessor se existir
    if 'cod_assessor' in tabela.df.columns:
        tabela.clean_column_code('cod_assessor', 'A')
    
    # Normaliza colunas de texto (apenas as que existem)
    text_cols_existentes = [col for col in TEXT_COLUMNS if col in tabela.df.columns]
    if text_cols_existentes:
        tabela.normalize_text_columns(text_cols_existentes)
    
    # Formata colunas numéricas (notas NPS) - apenas as que existem
    numeric_cols_existentes = [col for col in NUMERIC_COLUMNS if col in tabela.df.columns]
    if numeric_cols_existentes:
        tabela.format_numeric_columns(numeric_cols_existentes)
    
    # Formata colunas monetárias
    monetary_cols_existentes = [col for col in MONETARY_COLUMNS if col in tabela.df.columns]
    if monetary_cols_existentes:
        tabela.clean_monetary_columns(monetary_cols_existentes)
    
    # Formata colunas de data
    date_cols_existentes = [col for col in DATE_COLUMNS if col in tabela.df.columns]
    if date_cols_existentes:
        tabela.format_date_columns(date_cols_existentes)
    
    # Reordena apenas as colunas que existem
    colunas_existentes = [col for col in COLUMN_ORDER if col in tabela.df.columns]
    # Adiciona colunas extras que existem mas não estão na ordem definida
    colunas_extras = [col for col in tabela.df.columns if col not in colunas_existentes]
    ordem_final = colunas_existentes + colunas_extras
    tabela.reorder_columns(ordem_final)
    
    print(f"✅ NPS Respostas processado: {len(tabela.get_data())} linhas")
    return tabela


# Função de pré-processamento (executada ANTES da inserção)
# Descomentar e ajustar se necessário
# PRE_LOAD_FUNCTION = lambda df, table, engine: Tabela.filter_new_records_by_key(df, table, engine, 'id_resposta')


# Função de pós-processamento (executada APÓS a inserção)
# Descomentar e ajustar se necessário
# def post_load_cleanup(df: pd.DataFrame, table_name: str, engine) -> Dict[str, Any]:
#     """
#     Limpeza pós-carga para NPS Respostas.
#     Pode remover respostas duplicadas ou antigas do mesmo período.
#     """
#     return Tabela.post_load_cleanup_by_period(df, table_name, engine, 'data_resposta')
# 
# POST_LOAD_FUNCTION = post_load_cleanup


# Função auxiliar para cálculo de NPS (pode ser útil futuramente)
def calcular_nps_score(df: pd.DataFrame, coluna_nota: str) -> float:
    """
    Calcula o Net Promoter Score baseado nas notas.
    
    NPS = % Promotores - % Detratores
    
    Args:
        df: DataFrame com as respostas
        coluna_nota: Nome da coluna com as notas (0-10)
        
    Returns:
        Score NPS (-100 a 100)
    """
    if coluna_nota not in df.columns or len(df) == 0:
        return 0.0
    
    total = len(df)
    promotores = len(df[df[coluna_nota] >= 9])
    detratores = len(df[df[coluna_nota] <= 6])
    
    pct_promotores = (promotores / total) * 100
    pct_detratores = (detratores / total) * 100
    
    return round(pct_promotores - pct_detratores, 2)


if __name__ == "__main__":
    # Teste local do processador
    import sys
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Processando arquivo: {file_path}")
        
        tabela = processar_nps_respostas(file_path)
        
        print(f"✅ Arquivo processado com sucesso!")
        print(f"📊 Total de registros: {len(tabela.df)}")
        print(f"📋 Colunas: {list(tabela.df.columns)}")
        print("\n🔍 Amostra dos dados:")
        print(tabela.df.head())
        
        # Informações sobre o DataFrame
        print("\n📊 Informações do DataFrame:")
        print(tabela.df.info())
        
        # Calcula NPS para cada coluna de nota encontrada
        numeric_cols_no_df = [col for col in NUMERIC_COLUMNS if col in tabela.df.columns]
        if numeric_cols_no_df:
            print(f"\n📈 NPS Scores calculados:")
            for col_nps in numeric_cols_no_df:
                # Filtra apenas valores não nulos
                df_valido = tabela.df[tabela.df[col_nps].notna()]
                if len(df_valido) > 0:
                    nps = calcular_nps_score(df_valido, col_nps)
                    print(f"   {col_nps}: {nps} (base: {len(df_valido)} respostas)")
    else:
        print("Uso: python nps_respostas.py <caminho_do_arquivo>")
