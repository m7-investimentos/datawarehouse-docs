"""
Funções auxiliares genéricas para ETL

Este módulo contém funções utilitárias que seguem o princípio de 
responsabilidade única e podem ser reutilizadas por diferentes processadores.
"""

from processors.tabela import Tabela
from loaders.sql_server_loader import SQLServerLoader
from config.database_config import get_database_config, TableConfig, LoadStrategy
from datetime import datetime
import os


def inserir_tabela_no_banco(tabela: Tabela, nome_tabela: str, load_strategy: LoadStrategy = LoadStrategy.TRUNCATE_LOAD, batch_size: int = 5000, pre_load_func=None) -> dict:
    """
    Insere dados de uma instância Tabela no banco de dados.
    
    Função genérica que pode ser usada por qualquer processador.
    
    Args:
        tabela: Instância de Tabela processada
        nome_tabela: Nome da tabela no banco (sem schema)
        load_strategy: Estratégia de carregamento
        batch_size: Tamanho do lote para inserção
        pre_load_func: Função opcional para filtrar dados antes da inserção
        
    Returns:
        Resultado da inserção
    """
    print(f"💾 Inserindo no banco: {nome_tabela}")
    
    # Configura tabela se necessário
    db_config = get_database_config()
    if nome_tabela not in db_config.table_configs:
        config = TableConfig(
            name=nome_tabela,
            load_strategy=load_strategy,
            batch_size=batch_size
        )
        db_config.add_table_config(nome_tabela, config)
    
    # Obtém dados para inserir
    df_to_insert = tabela.get_data()
    
    # Aplica pré-processamento se fornecido
    if pre_load_func is not None:
        loader = SQLServerLoader()
        engine = loader._get_engine()
        full_table_name = db_config.get_full_table_name(nome_tabela)
        df_to_insert = pre_load_func(df_to_insert, full_table_name, engine)
        
        # Se o DataFrame filtrado estiver vazio, retorna sem inserir
        if len(df_to_insert) == 0:
            return {
                "rows_inserted": 0,
                "status": "skipped",
                "reason": "no new records to insert"
            }
    
    # Insere no banco
    loader = SQLServerLoader()
    resultado = loader.load_data(df_to_insert, nome_tabela)
    
    print(f"✅ Inserido no banco: {resultado['rows_inserted']} linhas")
    return resultado


def gerar_relatorio_processamento(file_path: str, tabela: Tabela, resultado_banco: dict, inicio: datetime, tabela_destino: str) -> dict:
    """
    Gera relatório padronizado de processamento ETL.
    
    Args:
        file_path: Caminho do arquivo original
        tabela: Instância de Tabela processada
        resultado_banco: Resultado da inserção no banco
        inicio: Timestamp de início do processamento
        tabela_destino: Nome completo da tabela de destino
        
    Returns:
        Relatório estruturado
    """
    fim = datetime.now()
    duracao = (fim - inicio).total_seconds()
    
    relatorio = {
        "arquivo": os.path.basename(file_path),
        "linhas_processadas": len(tabela.get_data()),
        "linhas_inseridas": resultado_banco["rows_inserted"],
        "duracao_segundos": duracao,
        "tabela_destino": tabela_destino,
        "estrategia": resultado_banco.get("strategy", "unknown"),
        "status": "sucesso"
    }
    
    return relatorio


def imprimir_relatorio(relatorio: dict):
    """
    Imprime relatório de processamento de forma padronizada.
    
    Args:
        relatorio: Dicionário com dados do relatório
    """
    print(f"\n📊 RELATÓRIO:")
    print(f"   📁 Arquivo: {relatorio['arquivo']}")
    print(f"   📊 Linhas processadas: {relatorio['linhas_processadas']:,}")
    print(f"   💾 Linhas inseridas: {relatorio['linhas_inseridas']:,}")
    print(f"   ⏱️ Duração: {relatorio['duracao_segundos']:.2f}s")
    print(f"   🎯 Tabela: {relatorio['tabela_destino']}")
    print(f"   🔄 Estratégia: {relatorio['estrategia']}")


def executar_etl_completo(file_path: str, funcao_processamento, nome_tabela: str, load_strategy: LoadStrategy = LoadStrategy.TRUNCATE_LOAD, batch_size: int = 5000) -> dict:
    """
    Executa um ETL completo genérico: processa arquivo + insere no banco.
    
    Esta função coordena todo o fluxo ETL de forma genérica, permitindo
    que diferentes processadores usem a mesma lógica de orquestração.
    
    Args:
        file_path: Caminho do arquivo a processar
        funcao_processamento: Função que processa o arquivo e retorna Tabela
        nome_tabela: Nome da tabela de destino (sem schema)
        load_strategy: Estratégia de carregamento
        batch_size: Tamanho do lote
        
    Returns:
        Relatório de execução
        
    Example:
        relatorio = executar_etl_completo(
            "arquivo.xlsx",
            processar_diversificacao,
            "tb_diversificacao"
        )
    """
    inicio = datetime.now()
    
    try:
        print(f"🚀 Iniciando ETL: {os.path.basename(file_path)}")
        
        # 1. Processa arquivo usando função específica
        tabela = funcao_processamento(file_path)
        
        # 2. Insere no banco usando função genérica
        resultado_banco = inserir_tabela_no_banco(tabela, nome_tabela, load_strategy, batch_size)
        
        # 3. Gera e imprime relatório
        schema = get_database_config().schema
        tabela_destino = f"{schema}.{nome_tabela}"
        relatorio = gerar_relatorio_processamento(file_path, tabela, resultado_banco, inicio, tabela_destino)
        imprimir_relatorio(relatorio)
        
        return relatorio
        
    except Exception as e:
        print(f"❌ Erro no ETL: {e}")
        raise


def validar_arquivo_existe(file_path: str) -> bool:
    """
    Valida se arquivo existe e é acessível.
    
    Args:
        file_path: Caminho do arquivo
        
    Returns:
        True se arquivo existe
        
    Raises:
        FileNotFoundError: Se arquivo não existir
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    if not file_path.lower().endswith('.xlsx'):
        raise ValueError(f"Arquivo deve ser .xlsx: {file_path}")
    
    return True


def configurar_tabela_padrao(nome_tabela: str, load_strategy: LoadStrategy, batch_size: int = 5000):
    """
    Configura uma tabela no database config se não existir.
    
    Args:
        nome_tabela: Nome da tabela
        load_strategy: Estratégia de carregamento
        batch_size: Tamanho do lote
    """
    db_config = get_database_config()
    
    if nome_tabela not in db_config.table_configs:
        config = TableConfig(
            name=nome_tabela,
            load_strategy=load_strategy,
            batch_size=batch_size
        )
        db_config.add_table_config(nome_tabela, config)
        print(f"⚙️ Configuração criada para tabela: {nome_tabela}")
