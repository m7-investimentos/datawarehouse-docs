#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Pipeline Completo - ETLs + Procedures Bronze to Metadata
================================================================================
Tipo: Script Pipeline
Versão: 1.0.0
Última atualização: 2025-01-17
Autor: bruno.chiaramonti@multisete.com
Revisor: arquitetura.dados@m7investimentos.com.br
Tags: [pipeline, etl, procedures, bronze, metadata]
Status: produção
Python: 3.8+
================================================================================

OBJETIVO:
    Executar o pipeline completo de performance:
    1. ETLs do Google Sheets para Bronze
    2. Procedures de Bronze para Metadata

CASOS DE USO:
    1. Execução diária automatizada completa
    2. Reprocessamento após mudanças estruturais
    3. Sincronização completa manual

EXEMPLOS DE USO:
    # Execução padrão
    python run_full_pipeline.py
    
    # Modo debug
    python run_full_pipeline.py --debug
    
    # Apenas ETLs (sem procedures)
    python run_full_pipeline.py --only-etls
    
    # Apenas procedures (sem ETLs)
    python run_full_pipeline.py --only-procedures
"""

# ==============================================================================
# 1. IMPORTS
# ==============================================================================
import os
import sys
import logging
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import pyodbc
from dotenv import load_dotenv

# ==============================================================================
# 2. CONFIGURAÇÕES E CONSTANTES
# ==============================================================================

# Diretórios
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / 'logs'
CREDENTIALS_DIR = BASE_DIR / 'credentials'
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Carregar variáveis de ambiente do arquivo .env no diretório credentials
load_dotenv(CREDENTIALS_DIR / '.env')

# Configuração de logging
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] [PIPELINE] %(message)s'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Configuração do banco
DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
}

# Definição das procedures e ordem de execução
PROCEDURES = [
    {
        'name': 'prc_bronze_to_metadata_indicators',
        'schema': 'metadata',
        'description': 'Processa indicadores de Bronze para Metadata',
        'parameters': {
            '@debug': 1
        }
    },
    {
        'name': 'prc_bronze_to_metadata_assignments',
        'schema': 'metadata',
        'description': 'Processa atribuições de Bronze para Metadata',
        'parameters': {
            '@validate_weights': 1,
            '@debug': 1
        }
    },
    {
        'name': 'prc_bronze_to_metadata_performance_targets',
        'schema': 'metadata',
        'description': 'Processa metas de Bronze para Metadata',
        'parameters': {
            '@validate_completeness': 1,
            '@debug': 1
        }
    }
]

# ==============================================================================
# 3. CONFIGURAÇÃO DE LOGGING
# ==============================================================================

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    """Configura o sistema de logging."""
    logger = logging.getLogger('PIPELINE')
    logger.setLevel(LOG_LEVEL)
    
    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(console_handler)
    
    # Handler para arquivo
    if log_file:
        file_handler = logging.FileHandler(LOG_DIR / log_file)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(file_handler)
    
    return logger

# ==============================================================================
# 4. FUNÇÕES PRINCIPAIS
# ==============================================================================

def get_db_connection():
    """Cria conexão com o banco de dados."""
    driver = DB_CONFIG['driver'].strip('{}')
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['user']};"
        f"PWD={DB_CONFIG['password']};"
        f"TrustServerCertificate=yes"
    )
    return pyodbc.connect(conn_str, timeout=30)

def run_etls(logger: logging.Logger, debug: bool = False) -> int:
    """
    Executa os ETLs usando o orquestrador.
    
    Args:
        logger: Logger configurado
        debug: Se True, modo debug
        
    Returns:
        Código de retorno (0 = sucesso)
    """
    logger.info("\n" + "="*60)
    logger.info("FASE 1: EXECUTANDO ETLs")
    logger.info("="*60)
    
    # Usar Python 3.11
    python_cmd = '/opt/homebrew/bin/python3.11' if os.path.exists('/opt/homebrew/bin/python3.11') else 'python3.11'
    cmd = [python_cmd, 'run_all_etls.py']
    if debug:
        cmd.append('--debug')
        
    try:
        process = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            capture_output=True,
            text=True
        )
        
        if process.returncode == 0:
            logger.info("ETLs executados com sucesso")
            return 0
        else:
            logger.error(f"ETLs falharam: {process.stderr}")
            return process.returncode
            
    except Exception as e:
        logger.error(f"Erro ao executar ETLs: {e}")
        return 1

def execute_procedure(conn: pyodbc.Connection, proc_def: Dict, 
                     logger: logging.Logger, debug: bool = False) -> bool:
    """
    Executa uma stored procedure.
    
    Args:
        conn: Conexão com banco
        proc_def: Definição da procedure
        logger: Logger configurado
        debug: Se True, modo debug
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        
        # Construir comando EXEC
        proc_name = f"[{proc_def['schema']}].[{proc_def['name']}]"
        params = []
        
        for param, value in proc_def.get('parameters', {}).items():
            if param == '@debug':
                value = 1 if debug else 0
            params.append(f"{param} = {value}")
            
        exec_cmd = f"EXEC {proc_name}"
        if params:
            exec_cmd += " " + ", ".join(params)
            
        logger.info(f"Executando: {exec_cmd}")
        
        # Executar procedure
        cursor.execute(exec_cmd)
        
        # Capturar mensagens
        while cursor.nextset():
            pass
            
        conn.commit()
        logger.info(f"Procedure {proc_def['name']} executada com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao executar procedure {proc_def['name']}: {e}")
        conn.rollback()
        return False

def run_procedures(logger: logging.Logger, debug: bool = False) -> int:
    """
    Executa as procedures de Bronze para Metadata.
    
    Args:
        logger: Logger configurado
        debug: Se True, modo debug
        
    Returns:
        Código de retorno (0 = sucesso)
    """
    logger.info("\n" + "="*60)
    logger.info("FASE 2: EXECUTANDO PROCEDURES")
    logger.info("="*60)
    
    try:
        conn = get_db_connection()
        logger.info("Conexão com banco estabelecida")
        
        success_count = 0
        
        for proc_def in PROCEDURES:
            logger.info(f"\n{proc_def['description']}...")
            
            if execute_procedure(conn, proc_def, logger, debug):
                success_count += 1
            else:
                logger.error(f"Falha na procedure {proc_def['name']}")
                if success_count == 0:  # Se primeira procedure falhou, parar
                    logger.error("Procedure crítica falhou - parando execução")
                    break
                    
        conn.close()
        
        if success_count == len(PROCEDURES):
            logger.info(f"\nTodas as {success_count} procedures executadas com sucesso")
            return 0
        else:
            logger.error(f"\n{len(PROCEDURES) - success_count} procedures falharam")
            return 1
            
    except Exception as e:
        logger.error(f"Erro ao executar procedures: {e}")
        return 1

def check_prerequisites(logger: logging.Logger) -> bool:
    """
    Verifica pré-requisitos antes de executar o pipeline.
    
    Args:
        logger: Logger configurado
        
    Returns:
        True se todos os pré-requisitos foram atendidos
    """
    logger.info("Verificando pré-requisitos...")
    
    # Verificar variáveis de ambiente
    missing_vars = []
    for var in ['DB_SERVER', 'DB_DATABASE', 'DB_USERNAME', 'DB_PASSWORD']:
        if not os.getenv(var):
            missing_vars.append(var)
            
    if missing_vars:
        logger.error(f"Variáveis de ambiente faltando: {', '.join(missing_vars)}")
        return False
        
    # Verificar conexão com banco
    try:
        conn = get_db_connection()
        conn.close()
        logger.info("✓ Conexão com banco de dados OK")
    except Exception as e:
        logger.error(f"✗ Erro ao conectar ao banco: {e}")
        return False
        
    # Verificar arquivos necessários
    required_files = [
        'run_all_etls.py',
        'etl_001_indicators.py',
        'etl_002_assignments.py'
    ]
    
    for file in required_files:
        if not (BASE_DIR / file).exists():
            logger.error(f"✗ Arquivo necessário não encontrado: {file}")
            return False
            
    logger.info("✓ Todos os pré-requisitos atendidos")
    return True

# ==============================================================================
# 5. FUNÇÃO PRINCIPAL
# ==============================================================================

def main():
    """Função principal do pipeline."""
    # Parse argumentos
    parser = argparse.ArgumentParser(
        description='Pipeline Completo - ETLs + Procedures',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Ativa modo debug com logs detalhados'
    )
    
    parser.add_argument(
        '--only-etls',
        action='store_true',
        help='Executar apenas ETLs (sem procedures)'
    )
    
    parser.add_argument(
        '--only-procedures',
        action='store_true',
        help='Executar apenas procedures (sem ETLs)'
    )
    
    parser.add_argument(
        '--skip-prerequisites',
        action='store_true',
        help='Pular verificação de pré-requisitos'
    )
    
    args = parser.parse_args()
    
    # Configurar logging
    log_filename = f"PIPELINE_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logging(log_filename)
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        
    try:
        start_time = datetime.now()
        
        logger.info("="*80)
        logger.info("PIPELINE COMPLETO - PERFORMANCE INDICATORS & ASSIGNMENTS")
        logger.info(f"Início: {start_time}")
        logger.info("="*80)
        
        # Verificar pré-requisitos
        if not args.skip_prerequisites:
            if not check_prerequisites(logger):
                logger.error("Pré-requisitos não atendidos - abortando")
                sys.exit(1)
                
        exit_code = 0
        
        # Fase 1: ETLs
        if not args.only_procedures:
            etl_result = run_etls(logger, args.debug)
            if etl_result != 0:
                logger.error("ETLs falharam - abortando pipeline")
                exit_code = etl_result
            else:
                logger.info("ETLs concluídos com sucesso")
        else:
            logger.info("Pulando ETLs (--only-procedures)")
            
        # Fase 2: Procedures
        if exit_code == 0 and not args.only_etls:
            proc_result = run_procedures(logger, args.debug)
            if proc_result != 0:
                logger.error("Procedures falharam")
                exit_code = proc_result
            else:
                logger.info("Procedures concluídas com sucesso")
        elif args.only_etls:
            logger.info("Pulando procedures (--only-etls)")
            
        # Resumo final
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("RESUMO DO PIPELINE")
        logger.info("="*80)
        logger.info(f"Status: {'SUCESSO' if exit_code == 0 else 'FALHA'}")
        logger.info(f"Duração total: {duration:.2f} segundos")
        logger.info(f"Log salvo em: {LOG_DIR / log_filename}")
        logger.info("="*80)
        
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)

# ==============================================================================
# 6. EXECUÇÃO
# ==============================================================================

if __name__ == '__main__':
    main()