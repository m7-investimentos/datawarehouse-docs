#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Pipeline ETL - Google Sheets para Bronze
================================================================================
Tipo: Script Pipeline
Versão: 2.0.0
Última atualização: 2025-01-23
Autor: bruno.chiaramonti@multisete.com
Revisor: arquitetura.dados@m7investimentos.com.br
Tags: [pipeline, etl, bronze, google-sheets]
Status: produção
Python: 3.8+
================================================================================

OBJETIVO:
    Executar ETLs do Google Sheets para Bronze com gerenciamento de 
    dependências e logging detalhado.

CASOS DE USO:
    1. Execução diária automatizada completa
    2. Reprocessamento após mudanças estruturais
    3. Sincronização completa manual
    4. Execução de ETLs específicos

EXEMPLOS DE USO:
    # Execução padrão
    python run_full_pipeline.py
    
    # Modo debug
    python run_full_pipeline.py --debug
    
    # Executar apenas ETLs específicos
    python run_full_pipeline.py --only-etl 001 002
    
    # Dry run (sem carga no banco)
    python run_full_pipeline.py --dry-run
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

# Configurar encoding UTF-8 para evitar problemas com caracteres especiais
import locale
import codecs

# Forçar UTF-8 no Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Configurar locale para UTF-8 quando possível
try:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, '')
    except:
        pass

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

# Definição dos ETLs e ordem de execução
ETL_DEFINITIONS = [
    {
        'id': '001',
        'name': 'Performance Indicators',
        'script': 'etl_001_indicators.py',
        'config': 'config/etl_001_config.json',
        'critical': True,  # Se falhar, para a execução
        'dependencies': []
    },
    {
        'id': '002',
        'name': 'Performance Assignments',
        'script': 'etl_002_assignments.py',
        'config': 'config/etl_002_config.json',
        'critical': True,
        'dependencies': ['001']  # Depende do ETL-001
    },
    {
        'id': '003',
        'name': 'Performance Targets',
        'script': 'etl_003_targets.py',
        'config': 'config/etl_003_config.json',
        'critical': True,
        'dependencies': ['001']  # Depende do ETL-001
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
        file_handler = logging.FileHandler(LOG_DIR / log_file, encoding='utf-8')
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
    conn = pyodbc.connect(conn_str, timeout=30)
    conn.autocommit = True
    return conn

def get_python_command():
    """Detecta o comando Python correto para qualquer sistema operacional."""
    # Primeiro, usar o mesmo Python que está executando este script
    if sys.executable:
        return sys.executable
    
    # Lista de comandos Python para tentar em ordem
    python_commands = []
    
    if sys.platform == 'win32':
        # Windows
        python_commands = ['python.exe', 'python', 'py.exe', 'py']
    else:
        # Unix/Linux/Mac
        python_commands = ['python3', 'python', '/usr/bin/python3', '/usr/local/bin/python3']
    
    # Tentar cada comando
    for cmd in python_commands:
        try:
            result = subprocess.run([cmd, '--version'], 
                                    capture_output=True, 
                                    text=True, 
                                    timeout=5)
            if result.returncode == 0:
                return cmd
        except (subprocess.SubprocessError, FileNotFoundError):
            continue
    
    # Se nenhum funcionou, usar 'python' como fallback
    return 'python'

def should_run_etl(etl_id: str, only_etls: Optional[List[str]] = None) -> bool:
    """
    Verifica se um ETL deve ser executado.
    
    Args:
        etl_id: ID do ETL
        only_etls: Lista de ETLs específicos para executar
        
    Returns:
        True se deve executar, False caso contrário
    """
    if only_etls is None:
        return True
    return etl_id in only_etls

def check_dependencies(etl: Dict, results: Dict) -> bool:
    """
    Verifica se as dependências de um ETL foram satisfeitas.
    
    Args:
        etl: Definição do ETL
        results: Resultados das execuções anteriores
        
    Returns:
        True se todas as dependências foram satisfeitas
    """
    for dep_id in etl['dependencies']:
        if dep_id not in results:
            return False
        if results[dep_id]['status'] != 'SUCCESS':
            return False
    return True

def execute_etl(etl: Dict, logger: logging.Logger, debug: bool = False, dry_run: bool = False) -> Dict:
    """
    Executa um ETL individual.
    
    Args:
        etl: Definição do ETL
        logger: Logger configurado
        debug: Se True, modo debug
        dry_run: Se True, simula execução
        
    Returns:
        Dicionário com resultado da execução
    """
    etl_start = datetime.now()
    result = {
        'etl_id': etl['id'],
        'etl_name': etl['name'],
        'start_time': etl_start,
        'status': 'PENDING',
        'message': '',
        'duration': 0
    }
    
    try:
        # Detectar comando Python
        python_cmd = get_python_command()
        cmd = [python_cmd, etl['script']]
        
        if debug:
            cmd.append('--debug')
        
        if dry_run:
            cmd.append('--dry-run')
            
        # Adicionar config se especificada
        if etl.get('config'):
            cmd.extend(['--config', etl['config']])
        
        logger.info(f"Executando: {' '.join(cmd)}")
        
        if dry_run:
            logger.info(f"[DRY RUN] Simulando execução de {etl['name']}")
            result['status'] = 'SUCCESS'
            result['message'] = 'Dry run - não executado'
        else:
            # Executar comando
            process = subprocess.run(
                cmd,
                cwd=BASE_DIR,
                capture_output=True,
                text=True
            )
            
            if process.returncode == 0:
                result['status'] = 'SUCCESS'
                result['message'] = 'ETL executado com sucesso'
                logger.info(f"ETL {etl['id']} concluído com sucesso")
            else:
                result['status'] = 'ERROR'
                result['message'] = f"Erro: {process.stderr}"
                logger.error(f"ETL {etl['id']} falhou: {process.stderr}")
                
            # Log output detalhado em modo debug
            if debug and process.stdout:
                logger.debug(f"Output do ETL {etl['id']}:\n{process.stdout}")
                    
    except Exception as e:
        result['status'] = 'ERROR'
        result['message'] = str(e)
        logger.error(f"Erro ao executar ETL {etl['id']}: {e}")
        
    finally:
        result['end_time'] = datetime.now()
        result['duration'] = (result['end_time'] - etl_start).total_seconds()
        
    return result

def run_etls(logger: logging.Logger, debug: bool = False, dry_run: bool = False, 
             only_etls: Optional[List[str]] = None, continue_on_error: bool = False) -> int:
    """
    Executa os ETLs com gerenciamento de dependências.
    
    Args:
        logger: Logger configurado
        debug: Se True, modo debug
        dry_run: Se True, simula execução
        only_etls: Lista de ETLs específicos para executar
        continue_on_error: Se True, continua mesmo com erros
        
    Returns:
        Código de retorno (0 = sucesso)
    """
    logger.info("\n" + "="*60)
    logger.info("EXECUTANDO ETLs")
    logger.info("="*60)
    
    if only_etls:
        logger.info(f"ETLs selecionados: {', '.join(only_etls)}")
    
    results = {}
    total_etls = len([e for e in ETL_DEFINITIONS if should_run_etl(e['id'], only_etls)])
    executed = 0
    failed = 0
    
    for etl in ETL_DEFINITIONS:
        if not should_run_etl(etl['id'], only_etls):
            logger.info(f"Pulando ETL {etl['id']} - {etl['name']} (não selecionado)")
            continue
            
        logger.info(f"\n{'='*50}")
        logger.info(f"ETL {etl['id']} - {etl['name']}")
        logger.info(f"{'='*50}")
        
        # Verificar dependências
        if not check_dependencies(etl, results):
            logger.error(f"Dependências não satisfeitas para ETL {etl['id']}")
            results[etl['id']] = {
                'status': 'SKIPPED',
                'message': 'Dependências não satisfeitas'
            }
            if etl['critical'] and not continue_on_error:
                logger.error("ETL crítico falhou - parando execução")
                break
            continue
        
        # Executar ETL
        result = execute_etl(etl, logger, debug, dry_run)
        results[etl['id']] = result
        executed += 1
        
        if result['status'] == 'ERROR':
            failed += 1
            if etl['critical'] and not continue_on_error:
                logger.error("ETL crítico falhou - parando execução")
                break
                
        logger.info(f"Duração: {result['duration']:.2f} segundos")
    
    # Resumo dos ETLs
    logger.info("\n" + "="*50)
    logger.info("RESUMO DOS ETLs")
    logger.info("="*50)
    logger.info(f"Total: {total_etls} | Executados: {executed} | Sucesso: {executed - failed} | Falhas: {failed}")
    
    # Detalhes por ETL
    for etl_id, result in results.items():
        status_symbol = "✓" if result['status'] == 'SUCCESS' else "✗"
        logger.info(f"  {status_symbol} ETL-{etl_id}: {result['status']} ({result.get('duration', 0):.2f}s)")
        if result['status'] == 'ERROR':
            logger.debug(f"    Erro: {result['message'][:100]}...")
    
    return 0 if failed == 0 else 1

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
        logger.info("[OK] Conexão com banco de dados OK")
    except Exception as e:
        logger.error(f"[ERRO] Erro ao conectar ao banco: {e}")
        return False
        
    # Verificar arquivos necessários
    required_files = []
    for etl in ETL_DEFINITIONS:
        required_files.append(etl['script'])
    
    for file in required_files:
        if not (BASE_DIR / file).exists():
            logger.error(f"[ERRO] Arquivo necessário não encontrado: {file}")
            return False
            
    logger.info("[OK] Todos os pré-requisitos atendidos")
    return True

# ==============================================================================
# 5. FUNÇÃO PRINCIPAL
# ==============================================================================

def main():
    """Função principal do pipeline."""
    # Parse argumentos
    parser = argparse.ArgumentParser(
        description='Pipeline ETL - Google Sheets para Bronze',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Ativa modo debug com logs detalhados'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simula execução sem carregar dados'
    )
    
    parser.add_argument(
        '--only-etl',
        nargs='+',
        metavar='ID',
        help='Executar apenas ETLs específicos (ex: --only-etl 001 002)'
    )
    
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continuar execução mesmo se ETL crítico falhar'
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
        logger.info("PIPELINE ETL - GOOGLE SHEETS PARA BRONZE")
        logger.info(f"Início: {start_time}")
        logger.info(f"Modo: {'DRY RUN' if args.dry_run else 'PRODUÇÃO'}")
        logger.info("="*80)
        
        # Verificar pré-requisitos
        if not args.skip_prerequisites:
            if not check_prerequisites(logger):
                logger.error("Pré-requisitos não atendidos - abortando")
                sys.exit(1)
                
        # Executar ETLs
        exit_code = run_etls(
            logger=logger,
            debug=args.debug,
            dry_run=args.dry_run,
            only_etls=args.only_etl,
            continue_on_error=args.continue_on_error
        )
            
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