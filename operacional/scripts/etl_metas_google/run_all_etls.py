#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Orquestrador de ETLs - Performance Indicators & Assignments
================================================================================
Tipo: Script Orquestrador
Versão: 1.0.0
Última atualização: 2025-01-17
Autor: bruno.chiaramonti@multisete.com
Revisor: arquitetura.dados@m7investimentos.com.br
Tags: [orquestrador, etl, performance, batch]
Status: produção
Python: 3.8+
================================================================================

OBJETIVO:
    Executar todos os ETLs de performance em sequência, garantindo a ordem
    correta de dependências e tratamento de erros.

CASOS DE USO:
    1. Execução diária automatizada de todos os ETLs
    2. Reprocessamento completo após mudanças
    3. Execução manual para sincronização

FREQUÊNCIA DE EXECUÇÃO:
    Diária ou sob demanda

EXEMPLOS DE USO:
    # Execução padrão
    python run_all_etls.py
    
    # Modo debug
    python run_all_etls.py --debug
    
    # Dry run (sem carga no banco)
    python run_all_etls.py --dry-run
    
    # Executar apenas ETLs específicos
    python run_all_etls.py --only-etl 001 002
"""

# ==============================================================================
# 1. IMPORTS
# ==============================================================================
import os
import sys
import json
import logging
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ==============================================================================
# 2. CONFIGURAÇÕES E CONSTANTES
# ==============================================================================

# Configuração de logging
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] [ETL-ORCHESTRATOR] %(message)s'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Diretórios
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

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
    }
]

# ==============================================================================
# 3. CONFIGURAÇÃO DE LOGGING
# ==============================================================================

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    """
    Configura o sistema de logging.
    
    Args:
        log_file: Nome do arquivo de log (opcional)
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger('ETL-ORCHESTRATOR')
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
# 4. CLASSE PRINCIPAL
# ==============================================================================

class ETLOrchestrator:
    """
    Orquestrador para executar múltiplos ETLs em sequência.
    
    Attributes:
        logger: Logger para registro de eventos
        dry_run: Se True, não executa realmente os ETLs
        debug: Se True, ativa modo debug
        only_etls: Lista de IDs de ETLs para executar (None = todos)
    """
    
    def __init__(self, logger: logging.Logger, dry_run: bool = False, 
                 debug: bool = False, only_etls: Optional[List[str]] = None):
        """
        Inicializa o orquestrador.
        
        Args:
            logger: Logger configurado
            dry_run: Se True, simula execução
            debug: Se True, modo debug
            only_etls: Lista de ETLs específicos para executar
        """
        self.logger = logger
        self.dry_run = dry_run
        self.debug = debug
        self.only_etls = only_etls
        self.results = {}
        self.start_time = None
        
    def should_run_etl(self, etl_id: str) -> bool:
        """
        Verifica se um ETL deve ser executado.
        
        Args:
            etl_id: ID do ETL
            
        Returns:
            True se deve executar, False caso contrário
        """
        if self.only_etls is None:
            return True
        return etl_id in self.only_etls
        
    def check_dependencies(self, etl: Dict) -> bool:
        """
        Verifica se as dependências de um ETL foram satisfeitas.
        
        Args:
            etl: Definição do ETL
            
        Returns:
            True se todas as dependências foram satisfeitas
        """
        for dep_id in etl['dependencies']:
            if dep_id not in self.results:
                self.logger.error(f"Dependência {dep_id} não foi executada")
                return False
            if self.results[dep_id]['status'] != 'SUCCESS':
                self.logger.error(f"Dependência {dep_id} falhou")
                return False
        return True
        
    def execute_etl(self, etl: Dict) -> Dict:
        """
        Executa um ETL individual.
        
        Args:
            etl: Definição do ETL
            
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
            # Construir comando
            cmd = ['python3', etl['script']]
            
            if self.debug:
                cmd.append('--debug')
            
            if self.dry_run:
                cmd.append('--dry-run')
                
            # Adicionar config se especificada
            if etl.get('config'):
                cmd.extend(['--config', etl['config']])
            
            self.logger.info(f"Executando: {' '.join(cmd)}")
            
            if self.dry_run:
                self.logger.info(f"[DRY RUN] Simulando execução de {etl['name']}")
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
                    self.logger.info(f"ETL {etl['id']} concluído com sucesso")
                else:
                    result['status'] = 'ERROR'
                    result['message'] = f"Erro: {process.stderr}"
                    self.logger.error(f"ETL {etl['id']} falhou: {process.stderr}")
                    
                # Log output detalhado em modo debug
                if self.debug and process.stdout:
                    self.logger.debug(f"Output do ETL {etl['id']}:\n{process.stdout}")
                    
        except Exception as e:
            result['status'] = 'ERROR'
            result['message'] = str(e)
            self.logger.error(f"Erro ao executar ETL {etl['id']}: {e}")
            
        finally:
            result['end_time'] = datetime.now()
            result['duration'] = (result['end_time'] - etl_start).total_seconds()
            
        return result
        
    def run(self):
        """
        Executa todos os ETLs em sequência.
        """
        self.start_time = datetime.now()
        self.logger.info("="*80)
        self.logger.info("INICIANDO ORQUESTRAÇÃO DE ETLs")
        self.logger.info(f"Timestamp: {self.start_time}")
        self.logger.info(f"Modo: {'DRY RUN' if self.dry_run else 'PRODUÇÃO'}")
        if self.only_etls:
            self.logger.info(f"ETLs selecionados: {', '.join(self.only_etls)}")
        self.logger.info("="*80)
        
        total_etls = len([e for e in ETL_DEFINITIONS if self.should_run_etl(e['id'])])
        executed = 0
        failed = 0
        
        for etl in ETL_DEFINITIONS:
            if not self.should_run_etl(etl['id']):
                self.logger.info(f"Pulando ETL {etl['id']} - {etl['name']} (não selecionado)")
                continue
                
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"ETL {etl['id']} - {etl['name']}")
            self.logger.info(f"{'='*60}")
            
            # Verificar dependências
            if not self.check_dependencies(etl):
                self.logger.error(f"Dependências não satisfeitas para ETL {etl['id']}")
                self.results[etl['id']] = {
                    'status': 'SKIPPED',
                    'message': 'Dependências não satisfeitas'
                }
                if etl['critical']:
                    self.logger.error("ETL crítico falhou - parando execução")
                    break
                continue
            
            # Executar ETL
            result = self.execute_etl(etl)
            self.results[etl['id']] = result
            executed += 1
            
            if result['status'] == 'ERROR':
                failed += 1
                if etl['critical']:
                    self.logger.error("ETL crítico falhou - parando execução")
                    break
                    
            self.logger.info(f"Duração: {result['duration']:.2f} segundos")
            
        # Resumo final
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        self.logger.info("\n" + "="*80)
        self.logger.info("RESUMO DA EXECUÇÃO")
        self.logger.info("="*80)
        self.logger.info(f"Total de ETLs: {total_etls}")
        self.logger.info(f"Executados: {executed}")
        self.logger.info(f"Sucesso: {executed - failed}")
        self.logger.info(f"Falhas: {failed}")
        self.logger.info(f"Tempo total: {total_duration:.2f} segundos")
        
        # Detalhes por ETL
        self.logger.info("\nDetalhes por ETL:")
        for etl_id, result in self.results.items():
            status_symbol = "✓" if result['status'] == 'SUCCESS' else "✗"
            self.logger.info(f"  {status_symbol} ETL-{etl_id}: {result['status']} ({result.get('duration', 0):.2f}s)")
            if result['status'] == 'ERROR':
                self.logger.info(f"    Erro: {result['message'][:100]}...")
                
        self.logger.info("="*80)
        
        # Retornar código de saída apropriado
        return 0 if failed == 0 else 1

# ==============================================================================
# 5. FUNÇÕES AUXILIARES
# ==============================================================================

def parse_arguments() -> argparse.Namespace:
    """
    Define e processa argumentos da linha de comando.
    
    Returns:
        Namespace com os argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='Orquestrador de ETLs - Performance',
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
    
    return parser.parse_args()

# ==============================================================================
# 6. FUNÇÃO PRINCIPAL
# ==============================================================================

def main():
    """Função principal do script."""
    # Parse argumentos
    args = parse_arguments()
    
    # Configurar logging
    log_filename = f"ETL-ORCHESTRATOR_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logging(log_filename)
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        
    try:
        # Modificar definições se continue-on-error
        if args.continue_on_error:
            for etl in ETL_DEFINITIONS:
                etl['critical'] = False
        
        # Criar e executar orquestrador
        orchestrator = ETLOrchestrator(
            logger=logger,
            dry_run=args.dry_run,
            debug=args.debug,
            only_etls=args.only_etl
        )
        
        exit_code = orchestrator.run()
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)

# ==============================================================================
# 7. EXECUÇÃO
# ==============================================================================

if __name__ == '__main__':
    main()