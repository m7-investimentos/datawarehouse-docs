#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Script para execução de pipelines individuais (ETL + Procedure)
================================================================================
Permite executar cada pipeline de forma isolada:
- Pipeline 001: Indicators (ETL + Procedure)
- Pipeline 002: Assignments (ETL + Procedure)  
- Pipeline 003: Targets (ETL + Procedure)
================================================================================
"""

import sys
import os
import subprocess
import pyodbc
from datetime import datetime
from pathlib import Path

# Adicionar diretório ao path
sys.path.append(str(Path(__file__).parent))

# Configurações
from dotenv import load_dotenv
load_dotenv('credentials/.env')

# Cores para output
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color

def print_colored(message, color=Colors.NC):
    """Imprime mensagem colorida"""
    print(f"{color}{message}{Colors.NC}")

def execute_etl(etl_number):
    """Executa o ETL especificado"""
    etl_scripts = {
        '001': 'etl_001_indicators.py',
        '002': 'etl_002_assignments.py',
        '003': 'etl_003_targets.py'
    }
    
    if etl_number not in etl_scripts:
        print_colored(f"ETL {etl_number} não encontrado", Colors.RED)
        return False
    
    script_name = etl_scripts[etl_number]
    print_colored(f"Executando {script_name}...", Colors.YELLOW)
    
    try:
        result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
        
        if result.returncode == 0:
            print_colored(f"✓ ETL-{etl_number} concluído com sucesso", Colors.GREEN)
            return True
        else:
            print_colored(f"✗ Erro no ETL-{etl_number}:", Colors.RED)
            print(result.stderr)
            return False
    except Exception as e:
        print_colored(f"✗ Erro ao executar ETL-{etl_number}: {str(e)}", Colors.RED)
        return False

def execute_procedure(proc_name, proc_desc):
    """Executa a procedure Bronze to Silver"""
    print_colored(f"Executando procedure: {proc_desc}...", Colors.YELLOW)
    
    # Configurações do banco
    server = os.getenv('DB_SERVER', 'localhost')
    database = os.getenv('DB_DATABASE', 'M7Medallion')
    username = os.getenv('DB_USERNAME', 'sa')
    password = os.getenv('DB_PASSWORD', '')
    
    try:
        # Conectar ao banco
        driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')
        conn_str = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Executar procedure
        cursor.execute(f"EXEC {proc_name}")
        conn.commit()
        
        print_colored("✓ Procedure executada com sucesso", Colors.GREEN)
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print_colored(f"✗ Erro ao executar procedure: {str(e)}", Colors.RED)
        return False

def run_pipeline(pipeline_number):
    """Executa o pipeline completo (ETL + Procedure)"""
    pipelines = {
        '001': {
            'name': 'Indicators',
            'procedure': 'bronze.prc_process_indicators_to_silver',
            'description': 'Bronze to Silver - Indicators'
        },
        '002': {
            'name': 'Assignments',
            'procedure': 'bronze.prc_bronze_to_silver_assignments',
            'description': 'Bronze to Silver - Assignments'
        },
        '003': {
            'name': 'Targets',
            'procedure': 'bronze.prc_bronze_to_silver_performance_targets',
            'description': 'Bronze to Silver - Targets'
        }
    }
    
    if pipeline_number not in pipelines:
        print_colored(f"Pipeline {pipeline_number} não encontrado", Colors.RED)
        return False
    
    pipeline = pipelines[pipeline_number]
    
    print_colored(f"\n{'='*60}", Colors.BLUE)
    print_colored(f"Pipeline {pipeline_number} - {pipeline['name']}", Colors.BLUE)
    print_colored(f"{'='*60}", Colors.BLUE)
    
    # Passo 1: ETL
    print_colored(f"\nPasso 1/2: Executando ETL-{pipeline_number}...", Colors.BLUE)
    if not execute_etl(pipeline_number):
        print_colored(f"✗ Erro no ETL-{pipeline_number}. Procedure não será executada.", Colors.RED)
        return False
    
    # Passo 2: Procedure
    print_colored(f"\nPasso 2/2: Executando procedure Bronze to Silver...", Colors.BLUE)
    if not execute_procedure(pipeline['procedure'], pipeline['description']):
        print_colored(f"✗ Erro na procedure do pipeline {pipeline_number}", Colors.RED)
        return False
    
    print_colored(f"\n✓ Pipeline {pipeline_number} concluído com sucesso!", Colors.GREEN)
    return True

def main():
    """Função principal"""
    if len(sys.argv) != 2:
        print_colored("Uso: python run_pipeline.py <número_pipeline>", Colors.YELLOW)
        print_colored("Exemplos:", Colors.BLUE)
        print_colored("  python run_pipeline.py 001  # Pipeline Indicators", Colors.BLUE)
        print_colored("  python run_pipeline.py 002  # Pipeline Assignments", Colors.BLUE)
        print_colored("  python run_pipeline.py 003  # Pipeline Targets", Colors.BLUE)
        sys.exit(1)
    
    pipeline_number = sys.argv[1]
    
    # Executar pipeline
    start_time = datetime.now()
    success = run_pipeline(pipeline_number)
    end_time = datetime.now()
    
    # Relatório final
    duration = (end_time - start_time).total_seconds()
    print_colored(f"\nTempo de execução: {duration:.2f} segundos", Colors.BLUE)
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()