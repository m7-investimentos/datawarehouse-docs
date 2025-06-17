#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de teste para a procedure prc_bronze_to_silver_performance_targets
"""

import os
import pyodbc
from dotenv import load_dotenv
from pathlib import Path

# Carregar variáveis de ambiente
CREDENTIALS_DIR = Path(__file__).parent / 'credentials'
load_dotenv(CREDENTIALS_DIR / '.env')

# Configuração do banco
DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
}

def test_procedure():
    """Testa a execução da procedure com autocommit."""
    print("Conectando ao banco de dados...")
    
    # Criar conexão
    driver = DB_CONFIG['driver'].strip('{}')
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['user']};"
        f"PWD={DB_CONFIG['password']};"
        f"TrustServerCertificate=yes"
    )
    
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        conn.autocommit = True  # Importante: usar autocommit
        cursor = conn.cursor()
        
        print("Executando procedure...")
        
        # Executar procedure
        cursor.execute("EXEC [bronze].[prc_bronze_to_silver_performance_targets] @validate_completeness = 1, @debug_mode = 1")
        
        # Capturar todos os resultsets
        while True:
            try:
                # Tentar pegar o próximo resultset
                if cursor.nextset():
                    # Se houver linhas, imprimir
                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            print(row)
                else:
                    break
            except pyodbc.ProgrammingError:
                # Não há mais resultsets
                break
        
        print("\nProcedure executada com sucesso!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro ao executar procedure: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_procedure()