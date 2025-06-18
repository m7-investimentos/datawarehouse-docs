#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar a view problemática
"""

import os
import pyodbc
from pathlib import Path
from dotenv import load_dotenv

# Configuração
BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = BASE_DIR / 'credentials'
load_dotenv(CREDENTIALS_DIR / '.env')

def connect_to_database():
    """Conecta ao banco de dados SQL Server"""
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes"
    )
    
    return pyodbc.connect(conn_str)

def main():
    """Verifica view problemática"""
    print("=== VERIFICAÇÃO DA VIEW ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Obter definição da view
        cursor.execute("""
            SELECT 
                SCHEMA_NAME(v.schema_id) as schema_name,
                v.name as view_name,
                m.definition
            FROM sys.views v
            JOIN sys.sql_modules m ON v.object_id = m.object_id
            WHERE v.name = 'vw_performance_assignments_current'
        """)
        
        result = cursor.fetchone()
        if result:
            print(f"Schema: {result[0]}")
            print(f"View: {result[1]}")
            print(f"\nDefinição:")
            print("-" * 60)
            print(result[2])
            print("-" * 60)
            
            # Testar se a view funciona
            print("\nTestando SELECT da view:")
            try:
                cursor.execute(f"SELECT TOP 1 * FROM {result[0]}.{result[1]}")
                print("✓ View funciona corretamente")
            except Exception as e:
                print(f"✗ Erro ao acessar view: {e}")
        
        # Verificar se a procedure referencia essa view
        print("\n\nVerificando se a procedure usa essa view:")
        cursor.execute("""
            SELECT OBJECT_DEFINITION(OBJECT_ID('bronze.prc_bronze_to_silver_assignments'))
        """)
        
        proc_def = cursor.fetchone()[0]
        if 'vw_performance_assignments_current' in proc_def:
            print("✗ A procedure referencia a view vw_performance_assignments_current")
            
            # Encontrar onde
            lines = proc_def.split('\n')
            for i, line in enumerate(lines):
                if 'vw_performance_assignments_current' in line:
                    print(f"   Linha {i}: {line.strip()}")
        else:
            print("✓ A procedure NÃO referencia diretamente essa view")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()