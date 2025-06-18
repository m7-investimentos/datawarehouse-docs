#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para diagnosticar erro específico da procedure
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
    """Diagnóstico do erro"""
    print("=== DIAGNÓSTICO DO ERRO VALID_FROM ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar se a procedure tem alguma view ou tabela derivada
        print("1. Verificando definição da procedure para 'valid_from':")
        cursor.execute("""
            SELECT OBJECT_DEFINITION(OBJECT_ID('bronze.prc_bronze_to_silver_assignments'))
        """)
        
        proc_def = cursor.fetchone()[0]
        if proc_def:
            # Procurar por valid_from sem alias
            import re
            lines = proc_def.split('\n')
            for i, line in enumerate(lines):
                if 'valid_from' in line.lower() and not any(alias in line for alias in ['.valid_from', 's.', 'b.', 'm.']):
                    print(f"   Linha {i}: {line.strip()}")
        
        # Testar queries específicas
        print("\n2. Testando queries da procedure manualmente:")
        
        # Query 1: Validação de pesos
        print("\n   a) Query de validação de pesos:")
        try:
            cursor.execute("""
                WITH test_data AS (
                    SELECT 
                        crm_id,
                        TRY_CAST(valid_from AS DATE) as valid_from,
                        CASE WHEN indicator_type = 'CARD' THEN TRY_CAST(weight AS DECIMAL(5,2)) ELSE 0 END as weight
                    FROM bronze.performance_assignments
                    WHERE is_processed = 0
                      AND indicator_type = 'CARD'
                )
                SELECT COUNT(*) FROM test_data
            """)
            count = cursor.fetchone()[0]
            print(f"      ✓ Query executada com sucesso - {count} registros")
        except Exception as e:
            print(f"      ✗ Erro: {e}")
        
        # Query 2: Update com valid_from
        print("\n   b) Query de update da Silver:")
        try:
            cursor.execute("""
                SELECT COUNT(*)
                FROM silver.performance_assignments m
                WHERE EXISTS (
                    SELECT 1 
                    FROM bronze.performance_assignments b
                    WHERE b.is_processed = 0
                      AND m.crm_id = b.crm_id
                )
            """)
            count = cursor.fetchone()[0]
            print(f"      ✓ Query executada - {count} registros existentes para atualizar")
        except Exception as e:
            print(f"      ✗ Erro: {e}")
        
        # Verificar se há alguma view envolvida
        print("\n3. Verificando views relacionadas:")
        cursor.execute("""
            SELECT 
                s.name as view_name,
                m.definition
            FROM sys.views s
            JOIN sys.sql_modules m ON s.object_id = m.object_id
            WHERE m.definition LIKE '%performance_assignments%'
        """)
        
        views = cursor.fetchall()
        if views:
            for view in views:
                print(f"   View: {view[0]}")
                if 'valid_from' in view[1]:
                    print("      Contém referência a valid_from")
        else:
            print("   Nenhuma view relacionada encontrada")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()