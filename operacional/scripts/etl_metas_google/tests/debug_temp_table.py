#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para debugar a criação da tabela temporária
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
    """Debug da tabela temporária"""
    print("=== DEBUG DA TABELA TEMPORÁRIA ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        print("1. Criando e testando tabela temporária:")
        
        # Criar tabela temporária
        cursor.execute("""
            IF OBJECT_ID('tempdb..#test_assignments') IS NOT NULL
                DROP TABLE #test_assignments;
                
            CREATE TABLE #test_assignments (
                crm_id VARCHAR(20),
                indicator_id INT,
                indicator_code VARCHAR(100),
                indicator_type VARCHAR(50),
                indicator_weight DECIMAL(5,2),
                valid_from DATE,
                valid_to DATE,
                created_by VARCHAR(100),
                approved_by VARCHAR(100),
                comments NVARCHAR(1000),
                row_hash VARCHAR(32),
                bronze_load_id INT,
                weight_sum_valid BIT,
                indicator_exists BIT,
                validation_errors NVARCHAR(MAX)
            );
        """)
        print("   ✓ Tabela temporária criada")
        
        # Inserir dados de teste
        cursor.execute("""
            INSERT INTO #test_assignments (crm_id, indicator_type, valid_from, indicator_weight)
            VALUES ('TEST001', 'CARD', '2025-01-01', 50.00);
        """)
        print("   ✓ Dados inseridos")
        
        # Testar query problemática
        print("\n2. Testando queries problemáticas:")
        
        # Query 1
        print("   a) Query de agregação:")
        try:
            cursor.execute("""
                SELECT 
                    crm_id,
                    valid_from,
                    SUM(indicator_weight) as total_weight
                FROM #test_assignments
                GROUP BY crm_id, valid_from
            """)
            result = cursor.fetchone()
            print(f"      ✓ Funcionou: {result}")
        except Exception as e:
            print(f"      ✗ Erro: {e}")
        
        # Query 2
        print("\n   b) Query COUNT com subquery:")
        try:
            cursor.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT crm_id, valid_from
                    FROM #test_assignments
                    WHERE indicator_type = 'CARD'
                ) AS distinct_errors
            """)
            result = cursor.fetchone()
            print(f"      ✓ Funcionou: {result[0]} registros")
        except Exception as e:
            print(f"      ✗ Erro: {e}")
        
        # Verificar se o problema é com o contexto da procedure
        print("\n3. Testando dentro de uma procedure:")
        
        # Criar procedure de teste
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'test_valid_from_error')
                DROP PROCEDURE test_valid_from_error;
        """)
        
        cursor.execute("""
            CREATE PROCEDURE test_valid_from_error
            AS
            BEGIN
                -- Criar tabela temp
                CREATE TABLE #test_staging (
                    crm_id VARCHAR(20),
                    valid_from DATE,
                    weight DECIMAL(5,2)
                );
                
                -- Inserir dados
                INSERT INTO #test_staging VALUES ('TEST', '2025-01-01', 50);
                
                -- Query problemática
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT crm_id, valid_from
                    FROM #test_staging
                ) AS x;
            END
        """)
        print("   ✓ Procedure de teste criada")
        
        # Executar procedure de teste
        try:
            cursor.execute("EXEC test_valid_from_error")
            result = cursor.fetchone()
            print(f"   ✓ Procedure executada com sucesso: {result[0]}")
        except Exception as e:
            print(f"   ✗ Erro na procedure: {e}")
        
        # Limpar
        cursor.execute("DROP PROCEDURE test_valid_from_error")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()