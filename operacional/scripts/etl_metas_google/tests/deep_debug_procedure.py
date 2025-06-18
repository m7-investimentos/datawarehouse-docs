#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para debug profundo da procedure
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
    """Debug profundo"""
    print("=== DEBUG PROFUNDO DA PROCEDURE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Verificar se o problema está no UPDATE da linha 291-305
        print("1. Testando UPDATE de encerramento de vigências:")
        cursor.execute("""
            SELECT COUNT(*)
            FROM silver.performance_assignments m
            WHERE m.valid_to IS NULL
              AND EXISTS (
                  SELECT 1 
                  FROM bronze.performance_assignments b
                  WHERE b.is_processed = 0
                    AND m.crm_id = b.crm_id
              )
        """)
        count = cursor.fetchone()[0]
        print(f"   Registros que seriam atualizados: {count}")
        
        # 2. Simular parte por parte da procedure
        print("\n2. Simulando partes da procedure:")
        
        # Criar tabela staging manualmente
        print("\n   a) Criando tabela staging:")
        try:
            cursor.execute("""
                IF OBJECT_ID('tempdb..#test_staging') IS NOT NULL
                    DROP TABLE #test_staging;
                    
                CREATE TABLE #test_staging (
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
            print("      ✓ Tabela criada")
        except Exception as e:
            print(f"      ✗ Erro: {e}")
        
        # Popular staging
        print("\n   b) Populando staging:")
        try:
            cursor.execute("""
                INSERT INTO #test_staging
                SELECT TOP 10
                    UPPER(LTRIM(RTRIM(b.crm_id))) as crm_id,
                    i.indicator_id,
                    UPPER(LTRIM(RTRIM(b.indicator_code))) as indicator_code,
                    UPPER(LTRIM(RTRIM(b.indicator_type))) as indicator_type,
                    CASE 
                        WHEN b.indicator_type = 'CARD' THEN TRY_CAST(b.weight AS DECIMAL(5,2))
                        ELSE 0.00
                    END as indicator_weight,
                    TRY_CAST(b.valid_from AS DATE) as valid_from,
                    TRY_CAST(b.valid_to AS DATE) as valid_to,
                    'ETL_SYSTEM' as created_by,
                    NULL as approved_by,
                    b.notes as comments,
                    b.row_hash,
                    b.load_id,
                    CASE WHEN b.weight_validation = '1' THEN 1 ELSE 0 END as weight_sum_valid,
                    CASE WHEN i.indicator_id IS NOT NULL THEN 1 ELSE 0 END as indicator_exists,
                    NULL as validation_errors
                FROM bronze.performance_assignments b
                LEFT JOIN silver.performance_indicators i 
                    ON UPPER(LTRIM(RTRIM(b.indicator_code))) = i.indicator_code
                WHERE b.is_processed = 0
            """)
            rows = cursor.rowcount
            print(f"      ✓ {rows} registros inseridos")
            
            # Verificar dados
            cursor.execute("SELECT TOP 5 crm_id, valid_from, indicator_code FROM #test_staging")
            for row in cursor.fetchall():
                print(f"      - {row[0]}: {row[1]}, {row[2]}")
                
        except Exception as e:
            print(f"      ✗ Erro: {e}")
        
        # Testar query problemática do UPDATE
        print("\n   c) Testando subquery do UPDATE:")
        try:
            cursor.execute("""
                SELECT DISTINCT crm_id, MIN(valid_from) as new_valid_from
                FROM #test_staging
                WHERE indicator_exists = 1
                GROUP BY crm_id
            """)
            rows = cursor.fetchall()
            print(f"      ✓ Query funcionou - {len(rows)} grupos")
        except Exception as e:
            print(f"      ✗ Erro: {e}")
        
        # 3. Verificar se há alguma view ou computed column
        print("\n3. Verificando computed columns na Silver:")
        cursor.execute("""
            SELECT 
                c.name as column_name,
                c.is_computed,
                cc.definition
            FROM sys.columns c
            LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
            WHERE c.object_id = OBJECT_ID('silver.performance_assignments')
              AND (c.is_computed = 1 OR c.name = 'valid_from')
        """)
        
        for row in cursor.fetchall():
            print(f"   Coluna: {row[0]}, Computed: {row[1]}, Definição: {row[2]}")
        
        # 4. Verificar se há triggers
        print("\n4. Verificando triggers:")
        cursor.execute("""
            SELECT 
                t.name,
                t.is_disabled,
                CASE 
                    WHEN m.definition LIKE '%valid_from%' THEN 'Contém valid_from'
                    ELSE 'Não contém valid_from'
                END as has_valid_from
            FROM sys.triggers t
            JOIN sys.sql_modules m ON t.object_id = m.object_id
            WHERE t.parent_id IN (
                OBJECT_ID('bronze.performance_assignments'),
                OBJECT_ID('silver.performance_assignments')
            )
        """)
        
        triggers = cursor.fetchall()
        if triggers:
            for trigger in triggers:
                print(f"   {trigger[0]} (Disabled: {trigger[1]}): {trigger[2]}")
        else:
            print("   Nenhum trigger encontrado")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()