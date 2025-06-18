#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para testar partes específicas da procedure
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
    """Testa partes da procedure"""
    print("=== TESTE DE PARTES DA PROCEDURE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Obter load_id
        cursor.execute("""
            SELECT MAX(load_id)
            FROM bronze.performance_assignments
            WHERE is_processed = 0
        """)
        
        load_id = cursor.fetchone()[0]
        print(f"Load ID: {load_id}")
        
        # Criar tabela temporária como na procedure
        print("\n1. Criando tabela temporária...")
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
        print("   ✓ Tabela temporária criada")
        
        # Inserir dados
        print("\n2. Inserindo dados na tabela temporária...")
        cursor.execute("""
            INSERT INTO #test_staging
            SELECT 
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
            WHERE b.load_id = ?
              AND b.is_processed = 0;
        """, load_id)
        
        rows = cursor.rowcount
        print(f"   ✓ {rows} registros inseridos")
        
        # Verificar dados
        cursor.execute("SELECT COUNT(*) FROM #test_staging")
        count = cursor.fetchone()[0]
        print(f"   Total na staging: {count}")
        
        # Testar validação de pesos
        print("\n3. Testando validação de pesos...")
        cursor.execute("""
            WITH weight_validation AS (
                SELECT 
                    crm_id,
                    valid_from,
                    SUM(indicator_weight) as total_weight,
                    COUNT(*) as card_count
                FROM #test_staging
                WHERE indicator_type = 'CARD'
                  AND indicator_exists = 1
                  AND (valid_to IS NULL OR valid_to > GETDATE())
                GROUP BY crm_id, valid_from
            )
            SELECT 
                crm_id,
                valid_from,
                total_weight,
                card_count
            FROM weight_validation
            WHERE ABS(total_weight - 100.00) >= 0.01
        """)
        
        invalid = cursor.fetchall()
        if invalid:
            print(f"   ⚠️  {len(invalid)} assessores com soma de pesos inválida")
        else:
            print("   ✓ Todos os pesos estão válidos")
        
        # Testar merge
        print("\n4. Testando INSERT na Silver...")
        
        # Primeiro verificar estrutura da Silver
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'silver'
            AND TABLE_NAME = 'performance_assignments'
            ORDER BY ORDINAL_POSITION
        """)
        
        print("\n   Colunas da tabela Silver:")
        for col in cursor.fetchall():
            print(f"     {col[0]}: {col[1]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n✗ ERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()