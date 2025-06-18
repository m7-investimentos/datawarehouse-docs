#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para executar a procedure passo a passo
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
    """Executa procedure passo a passo"""
    print("=== EXECUÇÃO PASSO A PASSO ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Simular início da procedure
        print("1. Iniciando transação...")
        cursor.execute("BEGIN TRANSACTION")
        
        # Verificar registros para processar
        print("\n2. Verificando registros não processados...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM bronze.performance_assignments 
            WHERE is_processed = 0
        """)
        count = cursor.fetchone()[0]
        print(f"   Registros para processar: {count}")
        
        # Criar tabela staging
        print("\n3. Criando tabela staging...")
        cursor.execute("""
            IF OBJECT_ID('tempdb..#assignments_staging') IS NOT NULL
                DROP TABLE #assignments_staging;
                
            CREATE TABLE #assignments_staging (
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
        
        # Popular staging
        print("\n4. Populando tabela staging...")
        cursor.execute("""
            INSERT INTO #assignments_staging
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
            WHERE b.is_processed = 0;
        """)
        rows = cursor.rowcount
        print(f"   ✓ {rows} registros inseridos na staging")
        
        # Verificar dados na staging
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT crm_id) FROM #assignments_staging")
        result = cursor.fetchone()
        print(f"   Total: {result[0]}, Assessores únicos: {result[1]}")
        
        # Testar validação de pesos
        print("\n5. Testando validação de pesos...")
        try:
            cursor.execute("""
                WITH weight_validation AS (
                    SELECT 
                        crm_id,
                        valid_from,
                        SUM(indicator_weight) as total_weight,
                        COUNT(*) as card_count
                    FROM #assignments_staging
                    WHERE indicator_type = 'CARD'
                      AND indicator_exists = 1
                      AND (valid_to IS NULL OR valid_to > GETDATE())
                    GROUP BY crm_id, valid_from
                )
                UPDATE s
                SET s.weight_sum_valid = CASE 
                    WHEN ABS(v.total_weight - 100.00) < 0.01 THEN 1 
                    ELSE 0 
                END
                FROM #assignments_staging s
                INNER JOIN weight_validation v 
                    ON s.crm_id = v.crm_id 
                    AND s.valid_from = v.valid_from
                WHERE s.indicator_type = 'CARD';
            """)
            print("   ✓ Validação de pesos executada")
        except Exception as e:
            print(f"   ✗ Erro na validação: {e}")
        
        # Testar contagem de erros
        print("\n6. Testando contagem de erros...")
        try:
            cursor.execute("""
                SELECT COUNT(DISTINCT crm_id + CAST(valid_from AS VARCHAR))
                FROM #assignments_staging
                WHERE weight_sum_valid = 0
                  AND indicator_type = 'CARD';
            """)
            errors = cursor.fetchone()[0]
            print(f"   Assessores com erro de peso: {errors}")
        except Exception as e:
            print(f"   ✗ Erro na contagem: {e}")
        
        # Reverter transação
        print("\n7. Revertendo transação...")
        cursor.execute("ROLLBACK")
        print("   ✓ Transação revertida")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n✗ ERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()