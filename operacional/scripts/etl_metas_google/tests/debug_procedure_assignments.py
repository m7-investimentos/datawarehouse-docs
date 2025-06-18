#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para debugar a procedure de assignments
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
    """Debug da procedure"""
    print("=== DEBUG PROCEDURE ASSIGNMENTS ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar última carga não processada
        cursor.execute("""
            SELECT MAX(load_id)
            FROM bronze.performance_assignments
            WHERE is_processed = 0
        """)
        
        load_id = cursor.fetchone()[0]
        print(f"Load ID para processar: {load_id}")
        
        # Testar a query de staging manualmente
        print("\nTestando query de staging...")
        cursor.execute("""
            SELECT TOP 5
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
                CASE WHEN i.indicator_id IS NOT NULL THEN 1 ELSE 0 END as indicator_exists
            FROM bronze.performance_assignments b
            LEFT JOIN silver.performance_indicators i 
                ON UPPER(LTRIM(RTRIM(b.indicator_code))) = i.indicator_code
            WHERE b.load_id = ?
              AND b.is_processed = 0
        """, load_id)
        
        print("\nResultados da query de staging:")
        print(f"{'CRM':<10} {'IND_ID':<8} {'CODE':<15} {'TYPE':<10} {'WEIGHT':<8} {'FROM':<12} {'TO':<12} {'EXISTS':<6}")
        print("-"*90)
        
        for row in cursor.fetchall():
            ind_id = str(row[1]) if row[1] else 'NULL'
            print(f"{row[0]:<10} {ind_id:<8} {row[2]:<15} {row[3]:<10} {row[4]:<8.2f} {str(row[5]):<12} {str(row[6]):<12} {row[7]:<6}")
        
        # Verificar se indicadores existem
        print("\n\nVerificando indicadores na Silver:")
        cursor.execute("""
            SELECT COUNT(DISTINCT indicator_code)
            FROM silver.performance_indicators
        """)
        
        ind_count = cursor.fetchone()[0]
        print(f"Total de indicadores na Silver: {ind_count}")
        
        if ind_count > 0:
            cursor.execute("""
                SELECT indicator_id, indicator_code, indicator_name
                FROM silver.performance_indicators
                WHERE is_active = 1
                ORDER BY indicator_code
            """)
            
            print("\nIndicadores disponíveis:")
            for row in cursor.fetchall():
                print(f"  {row[0]}: {row[1]} - {row[2]}")
        
        # Tentar executar procedure com debug
        print("\n\nExecutando procedure com debug=1...")
        try:
            cursor.execute("""
                EXEC bronze.prc_bronze_to_silver_assignments 
                    @load_id = ?,
                    @validate_weights = 1,
                    @force_update = 0,
                    @debug = 1
            """, load_id)
            
            # Capturar mensagens
            while cursor.nextset():
                try:
                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            print(row)
                except:
                    pass
            
            conn.commit()
            print("\n✓ Procedure executada com sucesso!")
            
        except Exception as proc_error:
            print(f"\n✗ Erro na procedure: {proc_error}")
            
            # Tentar identificar o problema específico
            print("\nVerificando problema específico...")
            
            # Verificar se é problema de conversão de tipos
            cursor.execute("""
                SELECT TOP 1 *
                FROM bronze.performance_assignments
                WHERE load_id = ?
            """, load_id)
            
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            
            print("\nPrimeiro registro do Bronze:")
            for i, col in enumerate(columns):
                print(f"  {col}: {row[i]} (tipo: {type(row[i]).__name__})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()