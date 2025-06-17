#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verify ETL-003 data loaded to bronze.performance_targets
"""
import os
import pyodbc
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv(Path(__file__).parent.parent / 'credentials' / '.env')

# Database configuration
DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
}

print("="*60)
print("VERIFICAÇÃO DE DADOS - ETL-003 PERFORMANCE TARGETS")
print("="*60)

try:
    # Create connection using pyodbc directly
    driver = DB_CONFIG['driver'].strip('{}')
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['user']};"
        f"PWD={DB_CONFIG['password']};"
        f"TrustServerCertificate=yes"
    )
    
    with pyodbc.connect(conn_str, timeout=30) as conn:
        cursor = conn.cursor()
        
        # 1. Total records
        cursor.execute("""
            SELECT COUNT(*) as total_records 
            FROM bronze.performance_targets
            WHERE load_timestamp >= DATEADD(HOUR, -1, GETDATE())
        """)
        total = cursor.fetchone()[0]
        print(f"\n✓ Total de registros carregados (última hora): {total}")
        
        # 2. Records by year
        cursor.execute("""
            SELECT target_year, COUNT(*) as count 
            FROM bronze.performance_targets
            WHERE load_timestamp >= DATEADD(HOUR, -1, GETDATE())
            GROUP BY target_year
            ORDER BY target_year
        """)
        print("\nRegistros por ano:")
        for row in cursor.fetchall():
            print(f"  - {row.target_year}: {row.count} registros")
            
        # 3. Unique assessors and indicators
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT cod_assessor) as unique_assessors,
                COUNT(DISTINCT indicator_code) as unique_indicators
            FROM bronze.performance_targets
            WHERE load_timestamp >= DATEADD(HOUR, -1, GETDATE())
        """)
        row = cursor.fetchone()
        print(f"\n✓ Assessores únicos: {row[0]}")
        print(f"✓ Indicadores únicos: {row[1]}")
        
        # 4. Sample data
        cursor.execute("""
            SELECT TOP 5 
                cod_assessor, 
                nome_assessor, 
                indicator_code,
                period_start,
                target_value,
                stretch_value,
                minimum_value
            FROM bronze.performance_targets
            WHERE load_timestamp >= DATEADD(HOUR, -1, GETDATE())
            ORDER BY load_id DESC
        """)
        print("\nAmostra dos últimos 5 registros:")
        print("-"*60)
        for row in cursor.fetchall():
            print(f"Assessor: {row.cod_assessor} - {row.nome_assessor}")
            print(f"Indicador: {row.indicator_code}")
            print(f"Período: {row.period_start}")
            print(f"Metas: Target={row.target_value}, Stretch={row.stretch_value}, Min={row.minimum_value}")
            print("-"*60)
            
        # 5. Validation errors
        cursor.execute("""
            SELECT COUNT(*) as errors
            FROM bronze.performance_targets
            WHERE load_timestamp >= DATEADD(HOUR, -1, GETDATE())
            AND validation_errors IS NOT NULL
        """)
        errors = cursor.fetchone()[0]
        print(f"\n✓ Registros com erros de validação: {errors}")
        
        # 6. Invalid logic
        cursor.execute("""
            SELECT COUNT(*) as invalid
            FROM bronze.performance_targets
            WHERE load_timestamp >= DATEADD(HOUR, -1, GETDATE())
            AND target_logic_valid = 0
        """)
        invalid = cursor.fetchone()[0]
        print(f"✓ Registros com lógica inválida: {invalid}")
        
except Exception as e:
    print(f"\n❌ Erro ao verificar dados: {e}")
    
print("\n" + "="*60)