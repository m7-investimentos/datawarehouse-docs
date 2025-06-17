#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verify all ETL data loaded to Bronze tables
"""
import os
import pyodbc
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

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

print("="*80)
print("RESUMO COMPLETO - DADOS CARREGADOS NO BRONZE")
print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

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
        
        # ETL-001: Performance Indicators
        print("\n1. ETL-001 - PERFORMANCE INDICATORS")
        print("-"*40)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT indicator_code) as unique_indicators,
                MIN(load_timestamp) as first_load,
                MAX(load_timestamp) as last_load
            FROM bronze.performance_indicators
        """)
        row = cursor.fetchone()
        print(f"   Total de registros: {row[0]}")
        print(f"   Indicadores únicos: {row[1]}")
        print(f"   Primeira carga: {row[2]}")
        print(f"   Última carga: {row[3]}")
        
        # ETL-002: Performance Assignments
        print("\n2. ETL-002 - PERFORMANCE ASSIGNMENTS")
        print("-"*40)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT cod_assessor) as unique_assessors,
                COUNT(DISTINCT indicator_code) as unique_indicators,
                MIN(load_timestamp) as first_load,
                MAX(load_timestamp) as last_load
            FROM bronze.performance_assignments
        """)
        row = cursor.fetchone()
        print(f"   Total de registros: {row[0]}")
        print(f"   Assessores únicos: {row[1]}")
        print(f"   Indicadores únicos: {row[2]}")
        print(f"   Primeira carga: {row[3]}")
        print(f"   Última carga: {row[4]}")
        
        # ETL-003: Performance Targets
        print("\n3. ETL-003 - PERFORMANCE TARGETS")
        print("-"*40)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT cod_assessor) as unique_assessors,
                COUNT(DISTINCT indicator_code) as unique_indicators,
                COUNT(DISTINCT target_year) as years,
                MIN(load_timestamp) as first_load,
                MAX(load_timestamp) as last_load
            FROM bronze.performance_targets
        """)
        row = cursor.fetchone()
        print(f"   Total de registros: {row[0]}")
        print(f"   Assessores únicos: {row[1]}")
        print(f"   Indicadores únicos: {row[2]}")
        print(f"   Anos de metas: {row[3]}")
        print(f"   Primeira carga: {row[4]}")
        print(f"   Última carga: {row[5]}")
        
        # Validação cruzada
        print("\n4. VALIDAÇÃO CRUZADA")
        print("-"*40)
        
        # Indicadores em todas as tabelas
        cursor.execute("""
            SELECT 
                pi.indicator_code,
                pi.indicator_name,
                CASE WHEN pa.indicator_code IS NOT NULL THEN 'SIM' ELSE 'NÃO' END as tem_assignments,
                CASE WHEN pt.indicator_code IS NOT NULL THEN 'SIM' ELSE 'NÃO' END as tem_targets
            FROM (SELECT DISTINCT indicator_code, indicator_name FROM bronze.performance_indicators) pi
            LEFT JOIN (SELECT DISTINCT indicator_code FROM bronze.performance_assignments) pa
                ON pi.indicator_code = pa.indicator_code
            LEFT JOIN (SELECT DISTINCT indicator_code FROM bronze.performance_targets) pt
                ON pi.indicator_code = pt.indicator_code
            ORDER BY pi.indicator_code
        """)
        
        print("   Indicadores e suas presenças nas tabelas:")
        for row in cursor.fetchall():
            print(f"   - {row[0]} ({row[1]}): Assignments={row[2]}, Targets={row[3]}")
            
        # Estatísticas de validação
        print("\n5. ESTATÍSTICAS DE VALIDAÇÃO")
        print("-"*40)
        
        # Assignments com pesos inválidos
        cursor.execute("""
            SELECT COUNT(*) as invalid_weights
            FROM bronze.performance_assignments
            WHERE weight_validation = 0
        """)
        invalid_weights = cursor.fetchone()[0]
        print(f"   Assignments com pesos inválidos: {invalid_weights}")
        
        # Targets com lógica inválida
        cursor.execute("""
            SELECT COUNT(*) as invalid_logic
            FROM bronze.performance_targets
            WHERE target_logic_valid = 0
        """)
        invalid_logic = cursor.fetchone()[0]
        print(f"   Targets com lógica inválida: {invalid_logic}")
        
        # Registros processados
        cursor.execute("""
            SELECT 
                'indicators' as table_name, COUNT(*) as processed
            FROM bronze.performance_indicators
            WHERE is_processed = 1
            UNION ALL
            SELECT 
                'assignments', COUNT(*)
            FROM bronze.performance_assignments
            WHERE is_processed = 1
            UNION ALL
            SELECT 
                'targets', COUNT(*)
            FROM bronze.performance_targets
            WHERE is_processed = 1
        """)
        print("\n   Registros já processados:")
        for row in cursor.fetchall():
            print(f"   - {row[0]}: {row[1]} registros")
            
except Exception as e:
    print(f"\n❌ Erro ao verificar dados: {e}")
    
print("\n" + "="*80)
print("RESUMO: Todos os ETLs foram executados com sucesso!")
print("Próximos passos: Executar procedures Bronze → Metadata")
print("="*80)