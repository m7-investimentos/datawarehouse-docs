#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar o status completo do ETL-002
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
    """Verifica status completo do ETL-002"""
    print("="*60)
    print("VERIFICAÇÃO COMPLETA ETL-002 - ASSIGNMENTS")
    print("="*60)
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Status Bronze
        print("\n1. TABELA BRONZE:")
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT crm_id) as assessores,
                   COUNT(DISTINCT indicator_code) as indicadores,
                   MIN(load_timestamp) as primeira_carga,
                   MAX(load_timestamp) as ultima_carga
            FROM bronze.performance_assignments
        """)
        
        result = cursor.fetchone()
        print(f"   Total de registros: {result[0]}")
        print(f"   Assessores únicos: {result[1]}")
        print(f"   Indicadores únicos: {result[2]}")
        print(f"   Primeira carga: {result[3]}")
        print(f"   Última carga: {result[4]}")
        
        # Verificar distribuição por tipo
        cursor.execute("""
            SELECT indicator_type, COUNT(*) as total
            FROM bronze.performance_assignments
            GROUP BY indicator_type
            ORDER BY total DESC
        """)
        
        print("\n   Distribuição por tipo:")
        for row in cursor.fetchall():
            print(f"     {row[0]}: {row[1]} registros")
        
        # 2. Status Silver
        print("\n2. TABELA SILVER:")
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM silver.performance_assignments
        """)
        
        silver_count = cursor.fetchone()[0]
        print(f"   Total de registros: {silver_count}")
        
        if silver_count == 0:
            print("   ⚠️  Tabela Silver está vazia - procedure precisa ser executada")
        
        # 3. Procedure
        print("\n3. PROCEDURE BRONZE TO SILVER:")
        cursor.execute("""
            SELECT 
                ROUTINE_NAME,
                CREATED,
                LAST_ALTERED
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            AND ROUTINE_SCHEMA = 'bronze'
            AND ROUTINE_NAME = 'prc_bronze_to_silver_assignments'
        """)
        
        proc = cursor.fetchone()
        if proc:
            print(f"   ✓ Procedure existe")
            print(f"   Criada em: {proc[1]}")
            print(f"   Última alteração: {proc[2]}")
        else:
            print("   ✗ Procedure NÃO existe")
        
        # 4. Análise de problemas
        print("\n4. ANÁLISE DE PROBLEMAS:")
        
        # Verificar pesos zerados
        cursor.execute("""
            SELECT COUNT(*) 
            FROM bronze.performance_assignments
            WHERE indicator_type = 'CARD' 
            AND (weight = 0 OR weight = '0' OR weight = '0.0')
        """)
        
        zero_weights = cursor.fetchone()[0]
        if zero_weights > 0:
            print(f"   ⚠️  {zero_weights} CARDs com peso zero")
        
        # Verificar indicadores não reconhecidos
        cursor.execute("""
            SELECT COUNT(*)
            FROM bronze.performance_assignments
            WHERE indicator_exists = 0
        """)
        
        unknown_indicators = cursor.fetchone()[0]
        if unknown_indicators > 0:
            print(f"   ⚠️  {unknown_indicators} registros com indicadores não reconhecidos")
        
        # 5. Resumo
        print("\n" + "="*60)
        print("RESUMO:")
        print("="*60)
        print(f"✓ ETL extraiu e carregou {result[0]} registros no Bronze")
        print(f"✓ Dados de {result[1]} assessores com {result[2]} indicadores")
        
        if silver_count == 0:
            print("\n⚠️  PRÓXIMO PASSO:")
            print("   A procedure Bronze to Silver precisa ser corrigida ou executada")
            print("   Erro atual: coluna 'valid_from' não existe na Silver")
            print("   Verifique o arquivo QRY-ASS-003-prc_bronze_to_silver_assignments.sql")
        else:
            print(f"\n✓ {silver_count} registros processados para Silver")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n✗ ERRO: {e}")

if __name__ == '__main__':
    main()