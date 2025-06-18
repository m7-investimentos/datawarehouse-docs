#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para executar a procedure Bronze to Silver para assignments
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
    """Executa a procedure e verifica resultados"""
    print("=== EXECUTANDO PROCEDURE BRONZE TO SILVER ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Executar procedure sem parâmetros
        print("Executando procedure...")
        cursor.execute("EXEC bronze.prc_bronze_to_silver_assignments")
        conn.commit()
        print("✓ Procedure executada com sucesso\n")
        
        # Verificar resultados na Silver
        print("Verificando dados na Silver...")
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT crm_id) as assessores,
                   COUNT(DISTINCT indicator_code) as indicadores
            FROM silver.performance_assignments
            WHERE is_active = 1
        """)
        
        result = cursor.fetchone()
        print(f"  Total de registros ativos: {result[0]}")
        print(f"  Total de assessores: {result[1]}")
        print(f"  Total de indicadores: {result[2]}")
        
        # Resumo por tipo
        print("\nResumo por tipo de indicador:")
        cursor.execute("""
            SELECT 
                indicator_type,
                COUNT(*) as total,
                COUNT(DISTINCT crm_id) as assessores,
                SUM(CASE WHEN indicator_type = 'CARD' THEN weight ELSE 0 END) as soma_pesos
            FROM silver.performance_assignments
            WHERE is_active = 1
            GROUP BY indicator_type
            ORDER BY indicator_type
        """)
        
        print(f"\n{'Tipo':<10} {'Total':<10} {'Assessores':<12} {'Soma Pesos':<10}")
        print("-" * 45)
        
        for row in cursor.fetchall():
            print(f"{row[0]:<10} {row[1]:<10} {row[2]:<12} {row[3] or 0:<10.1f}")
        
        # Verificar problemas de peso
        print("\nVerificando somas de pesos por assessor (apenas CARD):")
        cursor.execute("""
            SELECT 
                pa.crm_id,
                pa.nome_assessor,
                COUNT(*) as qtd_cards,
                SUM(pa.weight) as soma_pesos
            FROM silver.performance_assignments pa
            WHERE pa.is_active = 1
            AND pa.indicator_type = 'CARD'
            GROUP BY pa.crm_id, pa.nome_assessor
            HAVING SUM(pa.weight) <> 100.0
            ORDER BY pa.crm_id
        """)
        
        problemas = cursor.fetchall()
        if problemas:
            print(f"\n⚠️  Assessores com soma de pesos diferente de 100%:")
            for row in problemas:
                print(f"  {row[0]} - {row[1]}: {row[2]} CARDs, soma = {row[3]}%")
        else:
            print("\n✓ Todos os assessores têm soma de pesos = 100%")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ ERRO: {e}")

if __name__ == '__main__':
    main()