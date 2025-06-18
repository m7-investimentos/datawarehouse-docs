#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para testar a procedure corrigida
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
    """Testa a procedure corrigida"""
    print("=== TESTE DA PROCEDURE CORRIGIDA ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Verificar registros não processados
        print("1. Verificando registros Bronze não processados:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT load_id) as cargas,
                MIN(load_timestamp) as primeira_carga,
                MAX(load_timestamp) as ultima_carga
            FROM bronze.performance_assignments
            WHERE is_processed = 0
        """)
        
        result = cursor.fetchone()
        print(f"   Total: {result[0]} registros")
        print(f"   Cargas: {result[1]}")
        print(f"   Primeira: {result[2]}")
        print(f"   Última: {result[3]}")
        
        # 2. Executar procedure corrigida
        print("\n2. Executando procedure bronze.prc_bronze_to_silver_assignments...")
        print("   (Modo debug ativado para ver detalhes)")
        
        cursor.execute("EXEC bronze.prc_bronze_to_silver_assignments @debug = 1")
        
        # Capturar mensagens de debug
        messages = []
        while cursor.nextset():
            try:
                if cursor.description:
                    rows = cursor.fetchall()
                    for row in rows:
                        messages.append(row)
            except:
                pass
        
        conn.commit()
        print("   ✓ Procedure executada com sucesso!")
        
        # 3. Verificar resultados
        print("\n3. Verificando resultados:")
        
        # Registros processados no Bronze
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processados,
                SUM(CASE WHEN processing_status = 'SUCCESS' THEN 1 ELSE 0 END) as sucesso,
                SUM(CASE WHEN processing_status = 'WARNING' THEN 1 ELSE 0 END) as avisos,
                SUM(CASE WHEN processing_status = 'ERROR' THEN 1 ELSE 0 END) as erros
            FROM bronze.performance_assignments
        """)
        
        result = cursor.fetchone()
        print(f"\n   Bronze:")
        print(f"   - Total: {result[0]}")
        print(f"   - Processados: {result[1]}")
        print(f"   - Sucesso: {result[2]}")
        print(f"   - Avisos: {result[3]}")
        print(f"   - Erros: {result[4]}")
        
        # Registros na Silver
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT crm_id) as assessores,
                COUNT(DISTINCT indicator_id) as indicadores,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as ativos
            FROM silver.performance_assignments
        """)
        
        result = cursor.fetchone()
        print(f"\n   Silver:")
        print(f"   - Total: {result[0]}")
        print(f"   - Assessores: {result[1]}")
        print(f"   - Indicadores: {result[2]}")
        print(f"   - Ativos: {result[3]}")
        
        # Validação de pesos
        cursor.execute("""
            WITH weight_check AS (
                SELECT 
                    crm_id,
                    SUM(indicator_weight) as total_weight
                FROM silver.performance_assignments
                WHERE is_active = 1
                  AND indicator_id IN (SELECT indicator_id FROM silver.performance_indicators WHERE category = 'CARD')
                GROUP BY crm_id
            )
            SELECT 
                COUNT(*) as total_assessores,
                SUM(CASE WHEN ABS(total_weight - 100) < 0.01 THEN 1 ELSE 0 END) as pesos_validos,
                SUM(CASE WHEN ABS(total_weight - 100) >= 0.01 THEN 1 ELSE 0 END) as pesos_invalidos,
                MIN(total_weight) as min_peso,
                MAX(total_weight) as max_peso,
                AVG(total_weight) as media_peso
            FROM weight_check
        """)
        
        result = cursor.fetchone()
        print(f"\n   Validação de pesos:")
        print(f"   - Total assessores: {result[0]}")
        print(f"   - Pesos válidos (100%): {result[1]}")
        print(f"   - Pesos inválidos: {result[2]}")
        print(f"   - Peso mínimo: {result[3]:.2f}%")
        print(f"   - Peso máximo: {result[4]:.2f}%")
        print(f"   - Peso médio: {result[5]:.2f}%")
        
        # Mostrar alguns exemplos
        if result[2] > 0:  # Se há pesos inválidos
            print("\n   Exemplos de assessores com peso inválido:")
            cursor.execute("""
                WITH weight_check AS (
                    SELECT 
                        a.crm_id,
                        p.nome_pessoa,
                        SUM(a.indicator_weight) as total_weight
                    FROM silver.performance_assignments a
                    LEFT JOIN silver.dim_pessoas p ON a.crm_id = p.cod_assessor
                    WHERE a.is_active = 1
                      AND a.indicator_id IN (SELECT indicator_id FROM silver.performance_indicators WHERE category = 'CARD')
                    GROUP BY a.crm_id, p.nome_pessoa
                )
                SELECT TOP 5 
                    crm_id,
                    nome_pessoa,
                    total_weight
                FROM weight_check
                WHERE ABS(total_weight - 100) >= 0.01
                ORDER BY total_weight
            """)
            
            for row in cursor.fetchall():
                print(f"   - {row[0]} ({row[1]}): {row[2]:.2f}%")
        
        cursor.close()
        conn.close()
        
        print("\n✓ Teste concluído com sucesso!")
        
    except Exception as e:
        print(f"\n✗ ERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()