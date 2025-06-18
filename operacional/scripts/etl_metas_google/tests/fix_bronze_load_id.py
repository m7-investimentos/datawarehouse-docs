#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para corrigir load_id no Bronze e executar a procedure
"""

import os
import pyodbc
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Configuração
BASE_DIR = Path(__file__).resolve().parent
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
    """Corrige load_id e processa para Silver"""
    print("=== CORREÇÃO DE LOAD_ID E PROCESSAMENTO ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Verificar registros não processados
        cursor.execute("""
            SELECT COUNT(*), MIN(load_timestamp), MAX(load_timestamp)
            FROM bronze.performance_indicators
            WHERE is_processed = 0
        """)
        
        result = cursor.fetchone()
        unprocessed_count = result[0]
        
        if unprocessed_count == 0:
            print("✓ Nenhum registro para processar")
            return
        
        print(f"Encontrados {unprocessed_count} registros não processados")
        print(f"Período: {result[1]} até {result[2]}")
        
        # 2. Atualizar todos os registros não processados para usar o mesmo load_id
        # Usar o MAX(load_id) como o load_id comum
        print("\nAtualizando load_id para agrupar registros da mesma carga...")
        
        cursor.execute("""
            DECLARE @target_load_id INT;
            
            -- Usar o maior load_id dos registros não processados
            SELECT @target_load_id = MAX(load_id)
            FROM bronze.performance_indicators
            WHERE is_processed = 0;
            
            -- Atualizar todos os registros não processados para usar este load_id
            UPDATE bronze.performance_indicators
            SET load_id = @target_load_id
            WHERE is_processed = 0
              AND load_id != @target_load_id;
              
            SELECT @target_load_id as load_id, @@ROWCOUNT as rows_updated;
        """)
        
        result = cursor.fetchone()
        target_load_id = result[0]
        rows_updated = result[1]
        
        conn.commit()
        
        print(f"✓ Load ID unificado: {target_load_id}")
        print(f"✓ {rows_updated} registros atualizados")
        
        # 3. Executar a procedure
        print("\nExecutando procedure Bronze to Silver...")
        
        cursor.execute("EXEC bronze.prc_process_indicators_to_silver @debug_mode = 1")
        
        # Capturar mensagens da procedure
        messages = []
        while cursor.nextset():
            try:
                rows = cursor.fetchall()
                if rows:
                    for row in rows:
                        print(row)
            except:
                pass
        
        conn.commit()
        
        print("\n✓ Procedure executada com sucesso")
        
        # 4. Verificar resultado
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN is_active = 1 THEN 1 END) as active
            FROM silver.performance_indicators
        """)
        
        result = cursor.fetchone()
        print(f"\nResultado na Silver:")
        print(f"  Total de indicadores: {result[0]}")
        print(f"  Indicadores ativos: {result[1]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n✗ ERRO: {e}")

if __name__ == '__main__':
    main()