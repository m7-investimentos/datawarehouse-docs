#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para processar todos os indicadores individualmente
"""

import os
import pyodbc
from pathlib import Path
from dotenv import load_dotenv

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
    """Processa cada load_id individualmente"""
    print("=== PROCESSAMENTO INDIVIDUAL DE INDICADORES ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Listar todos os load_ids não processados
        cursor.execute("""
            SELECT DISTINCT load_id, indicator_code
            FROM bronze.performance_indicators
            WHERE is_processed = 0
            ORDER BY load_id
        """)
        
        unprocessed = cursor.fetchall()
        
        if not unprocessed:
            print("✓ Nenhum registro para processar")
            return
        
        print(f"Encontrados {len(unprocessed)} registros não processados:\n")
        
        success_count = 0
        error_count = 0
        
        # 2. Processar cada load_id individualmente
        for load_id, indicator_code in unprocessed:
            print(f"\nProcessando load_id {load_id} ({indicator_code})...")
            
            try:
                # Executar procedure para este load_id específico
                cursor.execute("""
                    EXEC bronze.prc_process_indicators_to_silver 
                        @load_id = ?,
                        @debug_mode = 0
                """, load_id)
                
                conn.commit()
                print(f"  ✓ Processado com sucesso")
                success_count += 1
                
            except Exception as e:
                error_msg = str(e)
                if "Nenhuma carga pendente" in error_msg:
                    print(f"  ℹ️  Já processado anteriormente")
                else:
                    print(f"  ✗ Erro: {error_msg}")
                    error_count += 1
                conn.rollback()
        
        # 3. Resumo final
        print(f"\n{'='*50}")
        print("RESUMO DO PROCESSAMENTO:")
        print(f"  Sucessos: {success_count}")
        print(f"  Erros: {error_count}")
        print(f"  Total: {len(unprocessed)}")
        
        # 4. Verificar resultado final na Silver
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN is_active = 1 THEN 1 END) as active
            FROM silver.performance_indicators
        """)
        
        result = cursor.fetchone()
        print(f"\nResultado na Silver:")
        print(f"  Total de indicadores: {result[0]}")
        print(f"  Indicadores ativos: {result[1]}")
        
        # 5. Mostrar indicadores na Silver
        cursor.execute("""
            SELECT indicator_code, indicator_name, category, unit, is_active
            FROM silver.performance_indicators
            ORDER BY indicator_code
        """)
        
        print(f"\nIndicadores na Silver:")
        print("-" * 80)
        print(f"{'Code':<15} {'Name':<30} {'Category':<15} {'Unit':<10} {'Active':<6}")
        print("-" * 80)
        
        for row in cursor.fetchall():
            active = 'Sim' if row[4] else 'Não'
            print(f"{row[0]:<15} {row[1][:30]:<30} {row[2]:<15} {row[3]:<10} {active:<6}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n✗ ERRO: {e}")

if __name__ == '__main__':
    main()