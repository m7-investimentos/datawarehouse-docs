#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para aplicar a correção completa da procedure
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
    """Aplica a correção completa da procedure"""
    print("=== APLICANDO CORREÇÃO COMPLETA DA PROCEDURE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Ler o arquivo SQL corrigido
        sql_file = Path('/Users/bchiaramonti/Documents/1_Projects/ProjetoAlicerce/datawarehouse-docs/operacional/queries/bronze/QRY-ASS-003-prc_bronze_to_silver_assignments.sql')
        
        print(f"Lendo arquivo: {sql_file}")
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Executar o script SQL
        print("\nExecutando script SQL...")
        
        # Dividir por GO statements
        sql_statements = sql_content.split('\nGO\n')
        
        for i, statement in enumerate(sql_statements):
            if statement.strip():
                try:
                    cursor.execute(statement)
                    print(f"  ✓ Statement {i+1} executado")
                except Exception as e:
                    if "Violation of PRIMARY KEY" in str(e):
                        print(f"  ℹ️  Statement {i+1}: Chave já existe")
                    else:
                        print(f"  ✗ Statement {i+1} erro: {e}")
        
        conn.commit()
        print("\n✓ Procedure atualizada com sucesso!")
        
        # Testar a procedure
        print("\n=== TESTANDO A PROCEDURE CORRIGIDA ===")
        
        # Verificar registros não processados
        cursor.execute("""
            SELECT COUNT(*) FROM bronze.performance_assignments WHERE is_processed = 0
        """)
        count = cursor.fetchone()[0]
        print(f"\nRegistros não processados: {count}")
        
        if count > 0:
            print("\nExecutando procedure...")
            try:
                cursor.execute("EXEC bronze.prc_bronze_to_silver_assignments @debug = 0")
                conn.commit()
                print("✓ Procedure executada com sucesso!")
                
                # Verificar resultados
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT crm_id) as assessores,
                        COUNT(DISTINCT indicator_id) as indicadores
                    FROM silver.performance_assignments
                    WHERE bronze_load_id > 0
                """)
                
                result = cursor.fetchone()
                print(f"\nResultados na Silver:")
                print(f"  Total: {result[0]} registros")
                print(f"  Assessores: {result[1]}")
                print(f"  Indicadores: {result[2]}")
                
                # Verificar Bronze processado
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processados,
                        SUM(CASE WHEN processing_status = 'SUCCESS' THEN 1 ELSE 0 END) as sucesso,
                        SUM(CASE WHEN processing_status = 'WARNING' THEN 1 ELSE 0 END) as avisos
                    FROM bronze.performance_assignments
                """)
                
                result = cursor.fetchone()
                print(f"\nStatus Bronze:")
                print(f"  Total: {result[0]}")
                print(f"  Processados: {result[1]}")
                print(f"  Sucesso: {result[2]}")
                print(f"  Avisos: {result[3]}")
                
            except Exception as e:
                print(f"✗ Erro ao executar procedure: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()