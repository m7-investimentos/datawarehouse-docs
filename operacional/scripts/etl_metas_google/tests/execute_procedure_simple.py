#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para executar a procedure de forma simples
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
    """Executa a procedure de forma simples"""
    print("=== EXECUTANDO PROCEDURE SIMPLES ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar se há dados para processar
        cursor.execute("""
            SELECT COUNT(*) FROM bronze.performance_assignments WHERE is_processed = 0
        """)
        count = cursor.fetchone()[0]
        print(f"Registros não processados: {count}")
        
        if count == 0:
            print("Nenhum registro para processar.")
            return
        
        # Executar procedure sem debug
        print("\nExecutando procedure...")
        try:
            cursor.execute("EXEC bronze.prc_bronze_to_silver_assignments")
            conn.commit()
            print("✓ Procedure executada com sucesso!")
        except pyodbc.Error as e:
            print(f"✗ Erro SQL: {e}")
            print(f"   SQLState: {e.args[0] if e.args else 'N/A'}")
            print(f"   Message: {e.args[1] if len(e.args) > 1 else 'N/A'}")
            
            # Tentar obter mais informações
            if "valid_from" in str(e):
                print("\n   O erro está relacionado a 'valid_from'")
                print("   Verificando se há algum problema específico...")
                
                # Verificar se a view está causando o problema
                cursor2 = conn.cursor()
                cursor2.execute("""
                    SELECT COUNT(*) 
                    FROM sys.views 
                    WHERE name = 'vw_performance_assignments_current'
                """)
                if cursor2.fetchone()[0] > 0:
                    print("   - A view vw_performance_assignments_current existe")
                    
                    # Testar a view
                    try:
                        cursor2.execute("SELECT TOP 1 * FROM silver.vw_performance_assignments_current")
                        print("   - A view funciona corretamente")
                    except Exception as ve:
                        print(f"   - Erro na view: {ve}")
        
        # Verificar resultados mesmo com erro
        print("\nVerificando estado atual:")
        cursor3 = conn.cursor()
        
        # Bronze
        cursor3.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processados
            FROM bronze.performance_assignments
        """)
        result = cursor3.fetchone()
        print(f"Bronze: {result[1]} de {result[0]} processados")
        
        # Silver
        cursor3.execute("SELECT COUNT(*) FROM silver.performance_assignments")
        count = cursor3.fetchone()[0]
        print(f"Silver: {count} registros")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO GERAL: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()