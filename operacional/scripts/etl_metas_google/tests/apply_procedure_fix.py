#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para aplicar a correção da procedure
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
    """Aplica a correção da procedure"""
    print("=== APLICANDO CORREÇÃO DA PROCEDURE ===\n")
    
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
        
        # Verificar se a correção foi aplicada
        print("\nVerificando a correção...")
        cursor.execute("""
            SELECT OBJECT_DEFINITION(OBJECT_ID('bronze.prc_bronze_to_silver_assignments'))
        """)
        
        proc_def = cursor.fetchone()[0]
        if 'distinct_errors' in proc_def:
            print("✓ Correção confirmada - a procedure contém 'distinct_errors'")
        else:
            print("✗ A correção não foi aplicada corretamente")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()