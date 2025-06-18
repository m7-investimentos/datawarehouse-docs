#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar tabelas de performance em todos os schemas
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
    """Verifica tabelas de performance em todos os schemas"""
    print("=== TABELAS DE PERFORMANCE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Buscar tabelas relacionadas a performance
        cursor.execute("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME LIKE '%performance%' 
               OR TABLE_NAME LIKE '%assignment%' 
               OR TABLE_NAME LIKE '%target%'
               OR TABLE_NAME LIKE '%indicator%'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        
        if tables:
            print(f"Tabelas encontradas ({len(tables)}):\n")
            
            current_schema = None
            for table in tables:
                if table[0] != current_schema:
                    current_schema = table[0]
                    print(f"\n[{current_schema}]")
                print(f"  - {table[1]}")
                
                # Verificar se tem dados
                try:
                    cursor2 = conn.cursor()
                    cursor2.execute(f"SELECT COUNT(*) FROM [{table[0]}].[{table[1]}]")
                    count = cursor2.fetchone()[0]
                    print(f"    ({count} registros)")
                    cursor2.close()
                except:
                    print(f"    (erro ao contar)")
        else:
            print("Nenhuma tabela de performance encontrada")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()