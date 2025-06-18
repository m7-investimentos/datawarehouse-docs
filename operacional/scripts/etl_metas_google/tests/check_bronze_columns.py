#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar as colunas da tabela Bronze
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
    """Verifica colunas da tabela Bronze"""
    print("=== COLUNAS DA TABELA BRONZE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Listar todas as colunas
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'bronze'
              AND TABLE_NAME = 'performance_assignments'
            ORDER BY ORDINAL_POSITION
        """)
        
        print("Colunas da tabela bronze.performance_assignments:")
        for row in cursor.fetchall():
            col_name = row[0]
            data_type = row[1]
            max_len = row[2] if row[2] else ''
            nullable = 'NULL' if row[3] == 'YES' else 'NOT NULL'
            
            if max_len:
                print(f"  {col_name} - {data_type}({max_len}) {nullable}")
            else:
                print(f"  {col_name} - {data_type} {nullable}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()