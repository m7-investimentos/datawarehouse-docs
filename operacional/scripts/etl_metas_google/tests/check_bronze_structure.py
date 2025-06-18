#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verifica estrutura da tabela Bronze assignments
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
    """Verifica estrutura da tabela"""
    print("=== ESTRUTURA DA TABELA BRONZE.PERFORMANCE_ASSIGNMENTS ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Listar todas as colunas
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'bronze' 
            AND TABLE_NAME = 'performance_assignments'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        
        print(f"Total de colunas: {len(columns)}\n")
        print(f"{'Coluna':<25} {'Tipo':<15} {'Tamanho':<10} {'Nullable':<10} {'Default':<20}")
        print("-" * 85)
        
        expected_columns = ['created_by', 'approved_by', 'comments']
        found_columns = []
        
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            max_length = str(col[2]) if col[2] else '-'
            nullable = col[3]
            default = str(col[4]) if col[4] else '-'
            
            print(f"{col_name:<25} {data_type:<15} {max_length:<10} {nullable:<10} {default:<20}")
            found_columns.append(col_name)
        
        # Verificar colunas faltantes
        print("\n\nCOLUNAS ESPERADAS PELO ETL MAS NÃO ENCONTRADAS:")
        missing = [col for col in expected_columns if col not in found_columns]
        if missing:
            for col in missing:
                print(f"  ✗ {col}")
        else:
            print("  ✓ Todas as colunas esperadas existem")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()