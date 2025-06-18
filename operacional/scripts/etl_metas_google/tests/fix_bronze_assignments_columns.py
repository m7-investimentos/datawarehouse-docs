#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para adicionar colunas faltantes na tabela bronze.performance_assignments
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

def add_missing_columns():
    """Adiciona colunas faltantes"""
    print("=== ADICIONANDO COLUNAS FALTANTES ===\n")
    
    columns_to_add = [
        ('created_by', 'VARCHAR(200)'),
        ('approved_by', 'VARCHAR(200)'),
        ('comments', 'VARCHAR(1000)')
    ]
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        for col_name, col_type in columns_to_add:
            # Verificar se coluna já existe
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = 'bronze' 
                AND TABLE_NAME = 'performance_assignments' 
                AND COLUMN_NAME = ?
            """, col_name)
            
            exists = cursor.fetchone()[0] > 0
            
            if not exists:
                # Adicionar coluna
                sql = f"ALTER TABLE bronze.performance_assignments ADD {col_name} {col_type} NULL"
                cursor.execute(sql)
                conn.commit()
                print(f"✓ Coluna {col_name} adicionada com sucesso")
            else:
                print(f"ℹ️  Coluna {col_name} já existe")
        
        # Verificar estrutura final
        print("\n=== ESTRUTURA ATUALIZADA ===\n")
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'bronze' 
            AND TABLE_NAME = 'performance_assignments'
            AND COLUMN_NAME IN ('created_by', 'approved_by', 'comments')
            ORDER BY ORDINAL_POSITION
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]}({row[2]})")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ ERRO: {e}")
        return False

def main():
    """Executa correção"""
    print("="*60)
    print("FIX BRONZE ASSIGNMENTS - COLUNAS FALTANTES")
    print("="*60)
    
    if add_missing_columns():
        print("\n✓ Colunas adicionadas com sucesso!")
        print("\nAgora você pode executar novamente o pipeline ETL-002")
    else:
        print("\n✗ Erro ao adicionar colunas")

if __name__ == '__main__':
    main()