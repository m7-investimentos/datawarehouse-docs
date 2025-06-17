#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para debugar colunas do ETL-002
"""

import os
import pandas as pd
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Configurações
BASE_DIR = Path(__file__).resolve().parent.parent  # Go up one level from tests directory
CREDENTIALS_DIR = BASE_DIR / 'credentials'

# Carregar variáveis de ambiente do arquivo .env no diretório credentials
load_dotenv(CREDENTIALS_DIR / '.env')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID = '1k9gM3poSzuwEZbwaRv-AwVqQj4WjFnJE6mT5RWTvUww'
RANGE_NAME = 'Página1!A1:J5'  # Apenas algumas linhas

def get_db_engine():
    """Cria engine do banco"""
    db_config = {
        'server': os.getenv('DB_SERVER'),
        'database': os.getenv('DB_DATABASE'),
        'user': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD'),
        'driver': os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    }
    
    driver = db_config['driver'].strip('{}')
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['user']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes"
    )
    
    connection_string = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"
    return create_engine(connection_string)

def main():
    print("="*80)
    print("DEBUG: Verificando estrutura de colunas")
    print("="*80)
    
    # 1. Verificar colunas da tabela Bronze
    print("\n1. Colunas da tabela bronze.performance_assignments:")
    engine = get_db_engine()
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'bronze' 
            AND TABLE_NAME = 'performance_assignments'
            ORDER BY ORDINAL_POSITION
        """))
        
        db_columns = []
        for row in result:
            db_columns.append(row[0])
            print(f"  {row[0]} ({row[1]}) - Nullable: {row[2]}")
            
    # 2. Verificar colunas do DataFrame após transformação
    print(f"\n2. Colunas do DataFrame após load (simulação):")
    
    # Simular processo de transformação
    df_columns = [
        'cod_assessor', 'nome_assessor', 'indicator_code', 'indicator_type',
        'weight', 'valid_from', 'valid_to', 'created_by', 'approved_by', 'comments',
        'row_number', 'row_hash', 'is_current', 'weight_sum_valid', 'indicator_exists',
        'load_timestamp', 'load_source', 'is_processed', 'processing_date',
        'processing_status', 'processing_notes', 'validation_errors'
    ]
    
    for col in df_columns:
        print(f"  {col}")
        
    # 3. Comparar
    print("\n3. Análise de diferenças:")
    print(f"Colunas no banco: {len(db_columns)}")
    print(f"Colunas no DataFrame: {len(df_columns)}")
    
    missing_in_df = set(db_columns) - set(df_columns)
    missing_in_db = set(df_columns) - set(db_columns)
    
    if missing_in_df:
        print(f"\nColunas no banco mas NÃO no DataFrame:")
        for col in missing_in_df:
            print(f"  - {col}")
            
    if missing_in_db:
        print(f"\nColunas no DataFrame mas NÃO no banco:")
        for col in missing_in_db:
            print(f"  - {col}")

if __name__ == '__main__':
    main()