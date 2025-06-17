#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de teste para inserção na tabela bronze.performance_assignments
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent  # Go up one level from tests directory
CREDENTIALS_DIR = BASE_DIR / 'credentials'

# Carregar variáveis de ambiente do arquivo .env no diretório credentials
load_dotenv(CREDENTIALS_DIR / '.env')

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
    print("Teste de inserção na tabela bronze.performance_assignments")
    print("="*80)
    
    engine = get_db_engine()
    
    # Criar um registro de teste minimalista
    test_data = pd.DataFrame([{
        'load_timestamp': datetime.now(),
        'load_source': 'TEST:Manual',
        'cod_assessor': 'TEST001',
        'nome_assessor': 'Assessor Teste',
        'indicator_code': 'CARD_TEST',
        'indicator_type': 'CARD',
        'weight': '25.00',
        'valid_from': '2025-01-01',
        'valid_to': '',
        'created_by': 'test_script',
        'approved_by': 'test_script',
        'comments': 'Registro de teste',
        'row_number': 1,
        'row_hash': 'test_hash_123',
        'is_current': 1,
        'is_processed': 0,
        'processing_date': None,
        'processing_status': None,
        'processing_notes': None,
        'weight_sum_valid': 1,
        'indicator_exists': 1,
        'validation_errors': None
    }])
    
    print("\nDados a serem inseridos:")
    print(test_data.info())
    print("\nPrimeira linha:")
    print(test_data.iloc[0])
    
    # Tentar inserir
    try:
        with engine.begin() as conn:
            # Limpar registro de teste anterior se existir
            conn.execute(text("""
                DELETE FROM bronze.performance_assignments 
                WHERE cod_assessor = 'TEST001'
            """))
            
            # Inserir novo registro
            test_data.to_sql(
                'performance_assignments',
                conn,
                schema='bronze',
                if_exists='append',
                index=False
            )
            
            print("\n✓ Inserção bem-sucedida!")
            
            # Verificar
            result = conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM bronze.performance_assignments 
                WHERE cod_assessor = 'TEST001'
            """)).fetchone()
            
            print(f"Registros inseridos: {result.count}")
            
    except Exception as e:
        print(f"\n✗ Erro na inserção: {e}")
        print(f"Tipo do erro: {type(e)}")

if __name__ == '__main__':
    main()