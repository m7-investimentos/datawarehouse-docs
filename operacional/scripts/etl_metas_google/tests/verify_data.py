#!/usr/bin/env python3
"""
Script para verificar se os dados foram carregados na tabela Bronze
"""

import os
import pyodbc
from dotenv import load_dotenv
from pathlib import Path

# Diretórios
BASE_DIR = Path(__file__).resolve().parent
CREDENTIALS_DIR = BASE_DIR / 'credentials'

# Carregar variáveis de ambiente do arquivo .env no diretório credentials
load_dotenv(CREDENTIALS_DIR / '.env')

# Configuração da conexão
db_server = os.getenv('DB_SERVER')
db_database = os.getenv('DB_DATABASE')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')

# Remover chaves extras do driver
driver_clean = db_driver.strip('{}')

# Conectar ao banco
conn_str = (
    f"DRIVER={{{driver_clean}}};"
    f"SERVER={db_server};"
    f"DATABASE={db_database};"
    f"UID={db_username};"
    f"PWD={db_password};"
    f"TrustServerCertificate=yes"
)

print("Verificando dados na tabela bronze.performance_indicators...")
print("=" * 60)

try:
    conn = pyodbc.connect(conn_str, timeout=10)
    cursor = conn.cursor()
    
    # Verificar quantidade de registros
    cursor.execute("SELECT COUNT(*) FROM bronze.performance_indicators")
    count = cursor.fetchone()[0]
    print(f"Total de registros na tabela: {count}")
    
    if count > 0:
        print("\nPrimeiros 5 registros:")
        print("-" * 60)
        cursor.execute("""
            SELECT TOP 5 
                indicator_code, 
                indicator_name, 
                category, 
                unit,
                load_timestamp
            FROM bronze.performance_indicators 
            ORDER BY load_timestamp DESC
        """)
        
        rows = cursor.fetchall()
        for row in rows:
            print(f"Código: {row[0]}")
            print(f"Nome: {row[1]}")
            print(f"Categoria: {row[2]}")
            print(f"Unidade: {row[3]}")
            print(f"Timestamp: {row[4]}")
            print("-" * 30)
    else:
        print("❌ Tabela está vazia!")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Erro ao consultar banco: {e}")