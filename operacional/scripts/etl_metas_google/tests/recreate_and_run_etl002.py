#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para recriar tabela Bronze e executar ETL-002
"""

import os
import sys
import pyodbc
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Configuração
BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = BASE_DIR / 'credentials'
QUERIES_DIR = BASE_DIR.parent.parent / 'queries'
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

def recreate_bronze_table():
    """Recria a tabela Bronze com as colunas corretas"""
    print("\n1. Recriando tabela Bronze...")
    
    sql_file = QUERIES_DIR / 'bronze' / 'QRY-ASS-001-create_bronze_performance_assignments.sql'
    
    if not sql_file.exists():
        print(f"   ✗ Arquivo SQL não encontrado: {sql_file}")
        return False
    
    try:
        # Ler arquivo SQL
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Conectar ao banco
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Executar statements separados por GO
        statements = sql_content.split('\nGO\n')
        
        for statement in statements:
            if statement.strip():
                try:
                    cursor.execute(statement)
                    conn.commit()
                except Exception as e:
                    if "já existe" not in str(e):
                        print(f"   Aviso: {str(e)[:100]}")
        
        print("   ✓ Tabela Bronze recriada com sucesso")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ✗ Erro ao recriar tabela: {e}")
        return False

def run_etl():
    """Executa o ETL-002"""
    print("\n2. Executando ETL-002...")
    
    try:
        os.chdir(BASE_DIR)
        result = subprocess.run(
            [sys.executable, 'etl_002_assignments.py'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("   ✓ ETL executado com sucesso")
            return True
        else:
            print("   ✗ Erro no ETL:")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"   ✗ Erro ao executar ETL: {e}")
        return False

def verify_data():
    """Verifica dados carregados"""
    print("\n3. Verificando dados...")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar Bronze
        cursor.execute("SELECT COUNT(*) FROM bronze.performance_assignments")
        bronze_count = cursor.fetchone()[0]
        print(f"   Bronze: {bronze_count} registros")
        
        if bronze_count > 0:
            # Executar procedure
            print("\n4. Executando procedure Bronze to Silver...")
            cursor.execute("EXEC bronze.prc_bronze_to_silver_assignments")
            conn.commit()
            print("   ✓ Procedure executada")
            
            # Verificar Silver
            cursor.execute("SELECT COUNT(*) FROM silver.performance_assignments")
            silver_count = cursor.fetchone()[0]
            print(f"   Silver: {silver_count} registros")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ✗ Erro: {e}")

def main():
    """Executa o processo completo"""
    print("="*60)
    print("RECREATE AND RUN ETL-002")
    print("="*60)
    
    # 1. Recriar tabela Bronze
    if not recreate_bronze_table():
        return
    
    # 2. Executar ETL
    if not run_etl():
        return
    
    # 3. Verificar dados
    verify_data()
    
    print("\n" + "="*60)
    print("PROCESSO CONCLUÍDO")
    print("="*60)

if __name__ == '__main__':
    main()