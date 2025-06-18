#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar procedures existentes no banco
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
    """Verifica procedures relacionadas a indicators"""
    print("=== VERIFICANDO PROCEDURES ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Buscar todas as procedures relacionadas a indicators
        cursor.execute("""
            SELECT 
                ROUTINE_SCHEMA,
                ROUTINE_NAME,
                CREATED,
                LAST_ALTERED
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            AND (ROUTINE_NAME LIKE '%indicator%' OR ROUTINE_NAME LIKE '%bronze%silver%')
            ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
        """)
        
        procedures = cursor.fetchall()
        
        if procedures:
            print(f"Procedures encontradas ({len(procedures)}):\n")
            for proc in procedures:
                print(f"  Schema: {proc[0]}")
                print(f"  Nome: {proc[1]}")
                print(f"  Criada em: {proc[2]}")
                print(f"  Última alteração: {proc[3]}")
                print("-" * 40)
        else:
            print("Nenhuma procedure encontrada com 'indicator' ou 'bronze_silver' no nome")
        
        # Verificar especificamente no schema modelagem_b2s
        print("\n=== PROCEDURES NO SCHEMA modelagem_b2s ===\n")
        cursor.execute("""
            SELECT ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            AND ROUTINE_SCHEMA = 'modelagem_b2s'
            ORDER BY ROUTINE_NAME
        """)
        
        b2s_procs = cursor.fetchall()
        if b2s_procs:
            print(f"Procedures em modelagem_b2s ({len(b2s_procs)}):")
            for proc in b2s_procs:
                print(f"  - {proc[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()