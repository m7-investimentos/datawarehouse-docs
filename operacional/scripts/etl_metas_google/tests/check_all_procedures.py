#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar todas as procedures de bronze to silver
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
    """Verifica procedures relacionadas a performance"""
    print("=== VERIFICANDO PROCEDURES DE PERFORMANCE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Buscar procedures relacionadas a performance, assignments, targets
        cursor.execute("""
            SELECT 
                ROUTINE_SCHEMA,
                ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            AND (
                ROUTINE_NAME LIKE '%performance%' 
                OR ROUTINE_NAME LIKE '%assignment%' 
                OR ROUTINE_NAME LIKE '%target%'
                OR ROUTINE_NAME LIKE '%indicator%'
            )
            ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
        """)
        
        procedures = cursor.fetchall()
        
        if procedures:
            print(f"Procedures encontradas ({len(procedures)}):\n")
            
            current_schema = None
            for proc in procedures:
                if proc[0] != current_schema:
                    current_schema = proc[0]
                    print(f"\n[{current_schema}]")
                print(f"  - {proc[1]}")
        else:
            print("Nenhuma procedure de performance encontrada")
        
        # Verificar tabelas bronze relacionadas
        print("\n\n=== TABELAS BRONZE RELACIONADAS ===\n")
        cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'bronze'
            AND TABLE_NAME LIKE '%performance%'
            ORDER BY TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        if tables:
            print("Tabelas bronze:")
            for table in tables:
                print(f"  - bronze.{table[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()