#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar formatos de data no Bronze
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
    """Verifica formatos de data"""
    print("=== VERIFICAÇÃO DE FORMATOS DE DATA NO BRONZE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar valores únicos de valid_from e valid_to
        print("1. Valores únicos de valid_from:")
        cursor.execute("""
            SELECT DISTINCT valid_from, COUNT(*) as qtd
            FROM bronze.performance_assignments
            GROUP BY valid_from
            ORDER BY valid_from
        """)
        
        for row in cursor.fetchall():
            print(f"   '{row[0]}' - {row[1]} registros")
        
        print("\n2. Valores únicos de valid_to:")
        cursor.execute("""
            SELECT DISTINCT valid_to, COUNT(*) as qtd
            FROM bronze.performance_assignments
            GROUP BY valid_to
            ORDER BY valid_to
        """)
        
        for row in cursor.fetchall():
            print(f"   '{row[0]}' - {row[1]} registros")
        
        # Testar conversão
        print("\n3. Teste de conversão para DATE:")
        cursor.execute("""
            SELECT TOP 5
                valid_from as original,
                TRY_CAST(valid_from AS DATE) as convertido,
                CASE 
                    WHEN TRY_CAST(valid_from AS DATE) IS NULL THEN 'ERRO'
                    ELSE 'OK'
                END as status
            FROM bronze.performance_assignments
            WHERE valid_from IS NOT NULL
        """)
        
        print(f"\n   {'Original':<20} {'Convertido':<15} {'Status':<10}")
        print("   " + "-"*45)
        
        for row in cursor.fetchall():
            print(f"   {row[0]:<20} {str(row[1]):<15} {row[2]:<10}")
        
        # Verificar registros com erro de conversão
        cursor.execute("""
            SELECT COUNT(*) 
            FROM bronze.performance_assignments
            WHERE valid_from IS NOT NULL
            AND TRY_CAST(valid_from AS DATE) IS NULL
        """)
        
        errors = cursor.fetchone()[0]
        if errors > 0:
            print(f"\n⚠️  {errors} registros com erro na conversão de valid_from")
            
            # Mostrar exemplos de erros
            cursor.execute("""
                SELECT TOP 5 valid_from
                FROM bronze.performance_assignments
                WHERE valid_from IS NOT NULL
                AND TRY_CAST(valid_from AS DATE) IS NULL
            """)
            
            print("\n   Exemplos de valores com erro:")
            for row in cursor.fetchall():
                print(f"   - '{row[0]}'")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()