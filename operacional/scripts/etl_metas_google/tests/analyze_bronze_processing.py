#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para analisar o estado de processamento do Bronze
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
    """Analisa estado de processamento"""
    print("=== ANÁLISE DO BRONZE - PROCESSAMENTO ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar estado de processamento
        cursor.execute("""
            SELECT 
                load_id,
                indicator_code,
                indicator_name,
                is_processed,
                processing_status,
                processing_notes,
                load_timestamp
            FROM bronze.performance_indicators
            ORDER BY load_id DESC
        """)
        
        records = cursor.fetchall()
        
        print(f"Total de registros no Bronze: {len(records)}\n")
        print("Estado de cada registro:")
        print("-" * 120)
        print(f"{'Load ID':<10} {'Code':<15} {'Name':<30} {'Processed':<10} {'Status':<10} {'Notes':<40}")
        print("-" * 120)
        
        for rec in records:
            processed = 'Sim' if rec[3] else 'Não'
            status = rec[4] or '-'
            notes = (rec[5][:37] + '...') if rec[5] and len(rec[5]) > 40 else (rec[5] or '-')
            
            print(f"{rec[0]:<10} {rec[1]:<15} {rec[2][:30]:<30} {processed:<10} {status:<10} {notes:<40}")
        
        # Resumo
        cursor.execute("""
            SELECT 
                is_processed,
                COUNT(*) as total,
                MAX(load_id) as max_load_id
            FROM bronze.performance_indicators
            GROUP BY is_processed
        """)
        
        print("\n\nRESUMO:")
        print("-" * 40)
        for row in cursor.fetchall():
            status = "Processados" if row[0] else "Não processados"
            print(f"{status}: {row[1]} registros (último load_id: {row[2]})")
        
        # Verificar último processamento
        cursor.execute("""
            SELECT TOP 1 
                processing_date,
                COUNT(*) OVER() as total_processed
            FROM bronze.performance_indicators
            WHERE is_processed = 1
            ORDER BY processing_date DESC
        """)
        
        result = cursor.fetchone()
        if result:
            print(f"\nÚltimo processamento: {result[0]}")
            print(f"Total de registros processados: {result[1]}")
        
        # Verificar se há registros para processar
        cursor.execute("""
            SELECT MAX(load_id)
            FROM bronze.performance_indicators
            WHERE is_processed = 0
        """)
        
        pending_load_id = cursor.fetchone()[0]
        if pending_load_id:
            print(f"\n⚠️  Load ID pendente para processamento: {pending_load_id}")
        else:
            print("\n✓ Nenhum registro pendente para processamento")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()