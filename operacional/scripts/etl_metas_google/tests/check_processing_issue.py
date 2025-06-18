#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar porque apenas 1 registro está sendo processado
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
    """Verifica processamento"""
    print("=== VERIFICAÇÃO DE PROCESSAMENTO ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Verificar registros não processados
        print("1. Registros no Bronze:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 0 THEN 1 ELSE 0 END) as nao_processados,
                COUNT(DISTINCT load_id) as cargas_distintas
            FROM bronze.performance_assignments
        """)
        
        result = cursor.fetchone()
        print(f"   Total: {result[0]}")
        print(f"   Não processados: {result[1]}")
        print(f"   Cargas distintas: {result[2]}")
        
        # 2. Verificar distribuição por load_id
        print("\n2. Distribuição por load_id (não processados):")
        cursor.execute("""
            SELECT 
                load_id,
                COUNT(*) as qtd,
                MIN(crm_id) as primeiro_crm,
                MAX(crm_id) as ultimo_crm
            FROM bronze.performance_assignments
            WHERE is_processed = 0
            GROUP BY load_id
            ORDER BY load_id DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   Load {row[0]}: {row[1]} registros (CRM {row[2]} até {row[3]})")
        
        # 3. Problema do MAX(load_id)
        print("\n3. Verificando MAX(load_id):")
        cursor.execute("""
            SELECT MAX(load_id)
            FROM bronze.performance_assignments
            WHERE is_processed = 0
        """)
        
        max_load_id = cursor.fetchone()[0]
        print(f"   MAX(load_id) = {max_load_id}")
        
        cursor.execute("""
            SELECT COUNT(*)
            FROM bronze.performance_assignments
            WHERE load_id = ? AND is_processed = 0
        """, max_load_id)
        
        count = cursor.fetchone()[0]
        print(f"   Registros com load_id = {max_load_id}: {count}")
        
        # 4. Solução: processar todos os não processados
        print("\n4. SOLUÇÃO - Processar todos os registros não processados:")
        print("   A procedure está pegando apenas MAX(load_id), mas cada registro tem")
        print("   um load_id diferente devido ao IDENTITY.")
        print("\n   Opções:")
        print("   a) Alterar a procedure para processar WHERE is_processed = 0")
        print("   b) Atualizar todos os registros para ter o mesmo load_id")
        print("   c) Processar cada load_id individualmente")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()