#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para encontrar onde está o erro de valid_from
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
    """Encontra onde está o erro"""
    print("=== PROCURANDO ERRO VALID_FROM ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Verificar se há views ou funções que referenciam valid_from incorretamente
        print("1. Verificando views que referenciam performance_assignments:")
        cursor.execute("""
            SELECT 
                s.name as object_name,
                s.type_desc,
                m.definition
            FROM sys.sql_modules m
            JOIN sys.objects s ON m.object_id = s.object_id
            WHERE m.definition LIKE '%performance_assignments%'
              AND m.definition LIKE '%valid_from%'
              AND s.type IN ('V', 'IF', 'FN', 'TF')
        """)
        
        for row in cursor.fetchall():
            print(f"\n   {row[1]}: {row[0]}")
            # Procurar por valid_from sem alias
            lines = row[2].split('\n')
            for i, line in enumerate(lines):
                if 'valid_from' in line.lower():
                    # Verificar se é problemático
                    if not any(prefix in line.lower() for prefix in ['.valid_from', 'as valid_from']):
                        print(f"      Linha suspeita: {line.strip()}")
        
        # 2. Testar a procedure em modo mais detalhado
        print("\n2. Executando procedure sem capturar mensagens:")
        try:
            cursor.execute("EXEC bronze.prc_bronze_to_silver_assignments @debug = 0")
            conn.commit()
            print("   ✓ Procedure executada com sucesso!")
            
            # Verificar resultados
            cursor.execute("SELECT COUNT(*) FROM silver.performance_assignments WHERE bronze_load_id > 0")
            count = cursor.fetchone()[0]
            print(f"   Registros na Silver: {count}")
            
        except Exception as e:
            print(f"   ✗ Erro na execução: {e}")
            
            # Tentar encontrar a linha exata do erro
            if "valid_from" in str(e):
                print("\n   Analisando definição da procedure para encontrar o erro...")
                cursor.execute("""
                    SELECT OBJECT_DEFINITION(OBJECT_ID('bronze.prc_bronze_to_silver_assignments'))
                """)
                
                proc_def = cursor.fetchone()[0]
                lines = proc_def.split('\n')
                
                # Procurar por queries problemáticas
                in_query = False
                query_lines = []
                
                for i, line in enumerate(lines):
                    line_lower = line.lower().strip()
                    
                    # Detectar início de query
                    if any(keyword in line_lower for keyword in ['select', 'update', 'insert', 'with']):
                        in_query = True
                        query_lines = [line]
                    elif in_query:
                        query_lines.append(line)
                        
                        # Detectar fim de query
                        if ';' in line or (i < len(lines)-1 and any(keyword in lines[i+1].lower() for keyword in ['select', 'update', 'insert', 'with', 'end', 'if', 'begin'])):
                            # Verificar se a query tem valid_from sem alias
                            query_text = '\n'.join(query_lines)
                            if 'valid_from' in query_text.lower():
                                # Procurar por valid_from problemático
                                for ql in query_lines:
                                    if 'valid_from' in ql.lower() and not any(prefix in ql.lower() for prefix in ['.valid_from', 'as valid_from', 'valid_from date', 'valid_from)', 'valid_from,']):
                                        print(f"\n   Query problemática encontrada na linha ~{i-len(query_lines)+1}:")
                                        print("   " + "-"*60)
                                        for ql in query_lines[:5]:  # Primeiras 5 linhas
                                            print("   " + ql.rstrip())
                                        print("   ...")
                                        break
                            in_query = False
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()