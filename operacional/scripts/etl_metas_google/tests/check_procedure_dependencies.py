#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar dependências da procedure
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
    """Verifica dependências"""
    print("=== VERIFICANDO DEPENDÊNCIAS DA PROCEDURE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Verificar todas as dependências da procedure
        print("1. Dependências da procedure:")
        cursor.execute("""
            SELECT 
                d.referenced_entity_name,
                d.referenced_minor_name,
                o.type_desc
            FROM sys.sql_expression_dependencies d
            LEFT JOIN sys.objects o ON d.referenced_id = o.object_id
            WHERE d.referencing_id = OBJECT_ID('bronze.prc_bronze_to_silver_assignments')
            ORDER BY d.referenced_entity_name
        """)
        
        deps = cursor.fetchall()
        for dep in deps:
            print(f"   - {dep[0]}.{dep[1] if dep[1] else ''} ({dep[2] if dep[2] else 'N/A'})")
        
        # 2. Verificar se há sinônimos
        print("\n2. Verificando sinônimos:")
        cursor.execute("""
            SELECT 
                s.name as synonym_name,
                s.base_object_name
            FROM sys.synonyms s
            WHERE s.base_object_name LIKE '%performance%'
               OR s.name LIKE '%performance%'
        """)
        
        syns = cursor.fetchall()
        if syns:
            for syn in syns:
                print(f"   - {syn[0]} -> {syn[1]}")
        else:
            print("   Nenhum sinônimo relacionado encontrado")
        
        # 3. Verificar objetos que dependem das tabelas envolvidas
        print("\n3. Objetos que dependem de performance_assignments:")
        cursor.execute("""
            SELECT DISTINCT
                o.type_desc,
                SCHEMA_NAME(o.schema_id) + '.' + o.name as object_name
            FROM sys.sql_expression_dependencies d
            JOIN sys.objects o ON d.referencing_id = o.object_id
            WHERE d.referenced_entity_name IN ('performance_assignments')
            ORDER BY o.type_desc, object_name
        """)
        
        for obj in cursor.fetchall():
            print(f"   - {obj[0]}: {obj[1]}")
        
        # 4. Verificar se há alguma função ou tipo customizado
        print("\n4. Verificando tipos customizados:")
        cursor.execute("""
            SELECT 
                t.name as type_name,
                s.name as schema_name,
                t.system_type_id,
                t.user_type_id
            FROM sys.types t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.is_user_defined = 1
        """)
        
        types = cursor.fetchall()
        if types:
            for t in types:
                print(f"   - {t[1]}.{t[0]} (system_type: {t[2]}, user_type: {t[3]})")
        else:
            print("   Nenhum tipo customizado encontrado")
        
        # 5. Testar execução linha por linha da procedure
        print("\n5. Executando procedure linha por linha para encontrar o erro:")
        
        # Obter definição da procedure
        cursor.execute("""
            SELECT OBJECT_DEFINITION(OBJECT_ID('bronze.prc_bronze_to_silver_assignments'))
        """)
        
        proc_def = cursor.fetchone()[0]
        
        # Procurar pela primeira ocorrência de valid_from que pode estar causando o erro
        lines = proc_def.split('\n')
        for i, line in enumerate(lines):
            if 'SELECT' in line.upper() and 'valid_from' in line.lower():
                # Verificar contexto
                context_start = max(0, i-5)
                context_end = min(len(lines), i+5)
                
                print(f"\n   Possível linha problemática {i}:")
                print("   " + "-"*60)
                for j in range(context_start, context_end):
                    marker = " >>> " if j == i else "     "
                    print(f"   {j:4d}{marker}{lines[j].rstrip()}")
                print("   " + "-"*60)
                
                # Parar após encontrar primeira ocorrência suspeita
                if not any(safe in line.lower() for safe in ['.valid_from', 'as valid_from', 'create table']):
                    break
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()