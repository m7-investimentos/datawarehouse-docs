#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnóstico final do erro valid_from
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
    """Diagnóstico final"""
    print("=== DIAGNÓSTICO FINAL - VALID_FROM ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Verificar triggers
        print("1. Verificando triggers na tabela Silver:")
        cursor.execute("""
            SELECT 
                t.name as trigger_name,
                t.is_disabled,
                m.definition
            FROM sys.triggers t
            JOIN sys.sql_modules m ON t.object_id = m.object_id
            WHERE t.parent_id = OBJECT_ID('silver.performance_assignments')
        """)
        
        triggers = cursor.fetchall()
        if triggers:
            for trigger in triggers:
                print(f"   Trigger: {trigger[0]} (Disabled: {trigger[1]})")
                if 'valid_from' in trigger[2]:
                    print("   ⚠️  Trigger contém referência a valid_from")
        else:
            print("   Nenhum trigger encontrado")
        
        # 2. Verificar constraints
        print("\n2. Verificando constraints:")
        cursor.execute("""
            SELECT 
                c.name as constraint_name,
                c.type_desc,
                c.definition
            FROM sys.check_constraints c
            WHERE c.parent_object_id = OBJECT_ID('silver.performance_assignments')
        """)
        
        constraints = cursor.fetchall()
        if constraints:
            for const in constraints:
                print(f"   {const[0]}: {const[1]}")
                print(f"   Definição: {const[2]}")
        else:
            print("   Apenas constraints padrão")
        
        # 3. Testar INSERT direto
        print("\n3. Testando INSERT direto na Silver:")
        try:
            cursor.execute("""
                INSERT INTO silver.performance_assignments (
                    crm_id,
                    indicator_id,
                    indicator_weight,
                    valid_from,
                    valid_to,
                    created_date,
                    created_by,
                    is_active,
                    bronze_load_id
                )
                SELECT TOP 1
                    '99999',
                    1,
                    0.00,
                    CAST('2025-01-01' AS DATE),
                    NULL,
                    GETDATE(),
                    'TEST',
                    1,
                    999999
                WHERE NOT EXISTS (
                    SELECT 1 FROM silver.performance_assignments 
                    WHERE crm_id = '99999' AND bronze_load_id = 999999
                )
            """)
            
            if cursor.rowcount > 0:
                print("   ✓ INSERT executado com sucesso")
                # Deletar registro de teste
                cursor.execute("DELETE FROM silver.performance_assignments WHERE bronze_load_id = 999999")
                conn.commit()
            else:
                print("   ℹ️  Registro já existe")
                
        except Exception as e:
            print(f"   ✗ Erro no INSERT: {e}")
            conn.rollback()
        
        # 4. Verificar definição exata da procedure
        print("\n4. Procurando erro específico na procedure:")
        cursor.execute("""
            SELECT OBJECT_DEFINITION(OBJECT_ID('bronze.prc_bronze_to_silver_assignments'))
        """)
        
        proc_def = cursor.fetchone()[0]
        if proc_def:
            lines = proc_def.split('\n')
            # Procurar por linhas problemáticas
            for i, line in enumerate(lines, 1):
                # Procurar por valid_from sem prefixo de tabela em contextos problemáticos
                if 'valid_from' in line.lower():
                    # Verificar se é uma referência sem alias
                    line_lower = line.lower().strip()
                    if ('select' in line_lower or 'where' in line_lower or 'group by' in line_lower) \
                       and not any(prefix in line_lower for prefix in ['.valid_from', 'as valid_from', 'valid_from date', 'valid_from)', 'valid_from,']):
                        print(f"   Linha {i}: {line.strip()}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == '__main__':
    main()