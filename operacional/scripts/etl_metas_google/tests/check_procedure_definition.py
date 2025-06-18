#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar a definição exata da procedure
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
    """Verifica definição da procedure"""
    print("=== VERIFICANDO DEFINIÇÃO DA PROCEDURE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Obter definição
        cursor.execute("""
            SELECT OBJECT_DEFINITION(OBJECT_ID('bronze.prc_bronze_to_silver_assignments'))
        """)
        
        proc_def = cursor.fetchone()[0]
        
        # Procurar por linhas específicas problemáticas
        lines = proc_def.split('\n')
        
        print("Procurando por potenciais problemas...\n")
        
        # 1. Verificar se a correção foi aplicada
        if 'distinct_errors' in proc_def:
            print("✓ A correção 'distinct_errors' está presente")
        else:
            print("✗ A correção 'distinct_errors' NÃO está presente")
        
        # 2. Procurar por valid_from em contextos problemáticos
        print("\nLinhas com 'valid_from' que podem ser problemáticas:")
        
        for i, line in enumerate(lines, 1):
            if 'valid_from' in line.lower():
                # Verificar contextos específicos
                line_stripped = line.strip()
                
                # Pular comentários
                if '--' in line_stripped and line_stripped.index('--') < line_stripped.index('valid_from'):
                    continue
                
                # Verificar se é uma definição de coluna (OK)
                if 'date' in line.lower() or 'varchar' in line.lower() or 'create table' in line.lower():
                    continue
                
                # Verificar se tem alias adequado
                has_alias = any(alias in line.lower() for alias in [
                    'b.valid_from', 
                    's.valid_from', 
                    'm.valid_from',
                    'valid_from as',
                    'as valid_from',
                    'cast(b.valid_from',
                    'try_cast(b.valid_from'
                ])
                
                if not has_alias and 'valid_from' in line:
                    print(f"   Linha {i}: {line_stripped[:100]}...")
        
        # 3. Verificar se há alguma subquery ou CTE mal formada
        print("\nVerificando CTEs e subqueries:")
        
        in_cte = False
        cte_name = ""
        for i, line in enumerate(lines, 1):
            if 'with' in line.lower() and ' as ' in line.lower():
                in_cte = True
                # Extrair nome da CTE
                parts = line.lower().split(' as ')
                if len(parts) > 0:
                    cte_name = parts[0].replace('with', '').strip()
                print(f"\n   CTE encontrada: {cte_name}")
            elif in_cte and ')' in line and not '(' in line:
                in_cte = False
            elif in_cte and 'valid_from' in line.lower():
                print(f"      Linha {i}: {line.strip()[:80]}...")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()