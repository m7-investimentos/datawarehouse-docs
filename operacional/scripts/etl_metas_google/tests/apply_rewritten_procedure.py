#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para aplicar a procedure reescrita
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
    """Aplica a procedure reescrita"""
    print("=== APLICANDO PROCEDURE REESCRITA ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Primeiro, resetar os registros Bronze para testar novamente
        print("1. Resetando registros Bronze para novo teste:")
        cursor.execute("""
            UPDATE bronze.performance_assignments
            SET is_processed = 0,
                processing_date = NULL,
                processing_status = NULL,
                processing_notes = NULL
            WHERE load_id > 0
        """)
        rows = cursor.rowcount
        conn.commit()
        print(f"   ✓ {rows} registros resetados")
        
        # Limpar Silver para teste limpo
        print("\n2. Limpando tabela Silver para teste:")
        cursor.execute("DELETE FROM silver.performance_assignments WHERE bronze_load_id > 0")
        rows = cursor.rowcount
        conn.commit()
        print(f"   ✓ {rows} registros removidos")
        
        # Ler e executar a procedure reescrita
        sql_file = BASE_DIR / 'tests' / 'procedure_rewritten.sql'
        print(f"\n3. Lendo arquivo: {sql_file}")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Executar statements
        print("\n4. Executando SQL...")
        statements = sql_content.split('\nGO\n')
        
        for i, stmt in enumerate(statements):
            if stmt.strip():
                try:
                    cursor.execute(stmt)
                    # Capturar mensagens PRINT
                    while cursor.nextset():
                        pass
                    print(f"   ✓ Statement {i+1} executado")
                except Exception as e:
                    print(f"   ✗ Statement {i+1} erro: {e}")
        
        conn.commit()
        
        # Verificar resultados finais
        print("\n5. Verificando resultados finais:")
        
        # Bronze
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processados,
                SUM(CASE WHEN processing_status = 'SUCCESS' THEN 1 ELSE 0 END) as sucesso
            FROM bronze.performance_assignments
        """)
        result = cursor.fetchone()
        print(f"\n   Bronze:")
        print(f"   - Total: {result[0]}")
        print(f"   - Processados: {result[1]}")
        print(f"   - Sucesso: {result[2]}")
        
        # Silver
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT crm_id) as assessores,
                COUNT(DISTINCT indicator_id) as indicadores,
                MIN(valid_from) as min_date,
                MAX(valid_from) as max_date
            FROM silver.performance_assignments
            WHERE bronze_load_id > 0
        """)
        result = cursor.fetchone()
        print(f"\n   Silver:")
        print(f"   - Total: {result[0]}")
        print(f"   - Assessores: {result[1]}")
        print(f"   - Indicadores: {result[2]}")
        print(f"   - Data mínima: {result[3]}")
        print(f"   - Data máxima: {result[4]}")
        
        # Exemplo de dados
        print("\n   Amostra de dados inseridos:")
        cursor.execute("""
            SELECT TOP 5
                a.crm_id,
                p.nome_pessoa,
                i.indicator_name,
                a.indicator_weight,
                a.valid_from
            FROM silver.performance_assignments a
            INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
            LEFT JOIN silver.dim_pessoas p ON a.crm_id = p.crm_id
            WHERE a.bronze_load_id > 0
            ORDER BY a.crm_id, i.indicator_name
        """)
        
        for row in cursor.fetchall():
            print(f"   - {row[0]} ({row[1]}): {row[2]} = {row[3]}% (desde {row[4]})")
        
        cursor.close()
        conn.close()
        
        print("\n✓ Processo concluído com sucesso!")
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()