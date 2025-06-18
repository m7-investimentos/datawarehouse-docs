#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para testar diferentes formas de executar a procedure
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
    """Testa diferentes formas de executar a procedure"""
    print("=== TESTANDO EXECUÇÃO DA PROCEDURE ===\n")
    
    try:
        conn = connect_to_database()
        
        # Teste 1: Execução sem parâmetros
        print("1. Executando sem parâmetros:")
        cursor1 = conn.cursor()
        try:
            cursor1.execute("EXEC bronze.prc_bronze_to_silver_assignments")
            conn.commit()
            print("   ✓ Sucesso!")
        except Exception as e:
            print(f"   ✗ Erro: {e}")
            conn.rollback()
        cursor1.close()
        
        # Teste 2: Com todos os parâmetros default
        print("\n2. Executando com parâmetros default explícitos:")
        cursor2 = conn.cursor()
        try:
            cursor2.execute("""
                EXEC bronze.prc_bronze_to_silver_assignments 
                    @load_id = NULL,
                    @validate_weights = 1,
                    @force_update = 0,
                    @debug = 0
            """)
            conn.commit()
            print("   ✓ Sucesso!")
        except Exception as e:
            print(f"   ✗ Erro: {e}")
            conn.rollback()
        cursor2.close()
        
        # Teste 3: Com debug desativado e validate_weights desativado
        print("\n3. Executando sem validações:")
        cursor3 = conn.cursor()
        try:
            cursor3.execute("""
                EXEC bronze.prc_bronze_to_silver_assignments 
                    @validate_weights = 0,
                    @debug = 0
            """)
            conn.commit()
            print("   ✓ Sucesso!")
        except Exception as e:
            print(f"   ✗ Erro: {e}")
            conn.rollback()
        cursor3.close()
        
        # Teste 4: Usando EXECUTE AS
        print("\n4. Executando com EXECUTE AS:")
        cursor4 = conn.cursor()
        try:
            cursor4.execute("""
                DECLARE @return_value INT;
                EXEC @return_value = bronze.prc_bronze_to_silver_assignments;
                SELECT @return_value as ReturnValue;
            """)
            result = cursor4.fetchone()
            if result:
                print(f"   Return value: {result[0]}")
            conn.commit()
            print("   ✓ Sucesso!")
        except Exception as e:
            print(f"   ✗ Erro: {e}")
            # Tentar capturar mais detalhes
            if hasattr(e, 'args') and len(e.args) > 1:
                print(f"   Detalhes: {e.args[1]}")
            conn.rollback()
        cursor4.close()
        
        # Verificar resultados finais
        print("\n5. Verificando resultados:")
        cursor5 = conn.cursor()
        
        # Bronze
        cursor5.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processados
            FROM bronze.performance_assignments
        """)
        result = cursor5.fetchone()
        print(f"   Bronze: {result[1]} de {result[0]} processados")
        
        # Silver
        cursor5.execute("SELECT COUNT(*) FROM silver.performance_assignments")
        count = cursor5.fetchone()[0]
        print(f"   Silver: {count} registros")
        
        cursor5.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO GERAL: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()