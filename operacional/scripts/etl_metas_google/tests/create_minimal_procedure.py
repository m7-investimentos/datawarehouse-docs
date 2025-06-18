#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para criar uma versão mínima da procedure para teste
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
    """Cria procedure mínima"""
    print("=== CRIANDO PROCEDURE MÍNIMA PARA TESTE ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Criar procedure de teste
        print("1. Criando procedure de teste:")
        
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'test_assignments_minimal')
                DROP PROCEDURE bronze.test_assignments_minimal;
        """)
        
        cursor.execute("""
            CREATE PROCEDURE bronze.test_assignments_minimal
            AS
            BEGIN
                SET NOCOUNT ON;
                
                PRINT 'Iniciando teste...';
                
                -- Criar tabela staging
                CREATE TABLE #staging (
                    crm_id VARCHAR(20),
                    indicator_id INT,
                    valid_from DATE,
                    bronze_load_id INT
                );
                
                -- Popular com dados de teste
                INSERT INTO #staging (crm_id, indicator_id, valid_from, bronze_load_id)
                SELECT TOP 5
                    b.crm_id,
                    1 as indicator_id,
                    TRY_CAST(b.valid_from AS DATE),
                    b.load_id
                FROM bronze.performance_assignments b
                WHERE b.is_processed = 0;
                
                -- Verificar dados
                DECLARE @count INT;
                SELECT @count = COUNT(*) FROM #staging;
                PRINT CONCAT('Registros na staging: ', @count);
                
                -- Query problemática
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT crm_id, valid_from
                    FROM #staging
                ) AS x;
                
                PRINT 'Teste concluído!';
            END
        """)
        
        print("   ✓ Procedure criada")
        
        # Executar procedure de teste
        print("\n2. Executando procedure de teste:")
        try:
            cursor.execute("EXEC bronze.test_assignments_minimal")
            
            # Capturar mensagens
            while cursor.nextset():
                try:
                    if cursor.description:
                        rows = cursor.fetchall()
                        for row in rows:
                            print(f"   Resultado: {row}")
                except:
                    pass
            
            print("   ✓ Procedure executada com sucesso!")
            
        except Exception as e:
            print(f"   ✗ Erro: {e}")
        
        # Limpar
        cursor.execute("DROP PROCEDURE bronze.test_assignments_minimal")
        
        # Testar se o problema é específico do nome da coluna
        print("\n3. Testando se é problema com o nome 'valid_from':")
        
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'test_valid_from_name')
                DROP PROCEDURE bronze.test_valid_from_name;
        """)
        
        cursor.execute("""
            CREATE PROCEDURE bronze.test_valid_from_name
            AS
            BEGIN
                -- Testar se valid_from é palavra reservada ou tem algum conflito
                DECLARE @sql NVARCHAR(MAX);
                
                -- Teste 1: SELECT direto
                SELECT TOP 1 valid_from FROM bronze.performance_assignments;
                
                -- Teste 2: Com alias
                SELECT TOP 1 b.valid_from FROM bronze.performance_assignments b;
                
                -- Teste 3: Em subquery
                SELECT * FROM (
                    SELECT TOP 1 valid_from FROM bronze.performance_assignments
                ) AS x;
                
                PRINT 'Todos os testes passaram!';
            END
        """)
        
        try:
            cursor.execute("EXEC bronze.test_valid_from_name")
            print("   ✓ Nenhum problema com o nome 'valid_from'")
        except Exception as e:
            print(f"   ✗ Erro com 'valid_from': {e}")
        
        cursor.execute("DROP PROCEDURE bronze.test_valid_from_name")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()