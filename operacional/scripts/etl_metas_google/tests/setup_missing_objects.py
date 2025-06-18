#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para criar objetos faltantes no banco de dados
"""

import os
import sys
import pyodbc
from pathlib import Path
from dotenv import load_dotenv

# Configuração
BASE_DIR = Path(__file__).resolve().parent
CREDENTIALS_DIR = BASE_DIR / 'credentials'
QUERIES_DIR = BASE_DIR.parent.parent / 'queries'

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

def check_object_exists(conn, object_type, schema, name):
    """Verifica se objeto existe no banco"""
    cursor = conn.cursor()
    
    if object_type == 'TABLE':
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, schema, name)
    elif object_type == 'PROCEDURE':
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE' 
            AND ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ?
        """, schema, name)
    
    exists = cursor.fetchone()[0] > 0
    cursor.close()
    return exists

def execute_sql_file(conn, file_path):
    """Executa arquivo SQL"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Dividir por GO statements
        sql_statements = sql_content.split('\nGO\n')
        
        cursor = conn.cursor()
        for statement in sql_statements:
            if statement.strip():
                try:
                    cursor.execute(statement)
                    conn.commit()
                except Exception as e:
                    if "already exists" not in str(e):
                        print(f"  Erro ao executar statement: {e}")
        
        cursor.close()
        return True
    except Exception as e:
        print(f"  Erro ao executar arquivo: {e}")
        return False

def main():
    """Cria objetos faltantes"""
    print("="*60)
    print("SETUP DE OBJETOS FALTANTES")
    print("="*60)
    
    # Lista de objetos para criar
    objects_to_create = [
        # Tabelas Bronze
        {
            'type': 'TABLE',
            'schema': 'bronze',
            'name': 'performance_assignments',
            'file': QUERIES_DIR / 'bronze' / 'QRY-ASS-001-create_bronze_performance_assignments.sql',
            'description': 'Tabela Bronze para assignments'
        },
        {
            'type': 'TABLE',
            'schema': 'bronze',
            'name': 'performance_targets',
            'file': QUERIES_DIR / 'bronze' / 'QRY-TAR-001-create_bronze_performance_targets.sql',
            'description': 'Tabela Bronze para targets'
        },
        # Tabelas Silver
        {
            'type': 'TABLE',
            'schema': 'silver',
            'name': 'performance_assignments',
            'file': QUERIES_DIR / 'silver' / 'QRY-ASS-002-create_silver_performance_assignments.sql',
            'description': 'Tabela Silver para assignments'
        },
        {
            'type': 'TABLE',
            'schema': 'silver',
            'name': 'performance_targets',
            'file': QUERIES_DIR / 'silver' / 'QRY-TAR-002-create_silver_performance_targets.sql',
            'description': 'Tabela Silver para targets'
        },
        # Procedures
        {
            'type': 'PROCEDURE',
            'schema': 'bronze',
            'name': 'prc_bronze_to_silver_assignments',
            'file': QUERIES_DIR / 'bronze' / 'QRY-ASS-003-prc_bronze_to_silver_assignments.sql',
            'description': 'Procedure Bronze to Silver para assignments'
        },
        {
            'type': 'PROCEDURE',
            'schema': 'bronze',
            'name': 'prc_bronze_to_silver_performance_targets',
            'file': QUERIES_DIR / 'bronze' / 'QRY-TAR-003-prc_bronze_to_silver_performance_targets.sql',
            'description': 'Procedure Bronze to Silver para targets'
        }
    ]
    
    try:
        conn = connect_to_database()
        print("\n✓ Conectado ao banco de dados\n")
        
        created_count = 0
        for obj in objects_to_create:
            print(f"\nVerificando {obj['type']}: {obj['schema']}.{obj['name']}")
            print(f"  Descrição: {obj['description']}")
            
            if check_object_exists(conn, obj['type'], obj['schema'], obj['name']):
                print(f"  ✓ Já existe")
            else:
                print(f"  ✗ Não existe - Criando...")
                
                if obj['file'].exists():
                    if execute_sql_file(conn, obj['file']):
                        print(f"  ✓ Criado com sucesso")
                        created_count += 1
                    else:
                        print(f"  ✗ Erro ao criar")
                else:
                    print(f"  ✗ Arquivo SQL não encontrado: {obj['file']}")
        
        print(f"\n\n{'='*60}")
        print(f"RESUMO: {created_count} objetos criados")
        print(f"{'='*60}")
        
        conn.close()
        
    except Exception as e:
        print(f"\n✗ ERRO: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()