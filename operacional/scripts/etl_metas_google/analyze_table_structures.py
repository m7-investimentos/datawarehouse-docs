#!/usr/bin/env python3
"""
Script para analisar as estruturas das tabelas bronze.performance_assignments 
e silver.performance_assignments no SQL Server
"""

import pyodbc
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv('credentials/.env')

# Configuração da conexão
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
driver = os.getenv('DB_DRIVER')

# String de conexão
conn_str = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

def get_column_info(connection, schema, table):
    """Obtém informações das colunas de uma tabela"""
    query = """
    SELECT 
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.NUMERIC_PRECISION,
        c.NUMERIC_SCALE,
        c.IS_NULLABLE,
        c.COLUMN_DEFAULT
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
    ORDER BY c.ORDINAL_POSITION
    """
    
    df = pd.read_sql_query(query, connection, params=(schema, table))
    return df

def get_constraints(connection, schema, table):
    """Obtém informações das constraints de uma tabela"""
    query = """
    SELECT 
        tc.CONSTRAINT_NAME,
        tc.CONSTRAINT_TYPE,
        STRING_AGG(kcu.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) as COLUMNS
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
        AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        AND tc.TABLE_NAME = kcu.TABLE_NAME
    WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
    GROUP BY tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE
    ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME
    """
    
    df = pd.read_sql_query(query, connection, params=(schema, table))
    return df

def get_foreign_keys(connection, schema, table):
    """Obtém informações das foreign keys de uma tabela"""
    query = """
    SELECT 
        fk.name AS FK_NAME,
        OBJECT_NAME(fk.parent_object_id) AS TABLE_NAME,
        COL_NAME(fc.parent_object_id, fc.parent_column_id) AS COLUMN_NAME,
        OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS REFERENCED_SCHEMA,
        OBJECT_NAME(fk.referenced_object_id) AS REFERENCED_TABLE,
        COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS REFERENCED_COLUMN
    FROM sys.foreign_keys AS fk
    INNER JOIN sys.foreign_key_columns AS fc ON fk.object_id = fc.constraint_object_id
    WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = ? 
    AND OBJECT_NAME(fk.parent_object_id) = ?
    """
    
    df = pd.read_sql_query(query, connection, params=(schema, table))
    return df

def get_indexes(connection, schema, table):
    """Obtém informações dos índices de uma tabela"""
    query = """
    SELECT 
        i.name AS INDEX_NAME,
        i.type_desc AS INDEX_TYPE,
        i.is_unique,
        i.is_primary_key,
        STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS COLUMNS
    FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE OBJECT_SCHEMA_NAME(i.object_id) = ? 
    AND OBJECT_NAME(i.object_id) = ?
    AND i.type > 0
    GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
    ORDER BY i.name
    """
    
    df = pd.read_sql_query(query, connection, params=(schema, table))
    return df

def format_data_type(row):
    """Formata o tipo de dado com precision/scale ou length"""
    data_type = row['DATA_TYPE']
    
    if row['CHARACTER_MAXIMUM_LENGTH'] is not None:
        if row['CHARACTER_MAXIMUM_LENGTH'] == -1:
            return f"{data_type}(MAX)"
        else:
            return f"{data_type}({row['CHARACTER_MAXIMUM_LENGTH']})"
    elif row['NUMERIC_PRECISION'] is not None:
        if row['NUMERIC_SCALE'] is not None and row['NUMERIC_SCALE'] > 0:
            return f"{data_type}({row['NUMERIC_PRECISION']},{row['NUMERIC_SCALE']})"
        else:
            return f"{data_type}({row['NUMERIC_PRECISION']})"
    else:
        return data_type

def analyze_table(connection, schema, table):
    """Analisa a estrutura completa de uma tabela"""
    print(f"\n{'='*80}")
    print(f"ANÁLISE DA TABELA: {schema}.{table}")
    print(f"{'='*80}")
    
    # 1. Colunas
    print("\n1. ESTRUTURA DAS COLUNAS:")
    print("-" * 80)
    columns_df = get_column_info(connection, schema, table)
    
    if not columns_df.empty:
        for _, row in columns_df.iterrows():
            data_type = format_data_type(row)
            nullable = "NULL" if row['IS_NULLABLE'] == 'YES' else "NOT NULL"
            default = f"DEFAULT {row['COLUMN_DEFAULT']}" if row['COLUMN_DEFAULT'] else ""
            
            print(f"   {row['COLUMN_NAME']:<30} {data_type:<20} {nullable:<10} {default}")
    else:
        print("   Nenhuma coluna encontrada.")
    
    # 2. Constraints
    print("\n2. CONSTRAINTS:")
    print("-" * 80)
    constraints_df = get_constraints(connection, schema, table)
    
    if not constraints_df.empty:
        for constraint_type in constraints_df['CONSTRAINT_TYPE'].unique():
            print(f"\n   {constraint_type}:")
            type_constraints = constraints_df[constraints_df['CONSTRAINT_TYPE'] == constraint_type]
            for _, row in type_constraints.iterrows():
                print(f"      - {row['CONSTRAINT_NAME']}: ({row['COLUMNS']})")
    else:
        print("   Nenhuma constraint encontrada.")
    
    # 3. Foreign Keys detalhadas
    print("\n3. FOREIGN KEYS (DETALHADAS):")
    print("-" * 80)
    fk_df = get_foreign_keys(connection, schema, table)
    
    if not fk_df.empty:
        for _, row in fk_df.iterrows():
            print(f"   - {row['FK_NAME']}:")
            print(f"     {row['COLUMN_NAME']} -> {row['REFERENCED_SCHEMA']}.{row['REFERENCED_TABLE']}.{row['REFERENCED_COLUMN']}")
    else:
        print("   Nenhuma foreign key encontrada.")
    
    # 4. Índices
    print("\n4. ÍNDICES:")
    print("-" * 80)
    indexes_df = get_indexes(connection, schema, table)
    
    if not indexes_df.empty:
        for _, row in indexes_df.iterrows():
            idx_type = []
            if row['is_primary_key']:
                idx_type.append("PRIMARY KEY")
            elif row['is_unique']:
                idx_type.append("UNIQUE")
            idx_type.append(row['INDEX_TYPE'])
            
            print(f"   - {row['INDEX_NAME']} ({', '.join(idx_type)}):")
            print(f"     Colunas: {row['COLUMNS']}")
    else:
        print("   Nenhum índice encontrado.")
    
    return columns_df

def compare_tables(bronze_columns, silver_columns):
    """Compara as estruturas das duas tabelas"""
    print(f"\n{'='*80}")
    print("COMPARAÇÃO ENTRE AS TABELAS")
    print(f"{'='*80}")
    
    # Converter para conjuntos para comparação
    bronze_cols = set(bronze_columns['COLUMN_NAME'])
    silver_cols = set(silver_columns['COLUMN_NAME'])
    
    # Colunas apenas em bronze
    only_bronze = bronze_cols - silver_cols
    if only_bronze:
        print("\nColunas APENAS em bronze.performance_assignments:")
        for col in sorted(only_bronze):
            print(f"   - {col}")
    
    # Colunas apenas em silver
    only_silver = silver_cols - bronze_cols
    if only_silver:
        print("\nColunas APENAS em silver.performance_assignments:")
        for col in sorted(only_silver):
            print(f"   - {col}")
    
    # Colunas em comum
    common_cols = bronze_cols & silver_cols
    if common_cols:
        print("\nColunas em COMUM (verificando diferenças de tipo):")
        
        # Criar dicionários para comparação
        bronze_types = {}
        for _, row in bronze_columns.iterrows():
            if row['COLUMN_NAME'] in common_cols:
                bronze_types[row['COLUMN_NAME']] = format_data_type(row)
        
        silver_types = {}
        for _, row in silver_columns.iterrows():
            if row['COLUMN_NAME'] in common_cols:
                silver_types[row['COLUMN_NAME']] = format_data_type(row)
        
        differences = []
        for col in sorted(common_cols):
            bronze_type = bronze_types.get(col, '')
            silver_type = silver_types.get(col, '')
            
            if bronze_type != silver_type:
                differences.append((col, bronze_type, silver_type))
        
        if differences:
            print("\n   Diferenças de tipo encontradas:")
            for col, bronze_type, silver_type in differences:
                print(f"   - {col}:")
                print(f"     Bronze: {bronze_type}")
                print(f"     Silver: {silver_type}")
        else:
            print("   Todas as colunas em comum têm o mesmo tipo de dados.")

def main():
    """Função principal"""
    try:
        # Conectar ao banco
        print("Conectando ao SQL Server...")
        conn = pyodbc.connect(conn_str)
        print("Conexão estabelecida com sucesso!")
        
        # Analisar tabela bronze
        bronze_columns = analyze_table(conn, 'bronze', 'performance_assignments')
        
        # Analisar tabela silver
        silver_columns = analyze_table(conn, 'silver', 'performance_assignments')
        
        # Comparar as tabelas
        compare_tables(bronze_columns, silver_columns)
        
        # Fechar conexão
        conn.close()
        print("\nAnálise concluída!")
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()