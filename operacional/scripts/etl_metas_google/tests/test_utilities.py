#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilidades comuns para testes e diagnósticos do ETL
Centraliza funções reutilizáveis para conexão, análise e verificação
"""

import os
import sys
import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import logging

# Configuração base
BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = BASE_DIR / 'credentials'
load_dotenv(CREDENTIALS_DIR / '.env')

# Configuração de logging
def setup_logging(name, level=logging.INFO):
    """Configura logging padronizado para testes"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        
        # Formato
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger

# Conexão com banco de dados
class DatabaseConnection:
    """Gerencia conexões com o banco de dados SQL Server"""
    
    def __init__(self):
        self.driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')
        self.server = os.getenv('DB_SERVER')
        self.database = os.getenv('DB_DATABASE')
        self.username = os.getenv('DB_USERNAME')
        self.password = os.getenv('DB_PASSWORD')
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Estabelece conexão com o banco"""
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes"
        )
        
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()
        return self.conn
    
    def execute_query(self, query, params=None, fetch=True):
        """Executa query e retorna resultados"""
        if not self.conn:
            self.connect()
        
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        
        if fetch:
            return self.cursor.fetchall()
        return self.cursor.rowcount
    
    def execute_dataframe(self, query, params=None):
        """Executa query e retorna DataFrame pandas"""
        if not self.conn:
            self.connect()
        
        return pd.read_sql_query(query, self.conn, params=params)
    
    def close(self):
        """Fecha conexão"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

# Funções de verificação de estrutura
class StructureChecker:
    """Verifica estruturas de tabelas e objetos do banco"""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    def check_table_exists(self, schema, table_name):
        """Verifica se tabela existe"""
        query = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        result = self.db.execute_query(query, (schema, table_name))
        return result[0][0] > 0
    
    def get_table_columns(self, schema, table_name):
        """Retorna informações das colunas da tabela"""
        query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        return self.db.execute_dataframe(query, (schema, table_name))
    
    def check_procedure_exists(self, schema, procedure_name):
        """Verifica se procedure existe"""
        query = """
            SELECT COUNT(*)
            FROM sys.procedures p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE s.name = ? AND p.name = ?
        """
        result = self.db.execute_query(query, (schema, procedure_name))
        return result[0][0] > 0
    
    def get_table_row_count(self, schema, table_name):
        """Retorna quantidade de registros na tabela"""
        query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"
        result = self.db.execute_query(query)
        return result[0][0]
    
    def get_indexes(self, schema, table_name):
        """Retorna índices da tabela"""
        query = """
            SELECT 
                i.name AS index_name,
                i.type_desc,
                i.is_unique,
                i.is_primary_key,
                STRING_AGG(c.name, ', ') AS columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ?
            GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
            ORDER BY i.index_id
        """
        return self.db.execute_dataframe(query, (schema, table_name))

# Funções de análise de dados
class DataAnalyzer:
    """Analisa dados nas tabelas para identificar problemas"""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    def analyze_bronze_table(self, table_name):
        """Análise completa de tabela Bronze"""
        schema = 'bronze'
        
        # Estatísticas básicas
        stats = {
            'total_records': 0,
            'processed_records': 0,
            'unprocessed_records': 0,
            'error_records': 0,
            'distinct_load_ids': 0,
            'date_range': None
        }
        
        # Total de registros
        query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"
        stats['total_records'] = self.db.execute_query(query)[0][0]
        
        # Registros processados/não processados
        if stats['total_records'] > 0:
            query = f"""
                SELECT 
                    is_processed,
                    COUNT(*) as count
                FROM [{schema}].[{table_name}]
                GROUP BY is_processed
            """
            for row in self.db.execute_query(query):
                if row[0] == 1:
                    stats['processed_records'] = row[1]
                else:
                    stats['unprocessed_records'] = row[1]
            
            # Registros com erro
            query = f"""
                SELECT COUNT(*) 
                FROM [{schema}].[{table_name}]
                WHERE processing_status = 'ERROR'
            """
            stats['error_records'] = self.db.execute_query(query)[0][0]
            
            # Load IDs distintos
            query = f"""
                SELECT COUNT(DISTINCT load_id)
                FROM [{schema}].[{table_name}]
            """
            stats['distinct_load_ids'] = self.db.execute_query(query)[0][0]
            
            # Range de datas
            query = f"""
                SELECT 
                    MIN(load_timestamp) as min_date,
                    MAX(load_timestamp) as max_date
                FROM [{schema}].[{table_name}]
            """
            result = self.db.execute_query(query)[0]
            stats['date_range'] = (result[0], result[1])
        
        return stats
    
    def check_data_quality(self, schema, table_name, column_name):
        """Verifica qualidade de dados em uma coluna"""
        query = f"""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT {column_name}) as distinct_values,
                COUNT(CASE WHEN {column_name} IS NULL THEN 1 END) as null_count,
                COUNT(CASE WHEN LTRIM(RTRIM({column_name})) = '' THEN 1 END) as empty_count
            FROM [{schema}].[{table_name}]
        """
        return self.db.execute_dataframe(query)
    
    def find_duplicates(self, schema, table_name, key_columns):
        """Encontra registros duplicados baseado em colunas chave"""
        key_cols = ', '.join(key_columns)
        query = f"""
            SELECT {key_cols}, COUNT(*) as duplicate_count
            FROM [{schema}].[{table_name}]
            GROUP BY {key_cols}
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
        """
        return self.db.execute_dataframe(query)

# Funções de validação
class DataValidator:
    """Valida dados conforme regras de negócio"""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    def validate_weight_sum(self, crm_id=None):
        """Valida se soma dos pesos CARD é 100% por assessor"""
        where_clause = f"AND a.crm_id = '{crm_id}'" if crm_id else ""
        
        query = f"""
            WITH weight_check AS (
                SELECT 
                    a.crm_id,
                    SUM(CASE WHEN i.category = 'CARD' THEN a.indicator_weight ELSE 0 END) as total_weight
                FROM silver.performance_assignments a
                INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
                WHERE a.is_active = 1 {where_clause}
                GROUP BY a.crm_id
            )
            SELECT 
                crm_id,
                total_weight,
                CASE 
                    WHEN ABS(total_weight - 100.00) < 0.01 THEN 'VÁLIDO'
                    ELSE 'INVÁLIDO'
                END as status
            FROM weight_check
            WHERE ABS(total_weight - 100.00) >= 0.01
        """
        return self.db.execute_dataframe(query)
    
    def validate_indicators_exist(self):
        """Valida se todos os indicadores nas atribuições existem na tabela mestre"""
        query = """
            SELECT DISTINCT 
                b.indicator_code,
                b.crm_id,
                CASE 
                    WHEN i.indicator_id IS NULL THEN 'NÃO EXISTE'
                    ELSE 'OK'
                END as status
            FROM bronze.performance_assignments b
            LEFT JOIN silver.performance_indicators i 
                ON UPPER(LTRIM(RTRIM(b.indicator_code))) = i.indicator_code
            WHERE b.is_processed = 0
              AND i.indicator_id IS NULL
        """
        return self.db.execute_dataframe(query)
    
    def validate_date_formats(self, schema, table_name, date_column):
        """Valida formatos de data em colunas varchar"""
        query = f"""
            SELECT 
                {date_column} as original_value,
                TRY_CAST({date_column} AS DATE) as converted_date,
                COUNT(*) as occurrences
            FROM [{schema}].[{table_name}]
            WHERE {date_column} IS NOT NULL
              AND TRY_CAST({date_column} AS DATE) IS NULL
            GROUP BY {date_column}
            ORDER BY COUNT(*) DESC
        """
        return self.db.execute_dataframe(query)

# Funções auxiliares
def print_dataframe_summary(df, title="DataFrame Summary"):
    """Imprime resumo formatado de um DataFrame"""
    print(f"\n{title}")
    print("=" * len(title))
    print(f"Shape: {df.shape}")
    print("\nPrimeiras 5 linhas:")
    print(df.head())
    print("\nInfo:")
    print(df.info())

def compare_tables(db, table1_info, table2_info):
    """Compara estrutura de duas tabelas"""
    schema1, table1 = table1_info
    schema2, table2 = table2_info
    
    checker = StructureChecker(db)
    
    cols1 = set(checker.get_table_columns(schema1, table1)['COLUMN_NAME'])
    cols2 = set(checker.get_table_columns(schema2, table2)['COLUMN_NAME'])
    
    only_in_1 = cols1 - cols2
    only_in_2 = cols2 - cols1
    common = cols1 & cols2
    
    return {
        'table1_only': list(only_in_1),
        'table2_only': list(only_in_2),
        'common_columns': list(common),
        'match_percentage': len(common) / max(len(cols1), len(cols2)) * 100
    }

def format_sql_query(query):
    """Formata query SQL para melhor legibilidade"""
    keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'JOIN', 'LEFT JOIN', 'INNER JOIN']
    formatted = query
    for keyword in keywords:
        formatted = formatted.replace(f' {keyword} ', f'\n{keyword} ')
    return formatted.strip()

# Teste da conexão
def test_database_connection():
    """Testa conexão com o banco de dados"""
    logger = setup_logging('test_connection')
    
    try:
        db = DatabaseConnection()
        db.connect()
        
        # Testa query simples
        result = db.execute_query("SELECT @@VERSION")
        logger.info(f"Conexão bem-sucedida! SQL Server: {result[0][0][:50]}...")
        
        # Testa acesso aos schemas
        schemas = ['bronze', 'silver', 'gold_performance']
        for schema in schemas:
            query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'"
            count = db.execute_query(query)[0][0]
            logger.info(f"Schema {schema}: {count} tabelas")
        
        db.close()
        return True
        
    except Exception as e:
        logger.error(f"Erro na conexão: {e}")
        return False

if __name__ == "__main__":
    # Teste básico das utilidades
    print("Testando utilidades...")
    if test_database_connection():
        print("\n✓ Todas as utilidades estão funcionando corretamente!")
    else:
        print("\n✗ Problemas encontrados. Verifique as configurações.")