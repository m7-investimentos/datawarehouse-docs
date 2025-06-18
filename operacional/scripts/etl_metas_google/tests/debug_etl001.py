#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Debug Script para ETL-001 Performance Indicators
Analisa problemas de carga de dados NULL e transformação Bronze->Silver
"""

import os
import sys
import pandas as pd
import pyodbc
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import json

# Configuração
BASE_DIR = Path(__file__).resolve().parent
CREDENTIALS_DIR = BASE_DIR / 'credentials'
load_dotenv(CREDENTIALS_DIR / '.env')

# Google Sheets
SPREADSHEET_ID = '1h3jC5EpXOv-O1oyL2tBlt9Q16pLHpsoWCHaeNiRHmeY'
RANGE_NAME = 'Página1!A:K'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

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

def check_google_sheets_data():
    """Verifica dados direto do Google Sheets"""
    print("\n=== VERIFICANDO GOOGLE SHEETS ===")
    
    try:
        # Carregar credenciais
        creds_path = CREDENTIALS_DIR / 'google_sheets_api.json'
        credentials = service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
        
        # Conectar ao Sheets
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        
        # Buscar dados
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("ERRO: Planilha vazia!")
            return None
            
        # Converter para DataFrame
        headers = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=headers)
        
        print(f"Headers encontrados: {headers}")
        print(f"Total de linhas de dados: {len(data)}")
        print(f"\nPrimeiras 5 linhas:")
        print(df.head())
        
        # Verificar valores vazios
        print(f"\nValores vazios por coluna:")
        for col in df.columns:
            empty_count = df[col].isna().sum() + (df[col] == '').sum()
            print(f"  {col}: {empty_count} vazios de {len(df)} total")
        
        # Verificar fórmulas que contém cod_assessor
        print(f"\nVerificando fórmulas com 'cod_assessor':")
        if 'formula' in df.columns or 'calculation_formula' in df.columns:
            formula_col = 'formula' if 'formula' in df.columns else 'calculation_formula'
            cod_assessor_count = df[formula_col].str.contains('cod_assessor', na=False).sum()
            print(f"  Fórmulas com 'cod_assessor': {cod_assessor_count}")
            
            if cod_assessor_count > 0:
                print("\n  Exemplos de fórmulas com cod_assessor:")
                examples = df[df[formula_col].str.contains('cod_assessor', na=False)][['indicator_code', formula_col]].head(3)
                for idx, row in examples.iterrows():
                    print(f"    {row['indicator_code']}: {row[formula_col][:100]}...")
        
        return df
        
    except Exception as e:
        print(f"ERRO ao acessar Google Sheets: {e}")
        return None

def check_bronze_data():
    """Verifica dados na tabela Bronze"""
    print("\n=== VERIFICANDO DADOS BRONZE ===")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar estrutura da tabela
        print("Colunas da tabela Bronze:")
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'performance_indicators'
            ORDER BY ORDINAL_POSITION
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} (Nullable: {row[2]})")
        
        # Verificar dados
        print("\nDados na tabela Bronze:")
        cursor.execute("SELECT COUNT(*) FROM bronze.performance_indicators")
        total = cursor.fetchone()[0]
        print(f"  Total de registros: {total}")
        
        if total > 0:
            # Verificar última carga
            cursor.execute("""
                SELECT TOP 1 load_timestamp, load_source 
                FROM bronze.performance_indicators 
                ORDER BY load_timestamp DESC
            """)
            last_load = cursor.fetchone()
            print(f"  Última carga: {last_load[0]} de {last_load[1]}")
            
            # Verificar valores NULL
            print("\nValores NULL por coluna:")
            columns = ['indicator_code', 'indicator_name', 'category', 'unit', 
                      'aggregation', 'formula', 'is_inverted', 'is_active']
            
            for col in columns:
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM bronze.performance_indicators 
                    WHERE {col} IS NULL OR {col} = '' OR {col} = 'nan'
                """)
                null_count = cursor.fetchone()[0]
                print(f"    {col}: {null_count} valores vazios/NULL")
            
            # Mostrar exemplos de dados
            print("\nExemplos de dados (primeiros 3 registros):")
            cursor.execute("""
                SELECT TOP 3 indicator_code, indicator_name, category, unit, formula
                FROM bronze.performance_indicators
                ORDER BY load_id DESC
            """)
            
            for row in cursor.fetchall():
                print(f"  Code: {row[0]}, Name: {row[1]}, Cat: {row[2]}, Unit: {row[3]}")
                print(f"  Formula: {row[4][:50] if row[4] else 'NULL'}...")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO ao verificar Bronze: {e}")

def check_silver_data():
    """Verifica dados na tabela Silver"""
    print("\n=== VERIFICANDO DADOS SILVER ===")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar se a tabela existe
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'performance_indicators'
        """)
        
        if cursor.fetchone()[0] == 0:
            print("ERRO: Tabela silver.performance_indicators não existe!")
            return
        
        # Verificar dados
        cursor.execute("SELECT COUNT(*) FROM silver.performance_indicators")
        total = cursor.fetchone()[0]
        print(f"Total de registros na Silver: {total}")
        
        if total > 0:
            # Verificar dados válidos
            cursor.execute("""
                SELECT COUNT(*) 
                FROM silver.performance_indicators 
                WHERE indicator_id IS NOT NULL
            """)
            valid_count = cursor.fetchone()[0]
            print(f"Registros com indicator_id válido: {valid_count}")
            
            # Mostrar exemplo
            cursor.execute("""
                SELECT TOP 3 indicator_id, indicator_code, indicator_name
                FROM silver.performance_indicators
                WHERE indicator_id IS NOT NULL
            """)
            
            print("\nExemplos de dados Silver:")
            for row in cursor.fetchall():
                print(f"  ID: {row[0]}, Code: {row[1]}, Name: {row[2]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO ao verificar Silver: {e}")

def test_bronze_to_silver_procedure():
    """Testa a procedure de transformação Bronze->Silver"""
    print("\n=== TESTANDO PROCEDURE BRONZE->SILVER ===")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar se a procedure existe
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_SCHEMA = 'bronze' 
            AND ROUTINE_NAME = 'prc_bronze_to_silver_indicators'
        """)
        
        if cursor.fetchone()[0] == 0:
            print("ERRO: Procedure bronze.prc_bronze_to_silver_indicators não existe!")
            return
        
        # Executar procedure manualmente
        print("Executando procedure...")
        try:
            cursor.execute("EXEC bronze.prc_bronze_to_silver_indicators")
            conn.commit()
            print("Procedure executada com sucesso!")
            
            # Verificar resultado
            cursor.execute("SELECT COUNT(*) FROM silver.performance_indicators")
            count = cursor.fetchone()[0]
            print(f"Registros na Silver após procedure: {count}")
            
        except Exception as proc_error:
            print(f"ERRO ao executar procedure: {proc_error}")
            
            # Tentar identificar o problema
            print("\nVerificando dados Bronze não processados:")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM bronze.performance_indicators 
                WHERE is_processed = 0
            """)
            unprocessed = cursor.fetchone()[0]
            print(f"Registros não processados: {unprocessed}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

def analyze_mapping_issues():
    """Analisa problemas de mapeamento entre Google Sheets e Bronze"""
    print("\n=== ANÁLISE DE MAPEAMENTO ===")
    
    # Buscar dados do Sheets
    sheets_df = check_google_sheets_data()
    if sheets_df is None:
        return
    
    print("\n\nComparando estrutura esperada vs real:")
    
    # Mapeamento esperado pelo ETL
    expected_columns = {
        'indicator_code': 'Código do indicador',
        'indicator_name': 'Nome do indicador',
        'category': 'Categoria (ex: FINANCEIRO, QUALIDADE)',
        'unit': 'Unidade de medida (ex: R$, %, QTD)',
        'aggregation': 'Método de agregação',
        'formula': 'Fórmula SQL',
        'is_inverted': 'Indicador invertido (0/1)',
        'is_active': 'Indicador ativo (0/1)',
        'description': 'Descrição',
        'notes': 'Observações'
    }
    
    print("\nColunas esperadas vs encontradas:")
    for expected, description in expected_columns.items():
        found = expected in sheets_df.columns
        print(f"  {expected}: {'✓' if found else '✗'} - {description}")
    
    # Verificar possíveis problemas de nomenclatura
    print("\n\nColunas no Google Sheets não mapeadas:")
    for col in sheets_df.columns:
        if col not in expected_columns:
            print(f"  - {col}")

def main():
    """Executa todas as verificações"""
    print("="*60)
    print("DEBUG ETL-001 - Performance Indicators")
    print(f"Timestamp: {datetime.now()}")
    print("="*60)
    
    # 1. Verificar dados do Google Sheets
    check_google_sheets_data()
    
    # 2. Verificar dados Bronze
    check_bronze_data()
    
    # 3. Verificar dados Silver
    check_silver_data()
    
    # 4. Testar procedure
    test_bronze_to_silver_procedure()
    
    # 5. Análise de mapeamento
    analyze_mapping_issues()
    
    print("\n" + "="*60)
    print("DEBUG CONCLUÍDO")
    print("="*60)

if __name__ == '__main__':
    main()