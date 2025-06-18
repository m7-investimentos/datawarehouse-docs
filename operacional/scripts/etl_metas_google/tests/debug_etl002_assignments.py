#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Debug Script para ETL-002 Performance Assignments
Analisa problemas de carga e transformação
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

# Configuração
BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = BASE_DIR / 'credentials'
load_dotenv(CREDENTIALS_DIR / '.env')

# Google Sheets
SPREADSHEET_ID = '1h3jC5EpXOv-O1oyL2tBlt9Q16pLHpsoWCHaeNiRHmeY'
RANGE_NAME = 'm7_performance_assignments!A:J'
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
    print("\n=== VERIFICANDO GOOGLE SHEETS - ASSIGNMENTS ===")
    
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
        
        # Verificar se tem a coluna esperada
        if 'nome_assessor' not in headers and 'name_assessor' in headers:
            print("\n⚠️  PROBLEMA: Planilha usa 'name_assessor' mas ETL espera 'nome_assessor'")
            print("   Solução: Renomear coluna na planilha ou ajustar mapeamento no ETL")
        
        # Mostrar primeiras linhas
        print(f"\nPrimeiras 5 linhas:")
        print(df.head())
        
        # Verificar crm_id vs cod_assessor
        has_crm_id = 'crm_id' in headers
        has_cod_assessor = 'cod_assessor' in headers
        
        if has_cod_assessor and not has_crm_id:
            print("\n⚠️  PROBLEMA: Planilha ainda usa 'cod_assessor' em vez de 'crm_id'")
        elif has_crm_id:
            print("\n✓ Planilha já usa 'crm_id' corretamente")
        
        return df
        
    except Exception as e:
        print(f"ERRO ao acessar Google Sheets: {e}")
        return None

def check_database_objects():
    """Verifica objetos no banco de dados"""
    print("\n=== VERIFICANDO OBJETOS NO BANCO ===")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar tabela Bronze
        print("\n1. Tabela Bronze:")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'bronze' 
            AND TABLE_NAME = 'performance_assignments'
        """)
        
        exists = cursor.fetchone()[0] > 0
        if exists:
            print("  ✓ bronze.performance_assignments existe")
            
            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM bronze.performance_assignments")
            count = cursor.fetchone()[0]
            print(f"  Registros: {count}")
        else:
            print("  ✗ bronze.performance_assignments NÃO EXISTE")
            print("  Ação: Executar script QRY-ASS-001-create_bronze_performance_assignments.sql")
        
        # Verificar tabela Silver
        print("\n2. Tabela Silver:")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'silver' 
            AND TABLE_NAME = 'performance_assignments'
        """)
        
        exists = cursor.fetchone()[0] > 0
        if exists:
            print("  ✓ silver.performance_assignments existe")
            
            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM silver.performance_assignments")
            count = cursor.fetchone()[0]
            print(f"  Registros: {count}")
        else:
            print("  ✗ silver.performance_assignments NÃO EXISTE")
            print("  Ação: Executar script QRY-ASS-002-create_silver_performance_assignments.sql")
        
        # Verificar procedure
        print("\n3. Procedure Bronze to Silver:")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            AND ROUTINE_SCHEMA = 'bronze'
            AND ROUTINE_NAME = 'prc_bronze_to_silver_assignments'
        """)
        
        exists = cursor.fetchone()[0] > 0
        if exists:
            print("  ✓ bronze.prc_bronze_to_silver_assignments existe")
        else:
            print("  ✗ bronze.prc_bronze_to_silver_assignments NÃO EXISTE")
            print("  Ação: Executar script QRY-ASS-003-prc_bronze_to_silver_assignments.sql")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERRO: {e}")

def check_etl_configuration():
    """Verifica configuração do ETL"""
    print("\n=== VERIFICANDO CONFIGURAÇÃO DO ETL ===")
    
    config_file = BASE_DIR / 'config' / 'etl_002_config.json'
    
    if config_file.exists():
        import json
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        print(f"Arquivo de configuração encontrado: {config_file}")
        
        # Verificar mapeamento de colunas
        if 'column_mapping' in config:
            print("\nMapeamento de colunas:")
            for google_col, db_col in config['column_mapping'].items():
                print(f"  {google_col} → {db_col}")
                
            # Verificar se tem mapeamento para name_assessor
            if 'name_assessor' not in config['column_mapping'] and 'nome_assessor' not in config['column_mapping']:
                print("\n⚠️  PROBLEMA: Falta mapeamento para coluna de nome do assessor")
        else:
            print("\n⚠️  Configuração não tem mapeamento de colunas")
    else:
        print(f"✗ Arquivo de configuração não encontrado: {config_file}")

def suggest_fixes():
    """Sugere correções"""
    print("\n" + "="*60)
    print("RESUMO DE AÇÕES NECESSÁRIAS:")
    print("="*60)
    
    print("""
1. CRIAR TABELA BRONZE:
   cd /Users/bchiaramonti/Documents/1_Projects/ProjetoAlicerce/datawarehouse-docs/operacional/queries/bronze
   Execute: QRY-ASS-001-create_bronze_performance_assignments.sql

2. CRIAR PROCEDURE:
   Execute: QRY-ASS-003-prc_bronze_to_silver_assignments.sql

3. AJUSTAR PLANILHA OU ETL:
   Opção A: Renomear coluna 'name_assessor' para 'nome_assessor' no Google Sheets
   Opção B: Adicionar mapeamento no arquivo config/etl_002_config.json:
   {
     "column_mapping": {
       "name_assessor": "nome_assessor",
       "crm_id": "crm_id"  // verificar se já foi alterado de cod_assessor
     }
   }

4. EXECUTAR NOVAMENTE O PIPELINE:
   ./run_etl.sh
   Escolher opção: Pipeline completo 002 - Assignments
""")

def main():
    """Executa todas as verificações"""
    print("="*60)
    print("DEBUG ETL-002 - Performance Assignments")
    print(f"Timestamp: {datetime.now()}")
    print("="*60)
    
    # 1. Verificar dados do Google Sheets
    check_google_sheets_data()
    
    # 2. Verificar objetos no banco
    check_database_objects()
    
    # 3. Verificar configuração do ETL
    check_etl_configuration()
    
    # 4. Sugerir correções
    suggest_fixes()
    
    print("\n" + "="*60)
    print("DEBUG CONCLUÍDO")
    print("="*60)

if __name__ == '__main__':
    main()