#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para analisar os dados da planilha de assignments
"""

import os
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd

# Configurações
BASE_DIR = Path(__file__).resolve().parent.parent  # Go up one level from tests directory
CREDENTIALS_DIR = BASE_DIR / 'credentials'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# ID da planilha
SPREADSHEET_ID = '1k9gM3poSzuwEZbwaRv-AwVqQj4WjFnJE6mT5RWTvUww'
RANGE_NAME = 'Página1!A:J'

def main():
    print("Analisando dados da planilha de assignments...")
    print(f"ID: {SPREADSHEET_ID}")
    print("="*60)
    
    try:
        # Carregar credenciais
        creds_path = CREDENTIALS_DIR / 'google_sheets_api.json'
        credentials = service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
        
        # Conectar ao Google Sheets
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        
        # Obter dados
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("❌ Planilha vazia!")
            return
            
        # Converter para DataFrame
        headers = values[0]
        data = values[1:]
        
        # Garantir que todas as linhas tenham o mesmo número de colunas
        max_cols = len(headers)
        normalized_data = []
        for row in data:
            # Preencher com strings vazias se faltar colunas
            normalized_row = row + [''] * (max_cols - len(row))
            normalized_data.append(normalized_row[:max_cols])  # Truncar se tiver colunas extras
            
        df = pd.DataFrame(normalized_data, columns=headers)
        
        print(f"Total de linhas: {len(df)}")
        print(f"Total de colunas: {len(df.columns)}")
        
        # Analisar campos vazios
        print("\n" + "="*60)
        print("Análise de campos vazios:")
        for col in df.columns:
            empty_count = df[col].isna().sum() + (df[col] == '').sum()
            empty_pct = (empty_count / len(df)) * 100
            print(f"  {col}: {empty_count} vazios ({empty_pct:.1f}%)")
            
        # Analisar valores únicos de indicator_type
        print("\n" + "="*60)
        print("Valores únicos de indicator_type:")
        indicator_types = df['indicator_type'].value_counts(dropna=False)
        for value, count in indicator_types.items():
            if pd.isna(value) or value == '':
                print(f"  [VAZIO]: {count}")
            else:
                print(f"  {value}: {count}")
                
        # Verificar primeiras linhas com problemas
        print("\n" + "="*60)
        print("Primeiras 5 linhas com indicator_type vazio:")
        empty_type = df[(df['indicator_type'].isna()) | (df['indicator_type'] == '')]
        if not empty_type.empty:
            for idx, row in empty_type.head().iterrows():
                print(f"\nLinha {idx+2}:")
                print(f"  cod_assessor: {row['cod_assessor']}")
                print(f"  indicator_code: {row['indicator_code']}")
                print(f"  indicator_type: '{row['indicator_type']}'")
                print(f"  weight: {row['weight']}")
                
        # Verificar se há padrões nos dados
        print("\n" + "="*60)
        print("Análise de padrões:")
        
        # Verificar se weight está preenchido quando indicator_type está vazio
        empty_type_df = df[(df['indicator_type'].isna()) | (df['indicator_type'] == '')]
        if not empty_type_df.empty:
            weights_in_empty = empty_type_df['weight'].apply(lambda x: x and x != '')
            print(f"Linhas com indicator_type vazio mas weight preenchido: {weights_in_empty.sum()}")
            
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == '__main__':
    main()