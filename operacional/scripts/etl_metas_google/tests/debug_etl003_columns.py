#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Debug Script: ETL-003 Column Mismatch Analysis
==============================================
This script helps identify column mismatches between the DataFrame
and the SQL Server table bronze.performance_targets
"""

import os
import sys
import json
import pandas as pd
import pyodbc
from pathlib import Path
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text, inspect

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Directories
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / 'config'
CREDENTIALS_DIR = BASE_DIR / 'credentials'

# Load environment variables
load_dotenv(CREDENTIALS_DIR / '.env')

# Google Sheets Configuration
SPREADSHEET_ID = '1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE'
RANGE_NAME = 'Página1!A:I'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

def load_config():
    """Load configuration from etl_003_config.json"""
    config_path = CONFIG_DIR / 'etl_003_config.json'
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
        
    # Replace environment variables
    config['database']['server'] = os.getenv('DB_SERVER', config['database']['server'])
    config['database']['database'] = os.getenv('DB_DATABASE', config['database']['database'])
    config['database']['user'] = os.getenv('DB_USERNAME', config['database']['user'])
    config['database']['password'] = os.getenv('DB_PASSWORD', config['database']['password'])
    
    if os.getenv('DB_DRIVER'):
        config['database']['driver'] = os.getenv('DB_DRIVER')
    
    return config

def get_database_connection(config):
    """Create database connection"""
    db_config = config['database']
    driver = db_config['driver'].strip('{}')
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['user']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes"
    )
    
    connection_string = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"
    return create_engine(connection_string)

def get_table_columns(engine):
    """Get columns from bronze.performance_targets table"""
    print("\n=== ANALYZING DATABASE TABLE STRUCTURE ===")
    
    inspector = inspect(engine)
    
    # Get columns from the table
    columns = inspector.get_columns('performance_targets', schema='bronze')
    
    print(f"\nFound {len(columns)} columns in bronze.performance_targets:")
    print("-" * 80)
    
    table_columns = []
    for col in columns:
        # Skip IDENTITY columns
        if col.get('autoincrement', False):
            print(f"  [SKIPPED - IDENTITY] {col['name']} ({col['type']})")
        else:
            table_columns.append(col['name'])
            nullable = "NULL" if col['nullable'] else "NOT NULL"
            print(f"  {col['name']} ({col['type']}) {nullable}")
    
    return table_columns

def extract_sample_data(config):
    """Extract a small sample from Google Sheets"""
    print("\n=== EXTRACTING SAMPLE DATA FROM GOOGLE SHEETS ===")
    
    # Load credentials
    creds_path = CREDENTIALS_DIR / 'google_sheets_api.json'
    credentials = service_account.Credentials.from_service_account_file(
        creds_path, scopes=SCOPES
    )
    
    # Connect to Google Sheets
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    
    # Get first 10 rows
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range='Página1!A1:I10'
    ).execute()
    
    values = result.get('values', [])
    
    if not values:
        print("No data found in Google Sheets!")
        return None
    
    headers = values[0]
    data_rows = values[1:]
    
    # Create DataFrame
    df = pd.DataFrame(data_rows, columns=headers)
    
    print(f"\nExtracted {len(df)} sample rows")
    print(f"Original columns from Google Sheets: {list(df.columns)}")
    
    return df

def process_dataframe(df):
    """Apply ETL-003 transformations to the DataFrame"""
    print("\n=== APPLYING ETL-003 TRANSFORMATIONS ===")
    
    # T1: Convert period_start to datetime and standardize
    df['period_start'] = pd.to_datetime(df['period_start'], errors='coerce')
    df['period_start'] = df['period_start'].apply(
        lambda x: x.replace(day=1) if pd.notna(x) else x
    )
    
    # T2: Calculate period_end
    import calendar
    df['period_end'] = df['period_start'].apply(
        lambda x: x.replace(day=calendar.monthrange(x.year, x.month)[1]) if pd.notna(x) else x
    )
    
    # T3: Add period_type
    df['period_type'] = 'MENSAL'
    
    # T4: Convert numeric columns
    value_cols = ['target_value', 'stretch_value', 'minimum_value']
    for col in value_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    # T5: Add metadata columns
    df['extraction_timestamp'] = datetime.now()
    df['source_file'] = f'GoogleSheets:{SPREADSHEET_ID}'
    df['row_number'] = range(2, len(df) + 2)
    
    # T6: Add row hash
    import hashlib
    df['row_hash'] = df.apply(
        lambda x: hashlib.md5(
            f"{x['cod_assessor']}_{x['indicator_code']}_{x['period_start'].strftime('%Y-%m') if pd.notna(x['period_start']) else ''}".encode()
        ).hexdigest(), 
        axis=1
    )
    
    # T7: Add date components
    df['target_year'] = df['period_start'].dt.year
    df['target_quarter'] = df['period_start'].dt.quarter
    
    # T8: Add validation columns
    df['is_inverted'] = 0  # Simplified for debug
    df['target_logic_valid'] = 1  # Simplified for debug
    
    # T9: Add load control columns
    df['load_timestamp'] = datetime.now()
    df['load_source'] = f'GoogleSheets:{SPREADSHEET_ID}'
    df['is_processed'] = 0
    df['processing_date'] = None
    df['processing_status'] = None
    df['processing_notes'] = None
    df['validation_errors'] = None
    
    # Convert dates to strings
    df['period_start'] = df['period_start'].dt.strftime('%Y-%m-%d')
    df['period_end'] = df['period_end'].dt.strftime('%Y-%m-%d')
    
    # Convert values to formatted strings
    for col in value_cols:
        df[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) and x != 0 else '')
    
    print(f"\nDataFrame columns after transformation: {list(df.columns)}")
    
    return df

def compare_columns(table_columns, df_columns):
    """Compare table columns with DataFrame columns"""
    print("\n=== COLUMN COMPARISON ANALYSIS ===")
    
    # Convert to sets for comparison
    table_set = set(table_columns)
    df_set = set(df_columns)
    
    # Find differences
    missing_in_df = table_set - df_set
    extra_in_df = df_set - table_set
    common_columns = table_set & df_set
    
    print(f"\nColumns in table: {len(table_columns)}")
    print(f"Columns in DataFrame: {len(df_columns)}")
    print(f"Common columns: {len(common_columns)}")
    
    if missing_in_df:
        print(f"\n[ERROR] Columns in table but MISSING in DataFrame ({len(missing_in_df)}):")
        for col in sorted(missing_in_df):
            print(f"  - {col}")
    
    if extra_in_df:
        print(f"\n[WARNING] Columns in DataFrame but NOT in table ({len(extra_in_df)}):")
        for col in sorted(extra_in_df):
            print(f"  - {col}")
    
    # Show the exact order expected
    print("\n=== EXPECTED COLUMN ORDER FOR INSERT ===")
    print("The INSERT statement should include these columns in this order:")
    ordered_columns = []
    for col in table_columns:
        if col in df_columns:
            ordered_columns.append(col)
            print(f"  ✓ {col}")
        else:
            print(f"  ✗ {col} [MISSING IN DATAFRAME]")
    
    return ordered_columns, missing_in_df, extra_in_df

def generate_insert_statement(table_columns, df):
    """Generate the exact INSERT statement that would be used"""
    print("\n=== GENERATED INSERT STATEMENT ===")
    
    # Get available columns in correct order
    available_columns = [col for col in table_columns if col in df.columns]
    
    # Generate column list
    column_list = ", ".join(available_columns)
    
    # Generate values placeholder
    values_placeholder = ", ".join(["?" for _ in available_columns])
    
    insert_stmt = f"""INSERT INTO bronze.performance_targets 
({column_list})
VALUES ({values_placeholder})"""
    
    print(insert_stmt)
    
    # Show sample values
    if len(df) > 0:
        print("\n=== SAMPLE VALUES FOR FIRST ROW ===")
        first_row = df.iloc[0]
        for col in available_columns:
            value = first_row.get(col, 'NULL')
            print(f"  {col}: {value}")
    
    return insert_stmt, available_columns

def test_insert(engine, df, table_columns):
    """Test inserting a single row to verify column mapping"""
    print("\n=== TESTING INSERT WITH SINGLE ROW ===")
    
    # Get available columns
    available_columns = [col for col in table_columns if col in df.columns]
    
    # Prepare test data
    test_df = df.head(1)[available_columns].copy()
    
    try:
        # Try to insert
        with engine.begin() as conn:
            # First, delete any test data
            conn.execute(text("""
                DELETE FROM bronze.performance_targets 
                WHERE row_hash = :hash
            """), {"hash": test_df.iloc[0]['row_hash']})
            
            # Try insert
            test_df.to_sql(
                'performance_targets',
                conn,
                schema='bronze',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print("✓ Test insert successful!")
            
            # Rollback to not affect production data
            conn.rollback()
            
    except Exception as e:
        print(f"✗ Test insert failed: {str(e)}")
        print("\nThis is the exact error you're getting in production.")
        
        # Try to extract more specific error information
        if "COUNT field incorrect" in str(e):
            print("\nThe error suggests a mismatch in the number of columns.")
            print(f"DataFrame has {len(available_columns)} columns")
            print(f"Table expects {len(table_columns)} non-IDENTITY columns")

def main():
    """Main debug function"""
    print("="*80)
    print("ETL-003 Column Mismatch Debug Script")
    print("="*80)
    
    try:
        # Load configuration
        config = load_config()
        print("✓ Configuration loaded")
        
        # Connect to database
        engine = get_database_connection(config)
        print("✓ Database connection established")
        
        # Get table columns
        table_columns = get_table_columns(engine)
        
        # Extract sample data
        df = extract_sample_data(config)
        if df is None:
            return
        
        # Process data with ETL-003 logic
        df = process_dataframe(df)
        
        # Compare columns
        ordered_columns, missing_cols, extra_cols = compare_columns(
            table_columns, 
            df.columns.tolist()
        )
        
        # Generate INSERT statement
        insert_stmt, insert_columns = generate_insert_statement(table_columns, df)
        
        # Test insert
        test_insert(engine, df, table_columns)
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        
        if missing_cols:
            print(f"\n[ACTION REQUIRED] Add these {len(missing_cols)} columns to the DataFrame:")
            for col in sorted(missing_cols):
                print(f"  df['{col}'] = ''  # or appropriate default value")
        else:
            print("\n✓ All required columns are present in the DataFrame")
        
        if extra_cols:
            print(f"\n[INFO] These {len(extra_cols)} DataFrame columns will be ignored:")
            for col in sorted(extra_cols):
                print(f"  - {col}")
        
        print("\n" + "="*80)
        
    except Exception as e:
        print(f"\n[FATAL ERROR] {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()