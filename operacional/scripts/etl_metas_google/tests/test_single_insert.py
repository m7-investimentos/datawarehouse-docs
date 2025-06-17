#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test single row insert for ETL-003
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_003_targets import PerformanceTargetsETL
import logging
import json
import pandas as pd
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger('SINGLE_INSERT_TEST')

# Load config
with open('config/etl_003_config.json', 'r') as f:
    config = json.load(f)

logger.info("Testing single row insert...")

try:
    etl = PerformanceTargetsETL(config, logger)
    etl.setup_connections()
    
    # Create a single test row
    test_data = pd.DataFrame([{
        'load_timestamp': datetime.now(),
        'load_source': 'TEST',
        'cod_assessor': '99999',
        'nome_assessor': 'Test User',
        'indicator_code': 'TEST_IND',
        'period_type': 'MENSAL',
        'period_start': '2025-01-01',
        'period_end': '2025-01-31',
        'target_value': '100.00',
        'stretch_value': '120.00',
        'minimum_value': '80.00',
        'row_number': 1,
        'row_hash': 'test123',
        'target_year': 2025,
        'target_quarter': 1,
        'is_processed': 0,
        'processing_date': None,
        'processing_status': None,
        'processing_notes': None,
        'target_logic_valid': 1,
        'is_inverted': 0,
        'validation_errors': None
    }])
    
    # Try to insert using to_sql
    logger.info("Attempting insert with to_sql...")
    try:
        with etl.db_engine.connect() as conn:
            test_data.to_sql(
                'performance_targets',
                conn,
                schema='bronze',
                if_exists='append',
                index=False,
                method=None  # Use default method
            )
            logger.info("✓ Single row insert succeeded!")
            
            # Clean up test data
            conn.execute("DELETE FROM bronze.performance_targets WHERE load_source = 'TEST'")
            conn.commit()
            
    except Exception as e:
        logger.error(f"✗ Single row insert failed: {e}")
        
        # Try raw SQL
        logger.info("\nAttempting insert with raw SQL...")
        try:
            with etl.db_engine.connect() as conn:
                sql = """
                INSERT INTO bronze.performance_targets 
                (load_timestamp, load_source, cod_assessor, nome_assessor, indicator_code, 
                 period_type, period_start, period_end, target_value, stretch_value, 
                 minimum_value, row_number, row_hash, target_year, target_quarter, 
                 is_processed, processing_date, processing_status, processing_notes, 
                 target_logic_valid, is_inverted, validation_errors)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                params = (
                    datetime.now(), 'TEST', '99999', 'Test User', 'TEST_IND',
                    'MENSAL', '2025-01-01', '2025-01-31', '100.00', '120.00',
                    '80.00', 1, 'test123', 2025, 1,
                    0, None, None, None,
                    1, 0, None
                )
                
                conn.execute(sql, params)
                conn.commit()
                logger.info("✓ Raw SQL insert succeeded!")
                
                # Clean up
                conn.execute("DELETE FROM bronze.performance_targets WHERE load_source = 'TEST'")
                conn.commit()
                
        except Exception as e2:
            logger.error(f"✗ Raw SQL insert also failed: {e2}")
            
except Exception as e:
    logger.error(f"Test setup failed: {e}")
    
logger.info("\nTest completed.")