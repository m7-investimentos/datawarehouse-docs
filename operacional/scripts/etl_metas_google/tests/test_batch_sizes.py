#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to find optimal batch size for ETL-003
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_003_targets import PerformanceTargetsETL
import logging
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger('BATCH_TEST')

# Load config
with open('config/etl_003_config.json', 'r') as f:
    config = json.load(f)

# Test different batch sizes
batch_sizes = [1, 10, 50, 100, 200]

for batch_size in batch_sizes:
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing batch size: {batch_size}")
    logger.info(f"{'='*60}")
    
    config['batch_size'] = batch_size
    
    try:
        etl = PerformanceTargetsETL(config, logger)
        etl.setup_connections()
        etl.load_inverted_indicators()
        
        # Extract just a small sample
        etl.batch_size = 100  # For extraction
        etl.extract_in_batches()
        
        # Use first 100 records only
        etl.data = etl.data.head(100)
        
        if etl.validate_data():
            etl.transform()
            
            # Set the batch size for loading
            etl.batch_size = batch_size
            
            # Try to load
            records_loaded = etl.load_optimized(dry_run=False)
            logger.info(f"✓ Batch size {batch_size}: SUCCESS - {records_loaded} records loaded")
            
            # Clean up test data
            with etl.db_engine.connect() as conn:
                conn.execute("DELETE FROM bronze.performance_targets WHERE load_source LIKE '%BATCH_TEST%'")
                conn.commit()
        else:
            logger.error(f"✗ Batch size {batch_size}: VALIDATION FAILED")
            
    except Exception as e:
        logger.error(f"✗ Batch size {batch_size}: ERROR - {str(e)[:100]}...")
        
logger.info("\n" + "="*60)
logger.info("Test completed")