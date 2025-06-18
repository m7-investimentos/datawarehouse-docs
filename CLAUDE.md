# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Data Warehouse Documentation project for M7 Investimentos following a medallion architecture (Bronze → Silver → Gold → Platinum layers). The project tracks and analyzes investment advisor performance metrics, client funding flows (captação líquida), and various KPIs.

## Architecture

### Data Layers
- **Bronze Layer**: Raw data from source systems (XP Investimentos, Open Investment, Google Sheets)
- **Silver Layer**: Cleaned, normalized facts and dimensions
- **Gold Layer**: Business-ready aggregated views and tables
  - **gold_performance**: Performance metrics and KPIs for business analysis
  - **gold_llm**: Data optimized for Large Language Models and AI applications
- **Platinum Layer**: Executive dashboards and final reporting tables

### Key Architectural Patterns
- **Medallion Architecture**: Strict Bronze → Silver → Gold → Platinum flow
- **EAV Pattern**: Used in performance tracking tables (indicator/assignment/target)
- **Slowly Changing Dimensions**: Type 2 for tracking historical changes
- **Row Level Security**: Implemented at Silver/Gold layers for data access control

## Common Commands

### ETL Pipeline Execution

```bash
# Run interactive menu for all ETL options
./run_etl.sh

# Run specific pipeline (ETL + Procedure)
python run_pipeline.py 001  # Indicators
python run_pipeline.py 002  # Assignments  
python run_pipeline.py 003  # Targets

# Run all ETLs in sequence
python run_all_etls.py

# Run full pipeline (all ETLs + all procedures)
python run_full_pipeline.py
```

### Database Operations

```bash
# Test database connection
python test_connection.py

# Verify data after ETL
python verify_data.py

# Check Bronze table structure
python tests/check_bronze_structure.py
```

### Common SQL Procedures

```sql
-- Process Bronze to Silver
EXEC bronze.prc_bronze_to_silver_indicators;
EXEC bronze.prc_bronze_to_silver_assignments;
EXEC bronze.prc_bronze_to_silver_targets;

-- Update Gold layer
EXEC dbo.prc_gold_performance_to_table_captacao_liquida_assessor @debug = 1;
EXEC dbo.prc_gold_performance_to_table_captacao_liquida_cliente @debug = 1;
```

## File Naming Conventions

### SQL Query Files (MANDATORY)
All SQL queries MUST follow the template at `/Users/bchiaramonti/Downloads/QRY-template.sql`.

Pattern: `QRY-[AREA]-[SEQUENTIAL]-[description].sql`

Area codes:
- ASS: Assignments (Atribuições)
- IND: Indicators (Indicadores)  
- TAR: Targets (Metas)
- CAP: Captação (Funding)
- RES: Resgates (Withdrawals)
- PAT: Patrimônio (Assets)
- CLI: Clientes (Clients)
- PES: Pessoas (People)
- EST: Estruturas (Structures)

### Documentation Files
- `MOD-[AREA]-[SEQ]-[description].md`: Data models
- `ETL-[AREA]-[SEQ]-[description].md`: ETL processes
- `ARQ-[SEQ]-[description].md`: Architecture documents

## ETL Pipeline Structure

### ETL Metas Google (operacional/scripts/etl_metas_google)

```
Google Sheets → Python ETL → Bronze → SQL Procedure → Silver → Gold
```

Key components:
- **etl_001_indicators.py**: Loads performance indicators master data
- **etl_002_assignments.py**: Loads advisor-indicator assignments with weights
- **etl_003_targets.py**: Loads performance targets by period
- **run_pipeline.py**: Orchestrates ETL + procedure execution
- **config/**: JSON configuration files with spreadsheet IDs and mappings

### ETL XP Hub (operacional/scripts/etl_xp_hub)

```
S3 → Extractors → Processors (Tabela) → Loaders → SQL Server (Bronze)
```

Key components:
- **main.py**: Auto-discovers and runs all processors
- **processors/**: Individual file processors (captacao, contas, etc.)
- **Tabela class**: Generic Excel processor with chainable transformations

## Environment Variables

```bash
# Database
DB_SERVER=172.17.0.10
DB_DATABASE=M7Medallion
DB_USERNAME=m7invest
DB_PASSWORD=!@Multi19732846
DB_DRIVER=ODBC Driver 17 for SQL Server

# AWS S3 (for etl_xp_hub)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=sa-east-1
S3_BUCKET_NAME=m7investimentos

# Google Sheets credentials in: credentials/google_sheets_api.json
```

## Key Business Entities

### Performance Management
- **performance_indicators**: Master list of KPIs (NPS, Captação, etc.)
- **performance_assignments**: Advisor-indicator relationships with weights
- **performance_targets**: Period-specific targets for each indicator

### Investment Tracking
- **Assessores/CRM**: Investment advisors (identified by crm_id)
- **Captação**: Client deposits/funding
- **Resgates**: Client withdrawals
- **Patrimônio**: Assets under management

## Critical Implementation Notes

### Column Name Changes
- **IMPORTANT**: All references to `cod_assessor` have been replaced with `crm_id` throughout the system
- This affects assignments, targets, and all related queries

### Date Handling
- Bronze layer stores dates as VARCHAR (for raw data preservation)
- Silver layer converts to proper DATE types
- Empty valid_from dates default to '2025-01-01'

### Weight Validation
- CARD type indicators must sum to 100% per advisor
- Validation occurs during Bronze → Silver transformation
- Non-CARD types (GATILHO, KPI, etc.) have 0% weight

### Common Procedure Issues
- The `bronze.prc_bronze_to_silver_assignments` was rewritten in v2.0.0 to fix complex subquery issues
- Avoid complex CTEs with temporary tables in procedures
- Always use table aliases when referencing columns in subqueries

## SQL Patterns

### Extended Properties (Required)
```sql
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Description here',
    @level0type=N'SCHEMA', @level0name=N'bronze',
    @level1type=N'TABLE', @level1name=N'table_name',
    @level2type=N'COLUMN', @level2name=N'column_name';
```

### Processing Pattern
```sql
-- Mark as processed after successful transformation
UPDATE bronze.source_table
SET 
    is_processed = 1,
    processing_date = GETDATE(),
    processing_status = 'SUCCESS'
WHERE is_processed = 0;
```

### Incremental Loading
```sql
WHERE NOT EXISTS (
    SELECT 1 FROM target_table t
    WHERE t.key_col = source.key_col
      AND t.valid_from = source.valid_from
)
```

## Version Control

- Main branch: `main`
- Author email: bruno.chiaramonti@multisete.com
- Always update version history in SQL files (section 14)
- Document changes in CHANGES.md for ETL scripts

## Project State

- **Current**: SQL Server with medallion architecture in M7Medallion database
- **Legacy**: M7InvestimentosOLAP database (being phased out)
- **Future**: Planning migration to Google BigQuery
- **Active Development**: Performance management system (KPIs, assignments, targets)